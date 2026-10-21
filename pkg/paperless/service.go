// Package paperless durably archives incoming attachments without an LLM call.
package paperless

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/h2non/filetype"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/media"
)

var ErrUnsupported = errors.New("unsupported file: use a PDF or raster image")

type Job struct {
	ID          string    `json:"job_id"`
	Filename    string    `json:"filename"`
	Title       string    `json:"title,omitempty"`
	Blob        string    `json:"-"`
	Status      string    `json:"status"`
	TaskID      string    `json:"task_id,omitempty"`
	DocumentID  int       `json:"document_id,omitempty"`
	URL         string    `json:"url,omitempty"`
	Error       string    `json:"error,omitempty"`
	Attempts    int       `json:"attempts"`
	NextAttempt time.Time `json:"next_attempt,omitempty"`
}

type item struct {
	JobID    string `json:"job_id,omitempty"`
	Filename string `json:"filename"`
	Error    string `json:"error,omitempty"`
}
type batch struct {
	ID              string             `json:"id"`
	Context         bus.InboundContext `json:"context"`
	Items           []item             `json:"items"`
	Notified        bool               `json:"notified"`
	PendingNotified bool               `json:"pending_notified"`
	Created         time.Time          `json:"created"`
}

type Notify func(context.Context, bus.OutboundMessage) error

type Service struct {
	mu      sync.Mutex
	cfg     config.PaperlessConfig
	store   media.MediaStore
	client  *http.Client
	jobs    map[string]Job
	batches map[string]batch
	notify  Notify
	cancel  context.CancelFunc
	done    chan struct{}
}

func New(cfg config.PaperlessConfig, store media.MediaStore, notify Notify) (*Service, error) {
	for _, raw := range []string{cfg.BaseURL, cfg.PublicURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("paperless requires valid HTTP(S) base_url and public_url without credentials, query, or fragment")
		}
	}
	if cfg.QueueDir == "" || cfg.TokenFile == "" {
		return nil, errors.New("paperless requires queue_dir and token_file")
	}
	for _, dir := range []string{cfg.QueueDir, filepath.Join(cfg.QueueDir, "jobs"), filepath.Join(cfg.QueueDir, "batches"), filepath.Join(cfg.QueueDir, "blobs")} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
		if err := os.Chmod(dir, 0700); err != nil {
			return nil, err
		}
	}
	s := &Service{cfg: cfg, store: store, notify: notify, jobs: map[string]Job{}, batches: map[string]batch{}, done: make(chan struct{}), client: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return errors.New("Paperless redirects are not allowed")
	}}}
	files, err := filepath.Glob(filepath.Join(cfg.QueueDir, "jobs", "*.json"))
	if err != nil {
		return nil, err
	}
	for _, p := range files {
		var j Job
		if err := readJSON(p, &j); err != nil {
			return nil, fmt.Errorf("load Paperless queue: %w", err)
		}
		if !validID(j.ID) {
			return nil, errors.New("invalid Paperless queue job ID")
		}
		j.Blob = filepath.Join(cfg.QueueDir, "blobs", j.ID)
		// A lost upload response is safe to retry: Paperless rejects duplicate content.
		if j.Status == "uploading" {
			j.Status = "queued"
		}
		s.jobs[j.ID] = j
	}
	files, err = filepath.Glob(filepath.Join(cfg.QueueDir, "batches", "*.json"))
	if err != nil {
		return nil, err
	}
	for _, p := range files {
		var b batch
		if err := readJSON(p, &b); err != nil {
			return nil, err
		}
		if !validID(b.ID) {
			return nil, errors.New("invalid Paperless batch ID")
		}
		s.batches[b.ID] = b
	}
	return s, nil
}

func validID(id string) bool { b, e := hex.DecodeString(id); return e == nil && len(b) == sha256.Size }
func readJSON(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}

// atomicJSON commits each state transition before source cleanup or notification.
func atomicJSON(path string, value any) error {
	b, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".state-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

func (s *Service) saveJob(j Job) error {
	if err := atomicJSON(filepath.Join(s.cfg.QueueDir, "jobs", j.ID+".json"), j); err != nil {
		return err
	}
	s.jobs[j.ID] = j
	return nil
}
func (s *Service) saveBatch(b batch) error {
	if err := atomicJSON(filepath.Join(s.cfg.QueueDir, "batches", b.ID+".json"), b); err != nil {
		return err
	}
	s.batches[b.ID] = b
	return nil
}

func (s *Service) EnqueuePath(path, title string) (Job, error) {
	f, err := os.Open(path)
	if err != nil {
		return Job{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return Job{}, err
	}
	if !st.Mode().IsRegular() {
		return Job{}, errors.New("only regular files can be archived")
	}
	return s.enqueue(f, filepath.Base(path), title)
}

func (s *Service) enqueue(r io.Reader, name, title string) (Job, error) {
	f, err := os.CreateTemp(filepath.Join(s.cfg.QueueDir, "blobs"), ".incoming-")
	if err != nil {
		return Job{}, err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err != nil {
		return Job{}, err
	}
	if n == 0 {
		return Job{}, errors.New("empty attachment")
	}
	head := make([]byte, 8192)
	sf, err := os.Open(tmp)
	if err != nil {
		return Job{}, err
	}
	count, _ := sf.Read(head)
	sf.Close()
	kind, err := filetype.Match(head[:count])
	if err != nil {
		return Job{}, err
	}
	if kind.MIME.Value != "application/pdf" && (!strings.HasPrefix(kind.MIME.Value, "image/") || kind.MIME.Value == "image/svg+xml") {
		return Job{}, ErrUnsupported
	}
	id := hex.EncodeToString(h.Sum(nil))
	name = filepath.Base(name)
	if name == "" || name == "." {
		name = "attachment." + kind.Extension
	}
	if filepath.Ext(name) == "" {
		name += "." + kind.Extension
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.jobs[id]; ok {
		return old, nil
	}
	blob := filepath.Join(s.cfg.QueueDir, "blobs", id)
	if err = os.Rename(tmp, blob); err != nil {
		return Job{}, err
	}
	d, err := os.Open(filepath.Dir(blob))
	if err != nil {
		return Job{}, err
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return Job{}, err
	}
	j := Job{ID: id, Filename: name, Title: title, Blob: blob, Status: "queued"}
	if err = s.saveJob(j); err != nil {
		return Job{}, err
	}
	return j, nil
}

// Capture is called only after channel access checks and never waits for HTTP/OCR.
func (s *Service) Capture(ctx context.Context, msg bus.InboundMessage) error {
	if !s.cfg.AutoArchive || len(msg.Media) == 0 {
		return nil
	}
	b := batch{Context: msg.Context, Created: time.Now()}
	for i, ref := range msg.Media {
		name := fmt.Sprintf("%s-%s-%d", msg.Context.Channel, msg.Context.MessageID, i+1)
		var j Job
		var err error
		if strings.HasPrefix(ref, "data:") {
			header, body, ok := strings.Cut(ref, ",")
			if !ok || !strings.HasSuffix(header, ";base64") || (!strings.HasPrefix(header, "data:image/") && !strings.HasPrefix(header, "data:application/pdf;")) {
				continue
			}
			j, err = s.enqueue(base64.NewDecoder(base64.StdEncoding, strings.NewReader(body)), name, "")
		} else {
			path := ref
			if strings.HasPrefix(ref, "media://") {
				if s.store == nil {
					err = errors.New("attachment media store unavailable")
				} else {
					var meta media.MediaMeta
					path, meta, err = s.store.ResolveWithMeta(ref)
					if meta.Filename != "" {
						name = meta.Filename
					}
				}
			} else {
				name = filepath.Base(ref)
			}
			if err == nil {
				var f *os.File
				f, err = os.Open(path)
				if err == nil {
					var st os.FileInfo
					st, err = f.Stat()
					if err == nil && !st.Mode().IsRegular() {
						err = errors.New("attachment is not a regular file")
					}
					if err == nil {
						j, err = s.enqueue(f, name, "")
					}
					f.Close()
				}
			}
		}
		if errors.Is(err, ErrUnsupported) {
			ext := strings.ToLower(filepath.Ext(name))
			if !strings.HasPrefix(ref, "data:image/") && !strings.Contains(".pdf .png .jpg .jpeg .gif .tif .tiff .webp .bmp .heic .svg", ext) {
				continue
			}
			if ext == "" && !strings.HasPrefix(ref, "data:image/") {
				continue
			}
		}
		it := item{JobID: j.ID, Filename: name}
		if err != nil {
			it.Error = "Could not queue attachment: " + err.Error()
		}
		b.Items = append(b.Items, it)
	}
	if len(b.Items) == 0 {
		return nil
	}
	key, _ := json.Marshal(struct {
		Channel   string
		Account   string
		ChatID    string
		SenderID  string
		MessageID string
		Items     []item
	}{b.Context.Channel, b.Context.Account, b.Context.ChatID, b.Context.SenderID, b.Context.MessageID, b.Items})
	h := sha256.Sum256(key)
	b.ID = hex.EncodeToString(h[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batches[b.ID]; ok {
		return nil
	}
	return s.saveBatch(b)
}

func (s *Service) Status(id string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.findJob(id)
}

// Receipts use a short hash; accept it only if it identifies one job uniquely.
func (s *Service) findJob(id string) (Job, error) {
	if j, ok := s.jobs[id]; ok {
		return j, nil
	}
	if len(id) < 12 {
		return Job{}, errors.New("unknown Paperless job")
	}
	var found Job
	count := 0
	for key, j := range s.jobs {
		if strings.HasPrefix(key, id) {
			found = j
			count++
		}
	}
	if count != 1 {
		return Job{}, errors.New("unknown or ambiguous Paperless job")
	}
	return found, nil
}

func (s *Service) Retry(id string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, err := s.findJob(id)
	if err != nil {
		return Job{}, err
	}
	id = j.ID
	if j.Status != "failed" {
		return j, nil
	}
	j.Status = "queued"
	j.TaskID = ""
	j.Error = ""
	j.NextAttempt = time.Time{}
	j.Attempts = 0
	if err := s.saveJob(j); err != nil {
		return Job{}, err
	}
	for _, b := range s.batches {
		for _, it := range b.Items {
			if it.JobID == id {
				b.Notified = false
				_ = s.saveBatch(b)
				break
			}
		}
	}
	return j, nil
}

func (s *Service) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	go func() {
		defer close(s.done)
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			s.step(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
}
func (s *Service) Stop() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
}

func terminal(status string) bool {
	return status == "success" || status == "duplicate" || status == "failed"
}

func (s *Service) step(ctx context.Context) {
	s.mu.Lock()
	ids := make([]string, 0, len(s.jobs))
	for id := range s.jobs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	s.mu.Unlock()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		s.mu.Lock()
		j := s.jobs[id]
		s.mu.Unlock()
		if terminal(j.Status) {
			if j.Status != "failed" {
				_ = os.Remove(j.Blob)
			}
			continue
		}
		if time.Now().Before(j.NextAttempt) {
			continue
		}
		var err error
		if j.TaskID == "" {
			j.Status = "uploading"
			s.mu.Lock()
			err = s.saveJob(j)
			s.mu.Unlock()
			if err != nil {
				continue
			}
			j.TaskID, err = s.upload(ctx, j)
			if err == nil {
				j.Status = "processing"
				j.Error = ""
				j.Attempts = 0
			}
		} else {
			err = s.poll(ctx, &j)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			j.Attempts++
			j.Error = err.Error()
			var ae *apiError
			if errors.As(err, &ae) && ae.status >= 400 && ae.status < 500 && ae.status != 408 && ae.status != 429 {
				j.Status = "failed"
			} else {
				j.Status = "queued"
				if j.TaskID != "" {
					j.Status = "processing"
				}
				delay := time.Second * time.Duration(1<<min(j.Attempts, 8))
				j.NextAttempt = time.Now().Add(min(delay, 5*time.Minute))
			}
		} else {
			j.NextAttempt = time.Time{}
		}
		s.mu.Lock()
		saveErr := s.saveJob(j)
		s.mu.Unlock()
		if saveErr == nil && (j.Status == "success" || j.Status == "duplicate") {
			_ = os.Remove(j.Blob)
		}
	}
	s.receipts(ctx)
}

type apiError struct {
	status  int
	message string
}

func (e *apiError) Error() string { return fmt.Sprintf("Paperless HTTP %d: %s", e.status, e.message) }

func (s *Service) request(ctx context.Context, method, path string, body io.Reader, contentType string, contentLength ...int64) ([]byte, error) {
	tokenBytes, err := os.ReadFile(s.cfg.TokenFile)
	if err != nil {
		return nil, errors.New("cannot read Paperless token file")
	}
	token := strings.TrimSpace(string(tokenBytes))
	if token == "" {
		return nil, errors.New("Paperless token file is empty")
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(s.cfg.BaseURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	if len(contentLength) > 0 {
		req.ContentLength = contentLength[0]
	}
	req.Header.Set("Authorization", "Token "+token)
	req.Header.Set("Accept", "application/json; version=10")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg := strings.ReplaceAll(string(data), token, "<redacted>")
		if len(msg) > 500 {
			msg = msg[:500]
		}
		return nil, &apiError{resp.StatusCode, msg}
	}
	return data, nil
}

func (s *Service) upload(ctx context.Context, j Job) (string, error) {
	f, err := os.Open(j.Blob)
	if err != nil {
		return "", &apiError{400, "queued file is missing; resend the attachment"}
	}
	r, w := io.Pipe()
	mw := multipart.NewWriter(w)
	// Django's multipart parser requires Content-Length. Count only the envelope;
	// the file itself is streamed once, without buffering it in RAM.
	st, err := f.Stat()
	if err != nil {
		f.Close()
		r.Close()
		w.Close()
		return "", err
	}
	counter := &countWriter{}
	countMultipart := multipart.NewWriter(counter)
	_ = countMultipart.SetBoundary(mw.Boundary())
	if _, err = countMultipart.CreateFormFile("document", j.Filename); err != nil {
		f.Close()
		r.Close()
		w.Close()
		return "", err
	}
	counter.n += st.Size()
	if j.Title != "" {
		_ = countMultipart.WriteField("title", j.Title)
	}
	_ = countMultipart.Close()
	go func() {
		defer f.Close()
		part, e := mw.CreateFormFile("document", j.Filename)
		if e == nil {
			_, e = io.Copy(part, f)
		}
		if e == nil && j.Title != "" {
			e = mw.WriteField("title", j.Title)
		}
		if e == nil {
			e = mw.Close()
		}
		w.CloseWithError(e)
	}()
	data, err := s.request(ctx, http.MethodPost, "/api/documents/post_document/", r, mw.FormDataContentType(), counter.n)
	r.Close()
	if err != nil {
		return "", err
	}
	var id string
	if err = json.Unmarshal(data, &id); err != nil || id == "" {
		return "", errors.New("invalid Paperless upload response")
	}
	return id, nil
}

type countWriter struct{ n int64 }

func (w *countWriter) Write(p []byte) (int, error) { w.n += int64(len(p)); return len(p), nil }

func (s *Service) poll(ctx context.Context, j *Job) error {
	data, err := s.request(ctx, http.MethodGet, "/api/tasks/?task_id="+url.QueryEscape(j.TaskID), nil, "")
	if err != nil {
		return err
	}
	type task struct {
		Status      string          `json:"status"`
		Result      string          `json:"result"`
		ResultData  json.RawMessage `json:"result_data"`
		Document    json.RawMessage `json:"related_document"`
		DocumentIDs []int           `json:"related_document_ids"`
	}
	var page struct {
		Results []task `json:"results"`
	}
	if err = json.Unmarshal(data, &page); err != nil {
		return err
	}
	if len(page.Results) == 0 {
		return errors.New("Paperless task not yet available")
	}
	t := page.Results[0]
	if len(t.DocumentIDs) > 0 {
		j.DocumentID = t.DocumentIDs[0]
	}
	if j.DocumentID == 0 && len(t.ResultData) > 0 {
		var result struct {
			DocumentID int `json:"document_id"`
		}
		if json.Unmarshal(t.ResultData, &result) == nil {
			j.DocumentID = result.DocumentID
		}
	}
	if t.Result == "" && len(t.ResultData) > 0 && string(t.ResultData) != "null" {
		t.Result = string(t.ResultData)
	}
	if len(t.Document) > 0 && string(t.Document) != "null" {
		raw := strings.Trim(string(t.Document), "\"")
		j.DocumentID, _ = strconv.Atoi(raw)
	}
	switch strings.ToUpper(t.Status) {
	case "SUCCESS":
		j.Status = "success"
		j.Error = ""
	case "FAILURE", "REVOKED":
		j.Status = "failed"
		j.Error = t.Result
		if len(j.Error) > 500 {
			j.Error = j.Error[:500]
		}
		lower := strings.ToLower(t.Result)
		if strings.Contains(lower, "duplicate") || strings.Contains(lower, "already exists") {
			j.Status = "duplicate"
			j.Error = "Already archived"
		}
	}
	if j.DocumentID > 0 {
		j.URL = strings.TrimRight(s.cfg.PublicURL, "/") + "/documents/" + strconv.Itoa(j.DocumentID) + "/details"
	}
	return nil
}

func (s *Service) receipts(ctx context.Context) {
	if s.notify == nil {
		return
	}
	s.mu.Lock()
	batches := make([]batch, 0, len(s.batches))
	for _, b := range s.batches {
		if !b.Notified {
			batches = append(batches, b)
		}
	}
	s.mu.Unlock()
	for _, b := range batches {
		complete := true
		lines := []string{"Paperless:"}
		for _, it := range b.Items {
			if it.Error != "" {
				lines = append(lines, it.Filename+": "+it.Error)
				continue
			}
			j, err := s.Status(it.JobID)
			if err != nil {
				complete = false
				continue
			}
			switch j.Status {
			case "success":
				lines = append(lines, j.Filename+": saved — "+j.URL)
			case "duplicate":
				line := j.Filename + ": already archived"
				if j.URL != "" {
					line += " — " + j.URL
				}
				lines = append(lines, line)
			case "failed":
				lines = append(lines, j.Filename+": could not save — "+j.Error+" [job "+j.ID[:12]+"]")
			default:
				complete = false
				lines = append(lines, j.Filename+": queued / processing [job "+j.ID[:12]+"]")
			}
		}
		if !complete && (b.PendingNotified || time.Since(b.Created) < 30*time.Second) {
			continue
		}
		err := s.notify(ctx, bus.OutboundMessage{Channel: b.Context.Channel, ChatID: b.Context.ChatID, Context: b.Context, ReplyToMessageID: b.Context.MessageID, Content: strings.Join(lines, "\n")})
		if err == nil {
			s.mu.Lock()
			b.Notified = complete
			b.PendingNotified = !complete
			_ = s.saveBatch(b)
			s.mu.Unlock()
		}
	}
}
