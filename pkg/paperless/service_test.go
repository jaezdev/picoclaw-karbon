package paperless

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/media"
)

func testConfig(t *testing.T, base string) config.PaperlessConfig {
	t.Helper()
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("test-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	return config.PaperlessConfig{Enabled: true, AutoArchive: true, BaseURL: base, PublicURL: "https://paperless.example.com", TokenFile: token, QueueDir: filepath.Join(dir, "queue")}
}
func pdf(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "invoice.pdf")
	if err := os.WriteFile(p, []byte("%PDF-1.4\nsynthetic test document"), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCaptureSurvivesCleanupRestartAndRedelivery(t *testing.T) {
	uploads := 0
	receipts := 0
	rejectReceipt := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Token test-secret" {
			t.Error("token missing")
		}
		if r.Header.Get("Accept") != "application/json; version=10" {
			t.Error("API version missing")
		}
		if r.Method == "POST" {
			if r.ContentLength <= 0 {
				t.Error("Django multipart uploads require Content-Length")
			}
			uploads++
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
			}
			f, h, e := r.FormFile("document")
			if e != nil {
				t.Error(e)
			} else {
				f.Close()
				if h.Filename != "invoice.pdf" {
					t.Error(h.Filename)
				}
			}
			fmt.Fprint(w, `"upload-task"`)
		} else {
			fmt.Fprint(w, `{"results":[{"status":"success","related_document_ids":[42],"result_data":{"document_id":42}}]}`)
		}
	}))
	defer server.Close()
	cfg := testConfig(t, server.URL)
	store := media.NewFileMediaStore()
	source := pdf(t)
	ref, err := store.Store(source, media.MediaMeta{Filename: "invoice.pdf", Source: "telegram"}, "scope")
	if err != nil {
		t.Fatal(err)
	}
	msg := bus.InboundMessage{Context: bus.InboundContext{Channel: "telegram", ChatID: "123", SenderID: "123", MessageID: "7"}, Media: []string{ref}}
	notify := func(_ context.Context, m bus.OutboundMessage) error {
		if rejectReceipt {
			return fmt.Errorf("channel unavailable")
		}
		receipts++
		if m.ChatID != "123" || m.ReplyToMessageID != "7" {
			t.Error("receipt routed incorrectly")
		}
		if !strings.Contains(m.Content, "/documents/42/details") {
			t.Error(m.Content)
		}
		return nil
	}
	s, err := New(cfg, store, notify)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Capture(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseAll("scope"); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(source); !os.IsNotExist(err) {
		t.Fatal("source cleanup did not run")
	}
	// Reopen the queue after all transient source files disappeared.
	s, err = New(cfg, store, notify)
	if err != nil {
		t.Fatal(err)
	}
	s.step(context.Background())
	s.step(context.Background())
	if uploads != 1 || receipts != 0 {
		t.Fatalf("uploads %d receipts %d", uploads, receipts)
	}
	s, err = New(cfg, store, notify)
	if err != nil {
		t.Fatal(err)
	}
	rejectReceipt = false
	s.step(context.Background())
	s.step(context.Background())
	if receipts != 1 {
		t.Fatalf("receipts: %d", receipts)
	}
	if err = os.WriteFile(source, []byte("%PDF-1.4\nsynthetic test document"), 0600); err != nil {
		t.Fatal(err)
	}
	ref, err = store.Store(source, media.MediaMeta{Filename: "invoice.pdf"}, "scope")
	if err != nil {
		t.Fatal(err)
	}
	msg.Media = []string{ref}
	msg.Context.Raw = map[string]string{"transport_connection": "new-connection-after-redelivery"}
	if err = s.Capture(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	s.step(context.Background())
	if uploads != 1 || receipts != 1 {
		t.Fatalf("redelivery duplicated upload/receipt: %d/%d", uploads, receipts)
	}
	for _, j := range s.jobs {
		if _, err = os.Stat(j.Blob); !os.IsNotExist(err) {
			t.Fatal("successful blob was not removed")
		}
	}
}

func TestServerFailureAuthFailureAndRetry(t *testing.T) {
	status := 503
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" && status != 200 {
			w.WriteHeader(status)
			fmt.Fprint(w, "synthetic rejection")
			return
		}
		if r.Method == "POST" {
			fmt.Fprint(w, `"task"`)
		} else {
			fmt.Fprint(w, `{"results":[{"status":"failure","result_data":{"error":"duplicate document already exists"},"related_document_ids":[9]}]}`)
		}
	}))
	defer server.Close()
	s, err := New(testConfig(t, server.URL), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.EnqueuePath(pdf(t), "")
	if err != nil {
		t.Fatal(err)
	}
	s.step(context.Background())
	j, _ = s.Status(j.ID)
	if j.Status != "queued" || !j.NextAttempt.After(time.Now()) {
		t.Fatalf("not scheduled for retry: %+v", j)
	}
	s.mu.Lock()
	j.NextAttempt = time.Time{}
	_ = s.saveJob(j)
	s.mu.Unlock()
	status = 401
	s.step(context.Background())
	j, _ = s.Status(j.ID)
	if j.Status != "failed" {
		t.Fatalf("auth failure not retained: %+v", j)
	}
	if _, err = os.Stat(j.Blob); err != nil {
		t.Fatal(err)
	}
	status = 200
	if _, err = s.Retry(j.ID); err != nil {
		t.Fatal(err)
	}
	s.step(context.Background())
	s.step(context.Background())
	j, _ = s.Status(j.ID)
	if j.Status != "duplicate" || j.DocumentID != 9 {
		t.Fatalf("duplicate not recognized: %+v", j)
	}
}

func TestInlineImageAndUnsupportedImage(t *testing.T) {
	var text string
	s, err := New(testConfig(t, "http://127.0.0.1"), nil, func(_ context.Context, m bus.OutboundMessage) error { text = m.Content; return nil })
	if err != nil {
		t.Fatal(err)
	}
	// A PNG signature is sufficient for content sniffing; OCR is tested separately against Paperless.
	image := []byte{137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 0}
	msg := bus.InboundMessage{Context: bus.InboundContext{Channel: "pico", ChatID: "chat", SenderID: "user", MessageID: "inline"}, Media: []string{"data:image/png;base64," + base64.StdEncoding.EncodeToString(image)}}
	if err = s.Capture(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if len(s.jobs) != 1 {
		t.Fatal("inline image not captured")
	}
	bad := filepath.Join(t.TempDir(), "broken.jpg")
	_ = os.WriteFile(bad, []byte("not an image"), 0600)
	msg.Context.MessageID = "bad"
	msg.Media = []string{bad}
	if err = s.Capture(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	// Avoid HTTP in this test and inspect terminal batch notification directly.
	s.receipts(context.Background())
	if !strings.Contains(text, "unsupported file") {
		t.Fatal(text)
	}
	for _, sub := range []string{"jobs", "batches", "blobs"} {
		st, err := os.Stat(filepath.Join(s.cfg.QueueDir, sub))
		if err != nil {
			t.Fatal(err)
		}
		if st.Mode().Perm() != 0700 {
			t.Fatal("queue permissions")
		}
	}
}
