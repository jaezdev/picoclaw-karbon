package paperless

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/media"
)

// This opt-in test uses synthetic fixtures and never sends a real chat message.
func TestLivePaperless(t *testing.T) {
	fixtures := os.Getenv("PAPERLESS_LIVE_FIXTURES")
	if fixtures == "" {
		t.Skip("set PAPERLESS_LIVE_FIXTURES to run against a real instance")
	}
	cfg := config.PaperlessConfig{
		Enabled:     true,
		AutoArchive: true,
		BaseURL:     "http://paperless:8000",
		PublicURL:   "https://paperless.home.solucional.com",
		TokenFile:   os.Getenv("PAPERLESS_LIVE_TOKEN"),
		QueueDir:    filepath.Join(t.TempDir(), "queue"),
	}
	store := media.NewFileMediaStore()
	var finalReceipt string
	notify := func(_ context.Context, m bus.OutboundMessage) error {
		if strings.Contains(m.Content, "/documents/") {
			finalReceipt = m.Content
		}
		return nil
	}
	s, err := New(cfg, store, notify)
	if err != nil {
		t.Fatal(err)
	}
	var jobs []Job
	var refs []string
	for _, name := range []string{"verification.pdf", "verification.png", "verification.jpg"} {
		b, readErr := os.ReadFile(filepath.Join(fixtures, name))
		if readErr != nil {
			t.Fatal(readErr)
		}
		p := filepath.Join(t.TempDir(), name)
		if err = os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if strings.HasSuffix(name, "pdf") {
			j, enqueueErr := s.EnqueuePath(p, "PicoClaw integration verification PDF")
			if enqueueErr != nil {
				t.Fatal(enqueueErr)
			}
			jobs = append(jobs, j)
			_ = os.Remove(p)
		} else {
			ref, storeErr := store.Store(p, media.MediaMeta{Filename: name, Source: "telegram"}, "album")
			if storeErr != nil {
				t.Fatal(storeErr)
			}
			refs = append(refs, ref)
		}
	}
	msg := bus.InboundMessage{
		Context: bus.InboundContext{
			Channel:   "telegram",
			ChatID:    "local-verification",
			SenderID:  "local-verification",
			MessageID: "verification-album",
		},
		Media: refs,
	}
	if err = s.Capture(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if err = store.ReleaseAll("album"); err != nil {
		t.Fatal(err)
	}
	// Persist upload task IDs, then reopen as if the gateway restarted.
	s.step(context.Background())
	s, err = New(cfg, store, notify)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		s.step(context.Background())
		all := len(s.jobs) == 3
		for _, j := range s.jobs {
			if j.Status == "failed" {
				t.Fatalf("ingestion failed: %s", j.Error)
			}
			if j.Status != "success" {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(2 * time.Second)
	}
	jobs = make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		if j.Status != "success" || j.DocumentID == 0 || j.URL == "" {
			t.Fatalf("incomplete archival: %+v", j)
		}
		jobs = append(jobs, j)
	}
	if len(jobs) != 3 || strings.Count(finalReceipt, "/documents/") != 2 {
		t.Fatalf("missing successful album receipt: %s", finalReceipt)
	}
	if _, err = s.request(context.Background(), "GET", "/api/documents/", nil, ""); err == nil {
		t.Fatal("ingest account unexpectedly has document-reading access")
	}
	if err = os.WriteFile(filepath.Join(fixtures, "verified-jobs.json"), mustJSON(t, jobs), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Log(
		"PDF, PNG and JPEG were processed; album receipt contains two links; recovery after source cleanup and queue reopen passed; archive-reading access denied.",
	)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	return b
}
