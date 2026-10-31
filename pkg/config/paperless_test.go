package config

import (
	"encoding/json"
	"testing"
)

func TestPaperlessConfigSurvivesConsoleRoundTrip(t *testing.T) {
	c := DefaultConfig()
	if c.Tools.Paperless.Enabled {
		t.Fatal("archival must be opt-in")
	}
	c.Tools.Paperless = PaperlessConfig{
		Enabled:     true,
		AutoArchive: true,
		BaseURL:     "http://paperless:8000",
		PublicURL:   "https://paperless.example.com",
		TokenFile:   "/private/token",
		QueueDir:    "/private/queue",
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var restored Config
	if err = json.Unmarshal(b, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Tools.Paperless != c.Tools.Paperless {
		t.Fatal("console configuration round trip dropped integration fields")
	}
}
