package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/session"
)

func TestSharedExplicitKeyCannotEnterEscapeOrAlias(t *testing.T) {
	shared := session.BuildSharedSessionKey("main", "owner")
	normal := "sk_v1_normal"
	if resolveScopeKey(normal, shared) != normal || resolveScopeKey(shared, normal) != shared {
		t.Fatal("explicit key bypass")
	}
	if a := buildSessionAliases(normal, shared, " SC_V1_test "); len(a) != 0 {
		t.Fatalf("shared alias admitted: %v", a)
	}
}

func TestSharedFullTurnsKeepOutboundDestinations(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.SummarizeMessageThreshold = 1000
	cfg.Session.SharedGroups = []config.SharedSessionGroup{
		{
			ID: "owner",
			Members: []config.SharedSessionMember{
				{Channel: "telegram", Account: "default", ChatType: "direct", ChatID: "123", SenderID: "123"},
				{Channel: "mqtt", Account: "default", ChatType: "direct", ChatID: "mqtt:job", SenderID: "job"},
			},
		},
	}
	b := bus.NewMessageBus()
	al := NewAgentLoop(cfg, b, &simpleMockProvider{response: "done"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msgs := []bus.InboundMessage{
		{
			Channel:  "telegram",
			ChatID:   "123",
			SenderID: "123",
			Content:  "telegram marker",
			Context: bus.InboundContext{
				Channel:  "telegram",
				Account:  "default",
				ChatType: "direct",
				ChatID:   "123",
				SenderID: "123",
			},
		},
		{
			Channel:  "mqtt",
			ChatID:   "mqtt:job",
			SenderID: "job",
			Content:  "mqtt marker",
			Context: bus.InboundContext{
				Channel:  "mqtt",
				Account:  "default",
				ChatType: "direct",
				ChatID:   "mqtt:job",
				SenderID: "job",
			},
		},
	}
	var wg sync.WaitGroup
	for _, m := range msgs {
		wg.Add(1)
		go func(m bus.InboundMessage) { defer wg.Done(); al.processMessageSync(ctx, m) }(m)
	}
	wg.Wait()
	seen := map[string]bool{}
	for range msgs {
		select {
		case out := <-b.OutboundChan():
			seen[out.Channel+":"+out.ChatID] = true
			if out.SessionKey != session.BuildSharedSessionKey("main", "owner") {
				t.Fatal("wrong outbound shared key")
			}
		case <-ctx.Done():
			t.Fatal("missing outbound")
		}
	}
	if !seen["telegram:123"] || !seen["mqtt:mqtt:job"] {
		t.Fatal("outbound misrouted")
	}
	key := session.BuildSharedSessionKey("main", "owner")
	h := al.registry.GetDefaultAgent().Sessions.GetHistory(key)
	users := 0
	for _, m := range h {
		if m.Role == "user" {
			users++
		}
	}
	if users != 2 {
		t.Fatalf("lost turns: %d", users)
	}
	// Supplying the shared key from an outsider must neither load nor relabel it.
	outsider := msgs[0]
	outsider.SenderID = "outsider"
	outsider.ChatID = "other"
	outsider.Context.SenderID = "outsider"
	outsider.Context.ChatID = "other"
	outsider.SessionKey = key
	if _, err := al.processMessage(ctx, outsider); err != nil {
		t.Fatal(err)
	}
	if len(al.registry.GetDefaultAgent().Sessions.GetHistory(key)) != len(h) {
		t.Fatal("outsider mutated shared history")
	}
}

func TestSharedRunQueueFIFOAndIndependentDestination(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	cfg.Agents.Defaults.SummarizeMessageThreshold = 1000
	cfg.Session.SharedGroups = []config.SharedSessionGroup{
		{
			ID: "owner",
			Members: []config.SharedSessionMember{
				{Channel: "telegram", Account: "default", ChatType: "direct", ChatID: "123", SenderID: "123"},
				{Channel: "mqtt", Account: "default", ChatType: "direct", ChatID: "mqtt:job", SenderID: "job"},
			},
		},
	}
	b := bus.NewMessageBus()
	al := NewAgentLoop(cfg, b, &simpleMockProvider{response: "done"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); al.Run(ctx) }()
	for i := 0; i < 6; i++ {
		c := bus.InboundContext{
			Channel:  "telegram",
			Account:  "default",
			ChatType: "direct",
			ChatID:   "123",
			SenderID: "123",
		}
		if i%2 == 1 {
			c = bus.InboundContext{
				Channel:  "mqtt",
				Account:  "default",
				ChatType: "direct",
				ChatID:   "mqtt:job",
				SenderID: "job",
			}
		}
		b.PublishInbound(ctx, bus.InboundMessage{Context: c, Content: fmt.Sprintf("marker-%d", i)})
	}
	for i := 0; i < 6; i++ {
		select {
		case out := <-b.OutboundChan():
			want := "telegram"
			if i%2 == 1 {
				want = "mqtt"
			}
			if out.Channel != want || out.SessionKey != session.BuildSharedSessionKey("main", "owner") {
				t.Fatalf("wrong destination/key at %d: %+v", i, out)
			}
		case <-ctx.Done():
			t.Fatal("missing response")
		}
	}
	history := al.registry.GetDefaultAgent().Sessions.GetHistory(session.BuildSharedSessionKey("main", "owner"))
	i := 0
	for _, m := range history {
		if m.Role == "user" {
			if !strings.Contains(m.Content, fmt.Sprintf("marker-%d", i)) {
				t.Fatalf("out of order %d: %s", i, m.Content)
			}
			i++
		}
	}
	if i != 6 {
		t.Fatalf("lost turn: %d", i)
	}
	cancel()
	<-done
}

func TestSharedDirectAndContinuationCannotImpersonateMember(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Agents.Defaults.Workspace = t.TempDir()
	al := NewAgentLoop(cfg, bus.NewMessageBus(), &simpleMockProvider{response: "done"})
	key := session.BuildSharedSessionKey("main", "owner")
	al.registry.GetDefaultAgent().Sessions.AddMessage(key, "user", "private")
	if _, err := al.Continue(context.Background(), key, "telegram", "123"); err == nil {
		t.Fatal("continuation accepted shared key")
	}
	if _, err := al.ProcessDirectWithChannel(context.Background(), "outsider", key, "telegram", "123"); err != nil {
		t.Fatal(err)
	}
	if h := al.registry.GetDefaultAgent().Sessions.GetHistory(key); len(h) != 1 {
		t.Fatal("direct entered shared history")
	}
	if _, err := al.runAgentLoop(
		context.Background(),
		al.registry.GetDefaultAgent(),
		processOptions{Dispatch: DispatchRequest{SessionKey: key}},
	); err == nil {
		t.Fatal("raw dispatch accepted")
	}
}
