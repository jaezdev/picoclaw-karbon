package session

import (
	"context"
	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/providers"
	"github.com/sipeed/picoclaw/pkg/routing"
	"sync"
	"testing"
	"time"
)

func TestSharedExactMembershipAndAgentIsolation(t *testing.T) {
	members := []config.SharedSessionMember{{Channel: "telegram", Account: "default", ChatType: "direct", ChatID: "123", SenderID: "123"}, {Channel: "mqtt", Account: "default", ChatType: "direct", ChatID: "mqtt:approved", SenderID: "approved"}}
	policy := routing.SessionPolicy{Dimensions: []string{"chat"}, SharedGroups: []config.SharedSessionGroup{{ID: "owner", Members: members}}}
	input := AllocationInput{AgentID: "main", SessionPolicy: policy, Context: bus.InboundContext{Channel: "telegram", Account: "default", ChatType: "direct", ChatID: "123", SenderID: "123"}}
	first := AllocateRouteSession(input)
	input.Context = bus.InboundContext{Channel: "mqtt", Account: "default", ChatType: "direct", ChatID: "mqtt:approved", SenderID: "approved"}
	if second := AllocateRouteSession(input); first.SessionKey != second.SessionKey || len(second.SessionAliases) != 0 || len(second.MainAliases) != 0 {
		t.Fatal("members must share key without aliases")
	}
	input.AgentID = "other"
	if AllocateRouteSession(input).SessionKey == first.SessionKey {
		t.Fatal("agents not isolated")
	}
	input.AgentID = "main"
	for _, mutate := range []func(*bus.InboundContext){func(c *bus.InboundContext) { c.SenderID = "outsider" }, func(c *bus.InboundContext) { c.Account = "other" }, func(c *bus.InboundContext) { c.ChatID = "mqtt:new" }, func(c *bus.InboundContext) { c.ChatType = "group" }, func(c *bus.InboundContext) { c.TopicID = "1" }, func(c *bus.InboundContext) { c.SpaceID = "1" }, func(c *bus.InboundContext) { c.Channel = "telegram" }} {
		x := input
		mutate(&x.Context)
		if IsSharedSessionKey(AllocateRouteSession(x).SessionKey) {
			t.Fatal("non-exact member admitted")
		}
	}
}
func TestSharedTurnGateHistoryAndSummarySerialization(t *testing.T) {
	var g TurnGate
	var wg sync.WaitGroup
	store := NewSessionManager(t.TempDir())
	key := BuildSharedSessionKey("main", "owner")
	store.GetOrCreate(key)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := g.Acquire(context.Background(), key)
			if err != nil {
				t.Error(err)
				return
			}
			defer release()
			h := store.GetHistory(key)
			time.Sleep(time.Millisecond)
			store.SetHistory(key, append(h, providers.Message{Role: "user", Content: "turn"}))
			store.SetSummary(key, "serialized")
		}()
	}
	wg.Wait()
	if len(store.GetHistory(key)) != 20 || store.GetSummary(key) != "serialized" {
		t.Fatal("lost history or summary")
	}
	release, _ := g.Acquire(context.Background(), key)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { _, err := g.Acquire(ctx, key); done <- err }()
	if <-done == nil {
		t.Fatal("cancellation ignored")
	}
	release()
	if len(g.entries) != 0 {
		t.Fatal("lock entries leaked")
	}
}
