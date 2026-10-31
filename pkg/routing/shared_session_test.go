package routing

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

func TestResolveRoutePreservesSharedGroupsWithDispatch(t *testing.T) {
	cfg := testConfig([]config.AgentConfig{{ID: "main", Default: true}, {ID: "support"}})
	cfg.Session.SharedGroups = []config.SharedSessionGroup{{ID: "owner", Members: []config.SharedSessionMember{
		{Channel: "telegram", Account: "default", ChatType: "direct", ChatID: "123", SenderID: "123"},
		{Channel: "mqtt", Account: "default", ChatType: "direct", ChatID: "job", SenderID: "n8n"},
	}}}
	cfg.Agents.Dispatch = &config.DispatchConfig{
		Rules: []config.DispatchRule{
			{
				Name:              "support",
				Agent:             "support",
				When:              config.DispatchSelector{Channel: "mqtt"},
				SessionDimensions: []string{"chat"},
			},
		},
	}
	r := NewRouteResolver(cfg)
	route := r.ResolveRoute(bus.InboundContext{Channel: "mqtt", ChatType: "direct", ChatID: "job", SenderID: "n8n"})
	if route.AgentID != "support" || len(route.SessionPolicy.SharedGroups) != 1 ||
		route.SessionPolicy.SharedGroups[0].ID != "owner" {
		t.Fatalf("shared policy lost: %+v", route)
	}
	route.SessionPolicy.SharedGroups[0].Members[0].SenderID = "mutated"
	if cfg.Session.SharedGroups[0].Members[0].SenderID != "123" {
		t.Fatal("route snapshot mutated config")
	}
}
