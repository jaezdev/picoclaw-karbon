package config

import (
	"encoding/json"
	"testing"
)

func TestSharedGroupsValidationAndRoundTrip(t *testing.T) {
	good := SessionConfig{SharedGroups: []SharedSessionGroup{{ID: "owner", Members: []SharedSessionMember{{Channel: "telegram", Account: "default", ChatType: "direct", ChatID: "123", SenderID: "123"}, {Channel: "mqtt", Account: "default", ChatType: "direct", ChatID: "mqtt:job", SenderID: "job"}}}}}
	if err := good.ValidateSharedGroups(); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(Config{Session: good})
	if err != nil {
		t.Fatal(err)
	}
	var restored Config
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if len(restored.Session.SharedGroups) != 1 {
		t.Fatal("groups dropped")
	}
	for _, mutate := range []func(*SessionConfig){func(s *SessionConfig) { s.SharedGroups = append(s.SharedGroups, s.SharedGroups[0]) }, func(s *SessionConfig) { s.SharedGroups[0].Members[1] = s.SharedGroups[0].Members[0] }, func(s *SessionConfig) { s.SharedGroups[0].Members[0].ChatType = "group" }, func(s *SessionConfig) { s.SharedGroups[0].Members[0].SenderID = "*" }, func(s *SessionConfig) { s.SharedGroups[0].Members[0].Channel = "system" }, func(s *SessionConfig) { s.SharedGroups[0].Members[0].Account = " default" }} {
		var x Config
		json.Unmarshal(raw, &x)
		mutate(&x.Session)
		if x.Session.ValidateSharedGroups() == nil {
			t.Fatal("invalid config accepted")
		}
	}
}
