package config

import (
	"fmt"
	"regexp"
	"strings"
)

// SharedSessionMember is an exact transport identity, not an authorization rule.
// All fields are required; identity_links and wildcard matching do not apply.
type SharedSessionMember struct {
	Channel  string `json:"channel"`
	Account  string `json:"account"`
	ChatType string `json:"chat_type"`
	ChatID   string `json:"chat_id"`
	SenderID string `json:"sender_id"`
}
type SharedSessionGroup struct {
	ID      string                `json:"id"`
	Members []SharedSessionMember `json:"members"`
}

var sharedGroupID = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

func (s *SessionConfig) ValidateSharedGroups() error {
	ids := map[string]bool{}
	members := map[SharedSessionMember]bool{}
	for _, g := range s.SharedGroups {
		if !sharedGroupID.MatchString(g.ID) || ids[g.ID] || len(g.Members) < 2 {
			return fmt.Errorf("invalid or duplicate shared session group %q (requires at least two members)", g.ID)
		}
		ids[g.ID] = true
		for _, m := range g.Members {
			if m.Channel == "" || m.Account == "" || m.ChatType != "direct" || m.ChatID == "" || m.SenderID == "" ||
				members[m] {
				return fmt.Errorf("invalid or overlapping member in shared session group %q", g.ID)
			}
			for _, field := range []string{m.Channel, m.Account, m.ChatID, m.SenderID} {
				if strings.TrimSpace(field) != field || strings.ContainsAny(field, "*?\n\r\t") {
					return fmt.Errorf("non-exact member in shared session group %q", g.ID)
				}
			}
			if m.Channel != strings.ToLower(m.Channel) || m.Account != strings.ToLower(m.Account) {
				return fmt.Errorf("noncanonical member in shared session group %q", g.ID)
			}
			// Reserved internal channels must never become shared ingress.
			if m.Channel == "system" || m.Channel == "cli" {
				return fmt.Errorf("internal channel in shared session group %q", g.ID)
			}
			members[m] = true
		}
	}
	return nil
}

// CloneSharedSessionGroups prevents route/turn snapshots from sharing mutable selectors.
func CloneSharedSessionGroups(groups []SharedSessionGroup) []SharedSessionGroup {
	if groups == nil {
		return nil
	}
	cloned := make([]SharedSessionGroup, len(groups))
	for i, group := range groups {
		cloned[i] = group
		cloned[i].Members = append([]SharedSessionMember(nil), group.Members...)
	}
	return cloned
}
