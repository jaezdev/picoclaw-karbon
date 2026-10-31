package session

import (
	"context"
	"sync"
)

// TurnGate serializes full history read/modify/write cycles, not just store writes.
// It is process-local; a session directory must have a single gateway writer.
// Idle entries are removed, so unbounded session IDs do not leak lock objects.
type TurnGate struct {
	mu      sync.Mutex
	entries map[string]*turnGateEntry
}
type turnGateEntry struct {
	token chan struct{}
	refs  int
}

func (g *TurnGate) Acquire(ctx context.Context, key string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g.mu.Lock()
	if g.entries == nil {
		g.entries = make(map[string]*turnGateEntry)
	}
	e := g.entries[key]
	if e == nil {
		e = &turnGateEntry{token: make(chan struct{}, 1)}
		g.entries[key] = e
	}
	e.refs++
	g.mu.Unlock()
	drop := func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		e.refs--
		if e.refs == 0 {
			delete(g.entries, key)
		}
	}
	select {
	case e.token <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-e.token; drop() }) }, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}
