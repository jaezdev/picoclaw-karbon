package session

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestTurnGateSerializationAndCleanup(t *testing.T) {
	var gate TurnGate
	var wg sync.WaitGroup
	count := 0
	for range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := gate.Acquire(context.Background(), "shared")
			if err != nil {
				t.Error(err)
				return
			}
			count++
			release()
			release()
		}()
	}
	wg.Wait()
	if count != 30 {
		t.Fatalf("lost turns: %d", count)
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if len(gate.entries) != 0 {
		t.Fatal("idle entries leaked")
	}
}
func TestTurnGateCancellationAndIndependentKeys(t *testing.T) {
	var gate TurnGate
	release, err := gate.Acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err = gate.Acquire(ctx, "a"); err == nil {
		t.Fatal("blocked waiter ignored cancellation")
	}
	other, err := gate.Acquire(context.Background(), "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Acquire(ctx, "c"); err == nil {
		t.Fatal("pre-cancelled context acquired gate")
	}
	other()
	release()
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if len(gate.entries) != 0 {
		t.Fatal("cancelled entry leaked")
	}
}
