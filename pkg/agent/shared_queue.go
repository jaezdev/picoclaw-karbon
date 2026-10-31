package agent

import (
	"context"
	"fmt"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/logger"
)

type sharedTurnContextKey struct{}

// enqueueSharedTurn preserves arrival order and bounds pending work per shared
// session. One worker per group competes for a global slot, never all waiters.
func (al *AgentLoop) enqueueSharedTurn(ctx context.Context, key string, msg bus.InboundMessage) {
	al.sharedQueueMu.Lock()
	if al.sharedQueues == nil {
		al.sharedQueues = make(map[string]chan bus.InboundMessage)
	}
	queue := al.sharedQueues[key]
	start := queue == nil
	if start {
		queue = make(chan bus.InboundMessage, 64)
		al.sharedQueues[key] = queue
	}
	select {
	case queue <- msg:
	default:
		al.sharedQueueMu.Unlock()
		al.maybePublishError(
			ctx,
			msg.Channel,
			msg.ChatID,
			key,
			fmt.Errorf("shared conversation queue full; please retry"),
		)
		return
	}
	al.sharedQueueMu.Unlock()
	if !start {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.RecoverPanicNoExit(r)
			}
			al.sharedQueueMu.Lock()
			if al.sharedQueues[key] == queue {
				delete(al.sharedQueues, key)
			}
			al.sharedQueueMu.Unlock()
		}()
		for {
			var m bus.InboundMessage
			select {
			case <-ctx.Done():
				return
			case m = <-queue:
			}
			select {
			case <-ctx.Done():
				return
			case al.workerSem <- struct{}{}:
			}
			func() { defer func() { <-al.workerSem }(); al.processMessageSync(ctx, m) }()
			al.sharedQueueMu.Lock()
			if len(queue) == 0 {
				delete(al.sharedQueues, key)
				al.sharedQueueMu.Unlock()
				return
			}
			al.sharedQueueMu.Unlock()
		}
	}()
}
