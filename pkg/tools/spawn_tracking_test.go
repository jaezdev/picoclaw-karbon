package tools

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type trackingSpawner struct {
	release chan struct{}
	result  *ToolResult
	err     error
}

func (s *trackingSpawner) SpawnSubTurn(ctx context.Context, cfg SubTurnConfig) (*ToolResult, error) {
	<-s.release
	return s.result, s.err
}

func TestSpawnTool_StatusLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		result       *ToolResult
		err          error
	}{
		{"success", "completed", NewToolResult("tracking done"), nil},
		{"error", "failed", nil, errors.New("spawner failed")},
		{"tool error", "failed", ErrorResult("tool failed"), nil},
		{"cancel", "canceled", nil, context.Canceled},
		{"result cancel", "canceled", ErrorResult("canceled").WithError(context.Canceled), nil},
		{"deadline", "canceled", nil, context.DeadlineExceeded},
		{"result deadline", "canceled", ErrorResult("deadline").WithError(context.DeadlineExceeded), nil},
		{"nil result", "failed", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := NewSubagentManager(nil, "test", t.TempDir())
			tool := NewSpawnTool(manager)
			status := NewSpawnStatusTool(manager)
			spawner := &trackingSpawner{make(chan struct{}), tc.result, tc.err}
			tool.SetSpawner(spawner)
			ctx := WithToolContext(context.Background(), "telegram", "chat-a")
			delivered := make(chan string, 1)
			ack := tool.ExecuteAsync(ctx, map[string]any{"task": "test", "label": "tracked", "agent_id": "worker"}, func(_ context.Context, _ *ToolResult) {
				snapshot, _ := manager.GetTaskCopy("subagent-1")
				delivered <- snapshot.Status
			})
			if !ack.Async || !strings.Contains(ack.ForLLM, "subagent-1") {
				t.Fatalf("missing task ID: %+v", ack)
			}
			snapshot, ok := manager.GetTaskCopy("subagent-1")
			if !ok || snapshot.Status != "running" || snapshot.AgentID != "worker" || snapshot.Label != "tracked" || snapshot.OriginChatID != "chat-a" {
				t.Fatalf("bad initial task: %+v", snapshot)
			}
			if got := status.Execute(ctx, map[string]any{}).ForLLM; !strings.Contains(got, "status=running") {
				t.Fatalf("not visible: %s", got)
			}
			other := WithToolContext(context.Background(), "telegram", "chat-b")
			if got := status.Execute(other, map[string]any{}).ForLLM; strings.Contains(got, "subagent-1") {
				t.Fatalf("task leaked: %s", got)
			}
			if got := status.Execute(other, map[string]any{"task_id": "subagent-1"}); !got.IsError {
				t.Fatal("cross-chat task lookup allowed")
			}
			otherChannel := WithToolContext(context.Background(), "mqtt", "chat-a")
			if got := status.Execute(otherChannel, map[string]any{}).ForLLM; strings.Contains(got, "subagent-1") {
				t.Fatalf("task leaked across channels: %s", got)
			}
			if got := status.Execute(otherChannel, map[string]any{"task_id": "subagent-1"}); !got.IsError {
				t.Fatal("cross-channel task lookup allowed")
			}
			close(spawner.release)
			select {
			case got := <-delivered:
				if got != tc.status {
					t.Fatalf("status at callback = %s, want %s", got, tc.status)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("callback timed out")
			}
			got := status.Execute(ctx, map[string]any{"task_id": "subagent-1"})
			if !strings.Contains(got.ForLLM, "status="+tc.status) {
				t.Fatalf("bad final status: %s", got.ForLLM)
			}
		})
	}
}

func TestSpawnTool_ConcurrentTracking(t *testing.T) {
	manager := NewSubagentManager(nil, "test", t.TempDir())
	tool := NewSpawnTool(manager)
	release := make(chan struct{})
	tool.SetSpawner(&trackingSpawner{release, NewToolResult("done"), nil})
	ctx := WithToolContext(context.Background(), "telegram", "chat-a")
	done := make(chan struct{}, 20)
	for i := 0; i < 20; i++ {
		go func() {
			tool.ExecuteAsync(ctx, map[string]any{"task": "test"}, func(context.Context, *ToolResult) { done <- struct{}{} })
		}()
	}
	close(release)
	for i := 0; i < 20; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout")
		}
	}
	tasks := manager.ListTaskCopies()
	if len(tasks) != 20 {
		t.Fatalf("got %d tasks", len(tasks))
	}
	for _, task := range tasks {
		if task.Status != "completed" {
			t.Fatalf("unfinished task: %+v", task)
		}
	}
}
