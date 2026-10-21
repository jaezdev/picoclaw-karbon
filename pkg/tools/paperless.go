package tools

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/sipeed/picoclaw/pkg/paperless"
	fstools "github.com/sipeed/picoclaw/pkg/tools/fs"
)

type PaperlessTool struct {
	service    *paperless.Service
	status     bool
	workspace  string
	restrict   bool
	allowPaths []*regexp.Regexp
}

func NewPaperlessTools(s *paperless.Service, workspace string, restrict bool, allow []*regexp.Regexp) []Tool {
	return []Tool{&PaperlessTool{service: s, workspace: workspace, restrict: restrict, allowPaths: allow}, &PaperlessTool{service: s, status: true}}
}
func (t *PaperlessTool) Name() string {
	if t.status {
		return "paperless_status"
	}
	return "paperless_save"
}
func (t *PaperlessTool) Description() string {
	if t.status {
		return "Check a Paperless archival job. Set retry=true to retry a failed job after resolving its cause. Queued and processing are not confirmation of a saved document."
	}
	return "Queue a local PDF or raster image for Paperless archival when requested. Incoming chat attachments are already queued automatically; do not upload them again. Returns job_id; use paperless_status to confirm processing completed."
}
func (t *PaperlessTool) Parameters() map[string]any {
	if t.status {
		return map[string]any{"type": "object", "properties": map[string]any{"job_id": map[string]any{"type": "string"}, "retry": map[string]any{"type": "boolean"}}, "required": []string{"job_id"}}
	}
	return map[string]any{"type": "object", "properties": map[string]any{"file_path": map[string]any{"type": "string"}, "title": map[string]any{"type": "string"}}, "required": []string{"file_path"}}
}
func (t *PaperlessTool) Execute(ctx context.Context, args map[string]any) *ToolResult {
	var j paperless.Job
	var err error
	if t.status {
		id, _ := args["job_id"].(string)
		if retry, _ := args["retry"].(bool); retry {
			j, err = t.service.Retry(id)
		} else {
			j, err = t.service.Status(id)
		}
	} else {
		path, _ := args["file_path"].(string)
		if path == "" {
			return ErrorResult("file_path is required")
		}
		path, err = fstools.ValidatePathWithAllowPaths(path, t.workspace, t.restrict, t.allowPaths)
		if err == nil {
			title, _ := args["title"].(string)
			j, err = t.service.EnqueuePath(path, title)
		}
	}
	if err != nil {
		return ErrorResult(err.Error())
	}
	data, err := json.Marshal(j)
	if err != nil {
		return ErrorResult("could not encode archival status")
	}
	return NewToolResult(string(data))
}
