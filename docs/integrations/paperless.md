# Paperless document archival

The gateway can archive received PDFs and raster images without an LLM call.
Capture runs in the shared channel intake path after the existing sender checks.
The original message still reaches the agent. Outbound tool images, historical
sessions, and links in message text are not captured.

## Configuration

Disabled by default. Add this block beneath `tools` in the configuration file:

```json
"paperless": {
  "enabled": true,
  "auto_archive": true,
  "base_url": "http://paperless:8000",
  "public_url": "https://paperless.home.solucional.com",
  "token_file": "/root/.picoclaw/paperless/token",
  "queue_dir": "/root/.picoclaw/paperless/queue"
}
```

Use a non-staff Paperless service account with an unusable password and only
`documents.add_document` and `documents.view_paperlesstask`. Documents belong to
that account. Paperless administrators retain access; other users need explicit
permissions. No document-reading, editing, deletion, or user-management permission
is required for the integration. The tasks API also exposes unowned system tasks
to non-staff accounts; the worker requests only the exact IDs it submitted.

Store the API token in a mode-0600 file within a private directory. It is read for
each request, so replacing it takes effect without restarting the gateway. Do not
put the token into prompts, workspace instructions, configuration JSON, or logs.
Use an internal container network or HTTPS for the API connection. HTTP redirects
are rejected to prevent credential forwarding.

## Intake, processing, and receipts

Telegram documents and photos resolve through the existing media store. Pico web
chat inline image data is decoded directly. Other channels use their existing
attachment support and limits; this feature does not add new upload formats to
their user interfaces. Actual file bytes determine PDF/image eligibility.
Known PDF/image names containing invalid data produce a failure receipt; unrelated
audio, video, and other files are ignored. Office conversion is not included.

Each attachment is copied to a mode-0600 queue blob, hashed, and committed to disk
before agent processing. A single background worker streams uploads with a known
multipart content length, records the returned task ID, and polls the Paperless
API v10 task response until OCR completes. A successful HTTP upload is not a saved
document confirmation.

The queue survives source-file cleanup and gateway restarts. Completed blobs are
removed; durable job records remain for duplicate suppression. Network errors,
rate limiting, and server failures retry with exponential backoff up to five
minutes. Authentication/validation errors and OCR failures retain the file as a
failed job for operator action. Nothing silently expires from the failed queue.

Each incoming message/album receives a final receipt in its original chat with
saved-document links, duplicate status, or errors. If processing takes over thirty
seconds, one preliminary queued/processing receipt is sent, followed by the final
result. Message redelivery does not ordinarily repeat uploads or receipts. A
receipt can repeat if the process stops after sending but before recording that
delivery; Paperless's checksum check handles an ambiguous upload response.

Intake staging failures are logged and annotated in the message for the agent to
report. Normal conversation continues. No source file is deleted by this feature;
the original channel's normal temporary-media cleanup remains responsible for it.

## Agent tools and recovery

- `paperless_save(file_path, title?)` copies a requested local file to the same
  queue. Existing workspace/read-path restrictions apply. It returns a job record,
  not an OCR-completion promise. Already captured inbound attachments should not
  be uploaded again.
- `paperless_status(job_id, retry?)` returns durable progress. Job IDs may be the
  full hash or an unambiguous prefix of at least twelve characters. After fixing
  an expired token or other failure, `retry=true` resubmits the retained file and
  re-enables the final batch receipt.

Queue layout: `jobs/<hash>.json`, `blobs/<hash>`, and `batches/<hash>.json`. Batch
records contain routing metadata for the original chat. Protect this directory
along with the token. Include it in the existing persistent PicoClaw data backup.
Do not edit queue files while the gateway is running. Restore the token and the
entire queue from the same backup, then restart the gateway to resume pending
uploads and unsent receipts. Review failed jobs before manually discarding data.

Both the gateway and web launcher must be rebuilt after changing the Go config
schema, so console configuration saves retain the new fields. No frontend form
change is required; the raw configuration editor supports this block.

## Validation

Run the focused Go tests for `pkg/paperless`, `pkg/channels`, `pkg/config`,
`pkg/tools`, `pkg/gateway`, and the web API/middleware. The optional
`TestLivePaperless` test uses PDF/PNG/JPEG fixtures, verifies task polling and
album links after queue reopen, and checks that the service account cannot read
the document archive. It never sends a real chat message.

On the homelab, deployment backups, the previous binaries, and operations notes
are stored under `/srv/containers/picoclaw/data/deploy/paperless-<timestamp>`.
Stop the PicoClaw user service before restoring the old binaries/configuration.
Retain its Paperless token and queue until all pending work is accounted for.
