package observability

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const eventJournalMaxBytes int64 = 16 << 20

type eventJournal struct {
	mu   sync.Mutex
	path string
	file *os.File
	size int64
}

func openEventJournal(path string) (*eventJournal, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		return nil, fmt.Errorf("create event journal directory: %w", errMkdir)
	}
	file, errOpen := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if errOpen != nil {
		return nil, fmt.Errorf("open event journal: %w", errOpen)
	}
	if errChmod := file.Chmod(0o600); errChmod != nil {
		_ = file.Close()
		return nil, fmt.Errorf("protect event journal: %w", errChmod)
	}
	info, _ := file.Stat()
	journal := &eventJournal{path: path, file: file}
	if info != nil {
		journal.size = info.Size()
	}
	return journal, nil
}

func (j *eventJournal) record(event WebsocketEvent, fields map[string]any) {
	if j == nil {
		return
	}
	record := map[string]any{
		"schema": 1, "at_unix_ms": time.Now().UnixMilli(), "name": boundedEnum(event.Name),
	}
	keyMaterial := event.RootCorrelation + "\x00" + event.ExecutionCorrelation
	if keyMaterial == "\x00" {
		keyMaterial = text(fields["session_id"])
	}
	if keyMaterial != "" && keyMaterial != "\x00" {
		sum := sha256.Sum256([]byte(keyMaterial))
		record["request_key"] = hex.EncodeToString(sum[:12])
	}
	for _, key := range eventJournalFieldAllowlist {
		if value, exists := fields[key]; exists && journalScalar(value) {
			record[key] = value
		}
	}
	payload, errMarshal := json.Marshal(record)
	if errMarshal != nil {
		return
	}
	payload = append(payload, '\n')
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return
	}
	if j.size+int64(len(payload)) > eventJournalMaxBytes {
		if !j.rotateLocked() {
			return
		}
	}
	written, errWrite := j.file.Write(payload)
	if errWrite == nil {
		j.size += int64(written)
	}
}

func (j *eventJournal) rotateLocked() bool {
	if errClose := j.file.Close(); errClose != nil {
		return false
	}
	_ = os.Remove(j.path + ".1")
	if errRename := os.Rename(j.path, j.path+".1"); errRename != nil && !os.IsNotExist(errRename) {
		return false
	}
	file, errOpen := os.OpenFile(j.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if errOpen != nil {
		j.file = nil
		return false
	}
	j.file = file
	j.size = 0
	return true
}

func (j *eventJournal) close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.file == nil {
		return nil
	}
	errClose := j.file.Close()
	j.file = nil
	return errClose
}

func journalScalar(value any) bool {
	switch value.(type) {
	case string, bool, int, int32, int64, uint, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

var eventJournalFieldAllowlist = []string{
	"role", "model", "connection_source", "source_format", "reason", "boundary", "chain_source",
	"incremental_reset_reason", "trigger", "prompt_cache_ttl", "prompt_cache_decision", "success", "reused",
	"overflow", "incremental", "fresh_response_chain", "has_previous_response", "prompt_cache_enabled",
	"compaction_applied", "duration_us", "elapsed_us", "since_send_us", "wait_us", "age_us", "translation_us",
	"downstream_blocked_us", "connection_age_us", "attempt", "transport_retries", "close_code", "status", "bytes",
	"client_body_bytes", "upstream_body_bytes", "upstream_bytes", "upstream_frames", "translated_chunks", "input_items",
	"instructions_bytes", "tools_count", "pool_idle", "pool_dialing", "input_tokens", "output_tokens", "reasoning_tokens",
	"cached_tokens", "cache_read_tokens", "cache_creation_tokens", "total_tokens", "response_service_tier",
	"tool_calls_started", "tool_calls_completed", "tool_calls_incomplete", "repaired_tool_uses",
	"separated_assistant_messages", "compaction_retained_messages", "compaction_retained_images",
	"compaction_dropped_items", "compaction_retained_tokens", "adaptive_target", "adaptive_hit_rate_basis_points",
	"counterfactual_wait_us", "predicted_savings_bytes",
}
