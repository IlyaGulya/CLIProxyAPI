package helps

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestExtractClaudeCodeSessionIDFromPayloadJSON(t *testing.T) {
	payload := []byte(`{"metadata":{"user_id":"{\"device_id\":\"d\",\"session_id\":\"cache-session-1\"}"}}`)
	got := ExtractClaudeCodeSessionID(context.Background(), payload, nil)
	if got != "cache-session-1" {
		t.Fatalf("ExtractClaudeCodeSessionID() = %q, want cache-session-1", got)
	}
}

func TestClaudeCodePromptCacheIDDeterministicWithoutStore(t *testing.T) {
	first := ClaudeCodePromptCacheID(" codex ", " gpt-5.6-luna ", " auth-a ", "session-a")
	second := ClaudeCodePromptCacheID("codex", "gpt-5.6-luna", "auth-a", "session-a")
	if first == "" || first != second {
		t.Fatalf("deterministic cache ID mismatch: first=%q second=%q", first, second)
	}
	if strings.Contains(first, "auth-a") || strings.Contains(first, "session-a") || strings.Contains(first, "gpt-5.6-luna") {
		t.Fatalf("cache ID leaked raw identity: %q", first)
	}
	if first == ClaudeCodePromptCacheID("codex", "gpt-5.6-luna", "auth-b", "session-a") ||
		first == ClaudeCodePromptCacheID("codex", "gpt-5.6-sol", "auth-a", "session-a") ||
		first == ClaudeCodePromptCacheID("codex", "gpt-5.6-luna", "auth-a", "session-b") ||
		first == ClaudeCodePromptCacheID("openai", "gpt-5.6-luna", "auth-a", "session-a") {
		t.Fatalf("cache ID did not isolate provider/model/auth/session: %q", first)
	}
}

func TestExtractClaudeCodeSessionIDFromHeader(t *testing.T) {
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	ginCtx.Request.Header.Set(ClaudeCodeSessionHeader, "header-session-1")
	ctx := context.WithValue(context.Background(), "gin", ginCtx)

	got := ExtractClaudeCodeSessionID(ctx, []byte(`{"model":"gpt-5.4"}`), nil)
	if got != "header-session-1" {
		t.Fatalf("ExtractClaudeCodeSessionID() = %q, want header-session-1", got)
	}
}

func TestClaudeCodePromptCacheStableAcrossRequests(t *testing.T) {
	ctx := context.Background()
	payload := []byte(`{"cache_control":{"type":"automatic"},"metadata":{"user_id":"{\"session_id\":\"cache-session-2\"}"},"messages":[{"role":"user","content":"hi"}]}`)
	first, ok, err := ClaudeCodePromptCache(ctx, "grok-composer-2.5-fast", payload, nil)
	if err != nil {
		t.Fatalf("ClaudeCodePromptCache first error: %v", err)
	}
	if !ok || first.ID == "" {
		t.Fatalf("ClaudeCodePromptCache first = %#v, ok=%v, want cached id", first, ok)
	}
	second, ok, err := ClaudeCodePromptCache(ctx, "grok-composer-2.5-fast", payload, nil)
	if err != nil {
		t.Fatalf("ClaudeCodePromptCache second error: %v", err)
	}
	if !ok || second.ID != first.ID {
		t.Fatalf("second cache id = %q, want %q", second.ID, first.ID)
	}
}

func TestClaudeCodePromptCacheIsolatesAuthAndModel(t *testing.T) {
	ctx := context.Background()
	payload := []byte(`{"cache_control":{"type":"automatic"},"metadata":{"user_id":"{\"session_id\":\"cache-isolation-session\"}"},"messages":[{"role":"user","content":"hi"}]}`)
	base, ok, errBase := ClaudeCodePromptCacheForAuth(ctx, "codex", "gpt-5.6-luna", "auth-a", payload, nil)
	if errBase != nil || !ok || base.ID == "" {
		t.Fatalf("base cache = %#v, ok=%v, err=%v", base, ok, errBase)
	}
	same, _, _ := ClaudeCodePromptCacheForAuth(ctx, "codex", "gpt-5.6-luna", "auth-a", payload, nil)
	otherAuth, _, _ := ClaudeCodePromptCacheForAuth(ctx, "codex", "gpt-5.6-luna", "auth-b", payload, nil)
	otherModel, _, _ := ClaudeCodePromptCacheForAuth(ctx, "codex", "gpt-5.6-sol", "auth-a", payload, nil)
	if same.ID != base.ID {
		t.Fatalf("same auth/model cache ID = %q, want %q", same.ID, base.ID)
	}
	if otherAuth.ID == base.ID || otherModel.ID == base.ID || otherAuth.ID == otherModel.ID {
		t.Fatalf("cache isolation failed: base=%q other_auth=%q other_model=%q", base.ID, otherAuth.ID, otherModel.ID)
	}
}

func TestExtractClaudeCodeSessionIDPrefersHeaderOverPayload(t *testing.T) {
	payload := []byte(`{"metadata":{"user_id":"{"session_id":"payload-session"}"}}`)
	headers := http.Header{}
	headers.Set(ClaudeCodeSessionHeader, "header-session")

	got := ExtractClaudeCodeSessionID(context.Background(), payload, headers)
	if got != "header-session" {
		t.Fatalf("ExtractClaudeCodeSessionID() = %q, want header-session", got)
	}
}
