package helps

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
)

const ClaudeCodeSessionHeader = "X-Claude-Code-Session-Id"
const ClaudeCodeAgentHeader = "X-Claude-Code-Agent-Id"
const ClaudeCodeWebsocketSessionPrefix = "claude-code:"

var claudeCodeSessionSuffixPattern = regexp.MustCompile(`_session_([a-f0-9-]+)$`)

// ExtractClaudeCodeSessionID resolves a Claude Code session ID, preferring X-Claude-Code-Session-Id over payload metadata.
func ExtractClaudeCodeSessionID(ctx context.Context, payload []byte, headers http.Header) string {
	if headers != nil {
		if sessionID := strings.TrimSpace(headers.Get(ClaudeCodeSessionHeader)); sessionID != "" {
			return sessionID
		}
	}
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			if sessionID := strings.TrimSpace(ginCtx.Request.Header.Get(ClaudeCodeSessionHeader)); sessionID != "" {
				return sessionID
			}
		}
	}
	return extractClaudeCodeSessionIDFromPayload(payload)
}

// ExtractClaudeCodeAgentID resolves the Claude Code agent ID from the request.
// The header is absent for the root agent.
func ExtractClaudeCodeAgentID(ctx context.Context, headers http.Header) string {
	if headers != nil {
		if agentID := strings.TrimSpace(headers.Get(ClaudeCodeAgentHeader)); agentID != "" {
			return agentID
		}
	}
	if ctx != nil {
		if ginCtx, ok := ctx.Value("gin").(*gin.Context); ok && ginCtx != nil && ginCtx.Request != nil {
			return strings.TrimSpace(ginCtx.Request.Header.Get(ClaudeCodeAgentHeader))
		}
	}
	return ""
}

// ClaudeCodeWebsocketSessionID returns a stable, agent-scoped execution
// session ID. Root and child agents intentionally use separate upstream
// websocket connections so parallel subagents cannot serialize on one socket.
func ClaudeCodeWebsocketSessionID(ctx context.Context, payload []byte, headers http.Header) string {
	rootSessionID := ExtractClaudeCodeSessionID(ctx, payload, headers)
	if rootSessionID == "" {
		return ""
	}
	agentID := ExtractClaudeCodeAgentID(ctx, headers)
	if agentID == "" {
		agentID = "main"
	}
	identity := rootSessionID + "\x00" + agentID
	return ClaudeCodeWebsocketSessionPrefix + uuid.NewSHA1(uuid.NameSpaceOID, []byte(identity)).String()
}

// ClaudeCodeCorrelationIDs returns privacy-safe, stable identifiers for
// joining root and agent request timelines without logging client session or
// agent IDs. The first value is shared by all agents under one root; the
// second identifies the individual root/agent execution.
func ClaudeCodeCorrelationIDs(ctx context.Context, payload []byte, headers http.Header) (string, string) {
	rootSessionID := ExtractClaudeCodeSessionID(ctx, payload, headers)
	if rootSessionID == "" {
		return "", ""
	}
	agentID := ExtractClaudeCodeAgentID(ctx, headers)
	if agentID == "" {
		agentID = "main"
	}
	rootCorrelation := "claude-root:" + uuid.NewSHA1(uuid.NameSpaceOID, []byte("root\x00"+rootSessionID)).String()
	executionCorrelation := "claude-exec:" + uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution\x00"+rootSessionID+"\x00"+agentID)).String()
	return rootCorrelation, executionCorrelation
}

func extractClaudeCodeSessionIDFromPayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	userID := gjson.GetBytes(payload, "metadata.user_id").String()
	if userID == "" {
		return ""
	}
	if matches := claudeCodeSessionSuffixPattern.FindStringSubmatch(userID); len(matches) >= 2 {
		return matches[1]
	}
	if len(userID) > 0 && userID[0] == '{' {
		return strings.TrimSpace(gjson.Get(userID, "session_id").String())
	}
	return ""
}

// ClaudeCodePromptCache maps a Claude Code session to a stable upstream prompt_cache_key.
func ClaudeCodePromptCache(ctx context.Context, modelName string, payload []byte, headers http.Header) (CodexCache, bool, error) {
	return ClaudeCodePromptCacheForAuth(ctx, modelName, "", payload, headers)
}

// ClaudeCodePromptCacheForAuth maps a Claude Code root/model/auth scope to a
// stable upstream prompt_cache_key. Auth isolation prevents cache identity
// from crossing credentials when the scheduler routes equivalent sessions.
func ClaudeCodePromptCacheForAuth(ctx context.Context, modelName string, authID string, payload []byte, headers http.Header) (CodexCache, bool, error) {
	sessionID := ExtractClaudeCodeSessionID(ctx, payload, headers)
	if sessionID == "" {
		return CodexCache{}, false, nil
	}
	key := CodexPromptCacheKey(modelName, "claude:"+strings.TrimSpace(authID)+":"+sessionID)
	if cache, ok, errCache := GetCodexCacheRequired(ctx, key); errCache != nil || ok {
		return cache, ok, errCache
	}
	cache := CodexCache{
		ID:     uuid.New().String(),
		Expire: time.Now().Add(1 * time.Hour),
	}
	if errSet := SetCodexCacheRequired(ctx, key, cache); errSet != nil {
		return CodexCache{}, false, errSet
	}
	return cache, true, nil
}
