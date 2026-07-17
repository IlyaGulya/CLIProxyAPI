package helps

import (
	"context"
	"fmt"
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

type ClaudePromptCachePolicyError struct{ message string }

func (e ClaudePromptCachePolicyError) Error() string { return e.message }

func IsClaudePromptCachePolicyError(err error) bool {
	_, ok := err.(ClaudePromptCachePolicyError)
	return ok
}

func claudePromptCachePolicyError(format string, args ...any) error {
	return ClaudePromptCachePolicyError{message: fmt.Sprintf(format, args...)}
}

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
	return ClaudeCodePromptCacheForAuth(ctx, "codex", modelName, "", payload, headers)
}

// ClaudePromptCacheDecision exposes only privacy-safe policy metadata for
// observability; it never returns cache keys, session IDs, or prompt content.
func ClaudePromptCacheDecision(payload []byte) (bool, string, string) {
	enabled, ttl, err := claudePromptCachePolicy(payload)
	if err != nil {
		return false, "", "invalid"
	}
	if !enabled {
		return false, "", "not_requested"
	}
	if ttl >= time.Hour {
		return true, "1h", "requested"
	}
	return true, "5m", "requested"
}

// ClaudeCodePromptCacheForAuth maps a Claude Code root/model/auth scope to a
// stable upstream prompt_cache_key. Auth isolation prevents cache identity
// from crossing credentials when the scheduler routes equivalent sessions.
func ClaudeCodePromptCacheForAuth(ctx context.Context, provider, modelName, authID string, payload []byte, headers http.Header) (CodexCache, bool, error) {
	enabled, ttl, errPolicy := claudePromptCachePolicy(payload)
	if errPolicy != nil {
		return CodexCache{}, false, errPolicy
	}
	if !enabled {
		return CodexCache{}, false, nil
	}
	sessionID := ExtractClaudeCodeSessionID(ctx, payload, headers)
	if sessionID == "" {
		return CodexCache{}, false, nil
	}
	key := CodexPromptCacheKey(modelName, "claude:"+strings.TrimSpace(provider)+":"+strings.TrimSpace(authID)+":"+sessionID)
	if cache, ok, errCache := GetCodexCacheRequired(ctx, key); errCache != nil {
		return CodexCache{}, false, errCache
	} else if ok {
		cache.Expire = time.Now().Add(ttl)
		if errSet := SetCodexCacheRequired(ctx, key, cache); errSet != nil {
			return CodexCache{}, false, errSet
		}
		return cache, true, nil
	}
	cache := CodexCache{ID: ClaudeCodePromptCacheID(provider, modelName, authID, sessionID), Expire: time.Now().Add(ttl)}
	if errSet := SetCodexCacheRequired(ctx, key, cache); errSet != nil {
		return CodexCache{}, false, errSet
	}
	return cache, true, nil
}

func claudePromptCachePolicy(payload []byte) (bool, time.Duration, error) {
	root := gjson.ParseBytes(payload)
	top := root.Get("cache_control")
	topEnabled := top.Exists() && top.Type != gjson.Null
	topTTL := 5 * time.Minute
	if topEnabled {
		topType := strings.ToLower(strings.TrimSpace(top.Get("type").String()))
		if topType != "ephemeral" && topType != "automatic" {
			return false, 0, claudePromptCachePolicyError("invalid top-level cache_control type %q", topType)
		}
		var err error
		topTTL, err = claudeCacheControlTTL(top)
		if err != nil {
			return false, 0, err
		}
	}

	explicitCount := 0
	maxExplicitTTL := time.Duration(0)
	lastEligibleControl := gjson.Result{}
	eligibleCount := 0
	visit := func(block gjson.Result) error {
		eligibleCount++
		control := block.Get("cache_control")
		lastEligibleControl = control
		if !control.Exists() || control.Type == gjson.Null {
			return nil
		}
		explicitCount++
		if strings.ToLower(strings.TrimSpace(control.Get("type").String())) != "ephemeral" {
			return claudePromptCachePolicyError("invalid block cache_control type %q", control.Get("type").String())
		}
		ttl, err := claudeCacheControlTTL(control)
		if err != nil {
			return err
		}
		if ttl > maxExplicitTTL {
			maxExplicitTTL = ttl
		}
		return nil
	}
	for _, tool := range root.Get("tools").Array() {
		if err := visit(tool); err != nil {
			return false, 0, err
		}
	}
	system := root.Get("system")
	if system.Type == gjson.String {
		eligibleCount++
		lastEligibleControl = gjson.Result{}
	} else {
		for _, block := range system.Array() {
			if err := visit(block); err != nil {
				return false, 0, err
			}
		}
	}
	for _, message := range root.Get("messages").Array() {
		content := message.Get("content")
		if content.Type == gjson.String {
			eligibleCount++
			lastEligibleControl = gjson.Result{}
			continue
		}
		for _, block := range content.Array() {
			switch block.Get("type").String() {
			case "thinking", "redacted_thinking":
				continue
			}
			if err := visit(block); err != nil {
				return false, 0, err
			}
		}
	}
	if !topEnabled {
		if explicitCount == 0 {
			return false, 0, nil
		}
		return true, maxExplicitTTL, nil
	}
	if eligibleCount == 0 {
		return false, 0, nil
	}
	if lastEligibleControl.Exists() && lastEligibleControl.Type != gjson.Null {
		lastTTL, err := claudeCacheControlTTL(lastEligibleControl)
		if err != nil {
			return false, 0, err
		}
		if lastTTL != topTTL {
			return false, 0, claudePromptCachePolicyError("automatic cache_control TTL conflicts with the last cacheable block")
		}
		return true, topTTL, nil
	}
	if explicitCount >= 4 {
		return false, 0, claudePromptCachePolicyError("automatic cache_control exceeds the four-breakpoint limit")
	}
	return true, topTTL, nil
}

func claudeCacheControlTTL(control gjson.Result) (time.Duration, error) {
	switch strings.ToLower(strings.TrimSpace(control.Get("ttl").String())) {
	case "", "5m":
		return 5 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	default:
		return 0, claudePromptCachePolicyError("invalid cache_control TTL %q", control.Get("ttl").String())
	}
}

// ClaudeCodePromptCacheID is stable across proxy processes while remaining
// opaque and isolated by upstream model, credential, and Claude session.
func ClaudeCodePromptCacheID(provider, modelName, authID, sessionID string) string {
	identity := strings.TrimSpace(provider) + "\x00" + strings.TrimSpace(modelName) + "\x00" + strings.TrimSpace(authID) + "\x00" + strings.TrimSpace(sessionID)
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("cli-proxy-api:claude-prompt-cache\x00"+identity)).String()
}
