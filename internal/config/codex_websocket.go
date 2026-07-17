package config

import "time"

const (
	defaultCodexWebsocketSessionTTL  = 10 * time.Minute
	defaultCodexWebsocketMaxSessions = 128
	defaultCodexPreconnectMaxIdle    = 2
	defaultCodexPreconnectTTL        = 30 * time.Second
)

// CodexWebsocketRuntimeConfig is the normalized runtime view of the
// backward-compatible top-level YAML fields in SDKConfig.
type CodexWebsocketRuntimeConfig struct {
	PreferUpstream        bool
	SessionTTL            time.Duration
	MaxSessions           int
	SpeculativePreconnect bool
	GenerateFalseWarmup   bool
	PreconnectReplenish   bool
	PreconnectMaxIdle     int
	PreconnectTTL         time.Duration
	ClassifierModel       string
}

func (c *Config) NormalizedCodexWebsocketConfig() CodexWebsocketRuntimeConfig {
	result := CodexWebsocketRuntimeConfig{
		SessionTTL:        defaultCodexWebsocketSessionTTL,
		MaxSessions:       defaultCodexWebsocketMaxSessions,
		PreconnectMaxIdle: defaultCodexPreconnectMaxIdle,
		PreconnectTTL:     defaultCodexPreconnectTTL,
	}
	if c == nil {
		return result
	}
	result.PreferUpstream = c.CodexPreferUpstreamWebsockets
	result.SpeculativePreconnect = c.CodexWebsocketSpeculativePreconnect
	result.GenerateFalseWarmup = c.CodexWebsocketGenerateFalseWarmup
	result.PreconnectReplenish = c.CodexWebsocketPreconnectReplenish
	result.ClassifierModel = c.ClaudeCodeAutoModeClassifierModel
	if c.CodexWebsocketSessionTTLSeconds > 0 {
		result.SessionTTL = time.Duration(c.CodexWebsocketSessionTTLSeconds) * time.Second
	}
	if c.CodexWebsocketMaxSessions > 0 {
		result.MaxSessions = c.CodexWebsocketMaxSessions
	}
	if c.CodexWebsocketPreconnectMaxIdle > 0 {
		result.PreconnectMaxIdle = c.CodexWebsocketPreconnectMaxIdle
	}
	if c.CodexWebsocketPreconnectTTLSeconds > 0 {
		result.PreconnectTTL = time.Duration(c.CodexWebsocketPreconnectTTLSeconds) * time.Second
	}
	return result
}
