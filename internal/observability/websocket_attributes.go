package observability

// Optional distinguishes an explicitly reported zero value from an attribute
// that was not measured for an event.
type Optional[T any] struct {
	Value T
	Set   bool
}

func Some[T any](value T) Optional[T] { return Optional[T]{Value: value, Set: true} }

// WebsocketAttributes is the typed, prompt-free transport telemetry contract.
// Conversion to dynamic OTEL/log attributes happens only at the encoder edge.
type WebsocketAttributes struct {
	SessionID               string
	Model                   string
	ConnectionSource        string
	OverflowBaseSource      string
	SourceFormat            string
	Reason                  string
	LastEventType           string
	Boundary                string
	SuppressionReason       string
	ChainSource             string
	IncrementalResetReason  string
	Trigger                 string
	ToolName                string
	PromptCacheScope        string
	PromptPrefixFingerprint string
	PromptCacheTTL          string
	PromptCacheDecision     string

	Success             Optional[bool]
	Reused              Optional[bool]
	Busy                Optional[bool]
	Overflow            Optional[bool]
	Incremental         Optional[bool]
	RateLimited         Optional[bool]
	DownstreamCommitted Optional[bool]
	ToolCallStarted     Optional[bool]
	ToolCallCompleted   Optional[bool]
	ToolCallInProgress  Optional[bool]
	Reserved            Optional[bool]
	GenerateFalseWarmup Optional[bool]
	FreshResponseChain  Optional[bool]
	HasPreviousResponse Optional[bool]
	ResponseIDPresent   Optional[bool]
	PromptCacheEnabled  Optional[bool]
	CompactionApplied   Optional[bool]

	DurationUS                 Optional[int64]
	ElapsedUS                  Optional[int64]
	SinceSendUS                Optional[int64]
	WaitUS                     Optional[int64]
	AgeUS                      Optional[int64]
	TranslationUS              Optional[int64]
	DownstreamBlockedUS        Optional[int64]
	FirstEventUS               Optional[int64]
	FirstReasoningDeltaUS      Optional[int64]
	FirstOutputTextDeltaUS     Optional[int64]
	ConnectionAgeUS            Optional[int64]
	ConnectionRequestCount     Optional[int64]
	Attempt                    Optional[int64]
	TransportRetries           Optional[int64]
	CloseCode                  Optional[int64]
	Status                     Optional[int64]
	Bytes                      Optional[int64]
	ClientBodyBytes            Optional[int64]
	UpstreamBodyBytes          Optional[int64]
	UpstreamBytes              Optional[int64]
	UpstreamFrames             Optional[int64]
	TranslatedChunks           Optional[int64]
	InputItems                 Optional[int64]
	InstructionsBytes          Optional[int64]
	ToolsCount                 Optional[int64]
	PoolIdle                   Optional[int64]
	PoolDialing                Optional[int64]
	InputTokens                Optional[int64]
	OutputTokens               Optional[int64]
	ReasoningTokens            Optional[int64]
	CachedTokens               Optional[int64]
	CacheReadTokens            Optional[int64]
	CacheCreationTokens        Optional[int64]
	TotalTokens                Optional[int64]
	ResponseServiceTier        string
	ToolCallsStarted           Optional[int64]
	ToolCallsCompleted         Optional[int64]
	ToolCallsIncomplete        Optional[int64]
	RepairedToolUses           Optional[int64]
	SeparatedAssistantMessages Optional[int64]
	CompactionRetainedMessages Optional[int64]
	CompactionRetainedImages   Optional[int64]
	CompactionDroppedItems     Optional[int64]
	CompactionRetainedTokens   Optional[int64]
	AdaptiveTarget             Optional[int64]
	AdaptiveHitRateBasisPoints Optional[int64]
	CounterfactualWaitUS       Optional[int64]
	PredictedSavingsBytes      Optional[int64]
}

func (a WebsocketAttributes) fields() map[string]any {
	fields := make(map[string]any)
	for key, value := range map[string]string{
		"session_id": a.SessionID, "model": a.Model, "connection_source": a.ConnectionSource,
		"overflow_base_source": a.OverflowBaseSource, "source_format": a.SourceFormat, "reason": a.Reason,
		"last_event_type": a.LastEventType, "boundary": a.Boundary, "suppression_reason": a.SuppressionReason,
		"chain_source": a.ChainSource, "incremental_reset_reason": a.IncrementalResetReason, "trigger": a.Trigger,
		"tool_name":          a.ToolName,
		"prompt_cache_scope": a.PromptCacheScope, "prompt_prefix_fingerprint": a.PromptPrefixFingerprint,
		"prompt_cache_ttl": a.PromptCacheTTL, "prompt_cache_decision": a.PromptCacheDecision,
	} {
		if value != "" {
			fields[key] = value
		}
	}
	addOptional(fields, "success", a.Success)
	addOptional(fields, "reused", a.Reused)
	addOptional(fields, "busy", a.Busy)
	addOptional(fields, "overflow", a.Overflow)
	addOptional(fields, "incremental", a.Incremental)
	addOptional(fields, "rate_limited", a.RateLimited)
	addOptional(fields, "downstream_committed", a.DownstreamCommitted)
	addOptional(fields, "tool_call_started", a.ToolCallStarted)
	addOptional(fields, "tool_call_completed", a.ToolCallCompleted)
	addOptional(fields, "tool_call_in_progress", a.ToolCallInProgress)
	addOptional(fields, "reserved", a.Reserved)
	addOptional(fields, "generate_false_warmup", a.GenerateFalseWarmup)
	addOptional(fields, "fresh_response_chain", a.FreshResponseChain)
	addOptional(fields, "has_previous_response", a.HasPreviousResponse)
	addOptional(fields, "response_id_present", a.ResponseIDPresent)
	addOptional(fields, "prompt_cache_enabled", a.PromptCacheEnabled)
	addOptional(fields, "compaction_applied", a.CompactionApplied)
	addOptional(fields, "duration_us", a.DurationUS)
	addOptional(fields, "elapsed_us", a.ElapsedUS)
	addOptional(fields, "since_send_us", a.SinceSendUS)
	addOptional(fields, "wait_us", a.WaitUS)
	addOptional(fields, "age_us", a.AgeUS)
	addOptional(fields, "translation_us", a.TranslationUS)
	addOptional(fields, "downstream_blocked_us", a.DownstreamBlockedUS)
	addOptional(fields, "first_event_us", a.FirstEventUS)
	addOptional(fields, "first_reasoning_delta_us", a.FirstReasoningDeltaUS)
	addOptional(fields, "first_output_text_delta_us", a.FirstOutputTextDeltaUS)
	addOptional(fields, "connection_age_us", a.ConnectionAgeUS)
	addOptional(fields, "connection_request_count", a.ConnectionRequestCount)
	addOptional(fields, "attempt", a.Attempt)
	addOptional(fields, "transport_retries", a.TransportRetries)
	addOptional(fields, "close_code", a.CloseCode)
	addOptional(fields, "status", a.Status)
	addOptional(fields, "bytes", a.Bytes)
	addOptional(fields, "client_body_bytes", a.ClientBodyBytes)
	addOptional(fields, "upstream_body_bytes", a.UpstreamBodyBytes)
	addOptional(fields, "upstream_bytes", a.UpstreamBytes)
	addOptional(fields, "upstream_frames", a.UpstreamFrames)
	addOptional(fields, "translated_chunks", a.TranslatedChunks)
	addOptional(fields, "input_items", a.InputItems)
	addOptional(fields, "instructions_bytes", a.InstructionsBytes)
	addOptional(fields, "tools_count", a.ToolsCount)
	addOptional(fields, "pool_idle", a.PoolIdle)
	addOptional(fields, "pool_dialing", a.PoolDialing)
	addOptional(fields, "input_tokens", a.InputTokens)
	addOptional(fields, "output_tokens", a.OutputTokens)
	addOptional(fields, "reasoning_tokens", a.ReasoningTokens)
	addOptional(fields, "cached_tokens", a.CachedTokens)
	addOptional(fields, "cache_read_tokens", a.CacheReadTokens)
	addOptional(fields, "cache_creation_tokens", a.CacheCreationTokens)
	addOptional(fields, "total_tokens", a.TotalTokens)
	if a.ResponseServiceTier != "" {
		fields["response_service_tier"] = a.ResponseServiceTier
	}
	addOptional(fields, "tool_calls_started", a.ToolCallsStarted)
	addOptional(fields, "tool_calls_completed", a.ToolCallsCompleted)
	addOptional(fields, "tool_calls_incomplete", a.ToolCallsIncomplete)
	addOptional(fields, "repaired_tool_uses", a.RepairedToolUses)
	addOptional(fields, "separated_assistant_messages", a.SeparatedAssistantMessages)
	addOptional(fields, "compaction_retained_messages", a.CompactionRetainedMessages)
	addOptional(fields, "compaction_retained_images", a.CompactionRetainedImages)
	addOptional(fields, "compaction_dropped_items", a.CompactionDroppedItems)
	addOptional(fields, "compaction_retained_tokens", a.CompactionRetainedTokens)
	addOptional(fields, "adaptive_target", a.AdaptiveTarget)
	addOptional(fields, "adaptive_hit_rate_basis_points", a.AdaptiveHitRateBasisPoints)
	addOptional(fields, "counterfactual_wait_us", a.CounterfactualWaitUS)
	addOptional(fields, "predicted_savings_bytes", a.PredictedSavingsBytes)
	return fields
}

func (a WebsocketAttributes) Fields() map[string]any { return a.fields() }

func addOptional[T any](fields map[string]any, key string, value Optional[T]) {
	if value.Set {
		fields[key] = value.Value
	}
}
