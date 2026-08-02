package claude

import (
	"github.com/router-for-me/CLIProxyAPI/v7/internal/claudecompat"
	"github.com/tidwall/gjson"
)

type codexClaudeUsage struct {
	Input         int64
	CacheRead     int64
	CacheCreation int64
	Output        int64
}

func estimateLogicalClaudeInput(request []byte) int64 {
	estimated, _ := claudecompat.EstimateInputTokensJSON(request)
	return int64(estimated)
}

// reconcileCodexClaudeUsage keeps provider usage useful for cache attribution
// while reporting the full logical Claude request size to the client. Codex
// response chaining can make provider input usage much smaller than the
// stateless request Claude Code must compact.
func reconcileCodexClaudeUsage(logicalInput int64, providerUsage gjson.Result) codexClaudeUsage {
	providerInput := nonNegativeUsage(providerUsage.Get("input_tokens").Int())
	cacheRead := nonNegativeUsage(providerUsage.Get("input_tokens_details.cached_tokens").Int())
	cacheCreation := nonNegativeUsage(providerUsage.Get("cache_creation_input_tokens").Int())
	if cacheRead > providerInput {
		cacheRead = providerInput
	}

	providerTotal := providerInput + cacheCreation
	if logicalInput < providerTotal {
		logicalInput = providerTotal
	}
	if cacheRead+cacheCreation > logicalInput {
		cacheCreation = logicalInput - cacheRead
	}

	return codexClaudeUsage{
		Input:         logicalInput - cacheRead - cacheCreation,
		CacheRead:     cacheRead,
		CacheCreation: cacheCreation,
		Output:        nonNegativeUsage(providerUsage.Get("output_tokens").Int()),
	}
}

func nonNegativeUsage(tokens int64) int64 {
	if tokens < 0 {
		return 0
	}
	return tokens
}
