package claudecompat

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/tiktoken-go/tokenizer"
)

const claudeImageTokenEstimate = 85

var (
	inputTokenizerOnce sync.Once
	inputTokenizer     tokenizer.Codec
	inputTokenizerErr  error
)

// EstimateInputTokensJSON estimates the logical Claude request size before
// provider-specific translation or response chaining changes its wire shape.
func EstimateInputTokensJSON(input []byte) (int, string) {
	if len(input) == 0 {
		return 0, "bytes_fallback"
	}
	var decoded any
	if json.Unmarshal(input, &decoded) != nil {
		return approximateInputTokens(input), "bytes_fallback"
	}
	return EstimateInputTokensValue(decoded)
}

// EstimateInputTokensValue estimates an already decoded Claude request.
func EstimateInputTokensValue(value any) (int, string) {
	images := 0
	sanitized := sanitizeInputTokenValue(value, &images)
	encoded, errEncode := json.Marshal(sanitized)
	if errEncode != nil {
		return 0, "bytes_fallback"
	}
	inputTokenizerOnce.Do(func() {
		inputTokenizer, inputTokenizerErr = tokenizer.ForModel(tokenizer.GPT5)
	})
	if inputTokenizerErr != nil || inputTokenizer == nil {
		return approximateInputTokens(encoded) + images*claudeImageTokenEstimate, "bytes_fallback"
	}
	count, errCount := inputTokenizer.Count(string(encoded))
	if errCount != nil {
		return approximateInputTokens(encoded) + images*claudeImageTokenEstimate, "bytes_fallback"
	}
	return count + images*claudeImageTokenEstimate, "gpt5_tokenizer"
}

func sanitizeInputTokenValue(value any, images *int) any {
	switch typed := value.(type) {
	case []any:
		out := make([]any, len(typed))
		for index := range typed {
			out[index] = sanitizeInputTokenValue(typed[index], images)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		typeName, _ := typed["type"].(string)
		isImage := strings.Contains(strings.ToLower(typeName), "image")
		isBase64 := strings.EqualFold(typeName, "base64")
		if isImage {
			*images++
		}
		for key, child := range typed {
			if isBase64 && key == "data" {
				out[key] = "[image bytes omitted]"
				continue
			}
			out[key] = sanitizeInputTokenValue(child, images)
		}
		return out
	default:
		return value
	}
}

func approximateInputTokens(value []byte) int {
	if len(value) == 0 {
		return 0
	}
	return (len(value) + 3) / 4
}
