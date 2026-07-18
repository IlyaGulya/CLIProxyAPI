package claude

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/claudecompat"
	"github.com/tiktoken-go/tokenizer"
)

// claudeRequestDocument owns decoded request state for one handler invocation.
// It is deliberately package-private and must not escape the request lifetime.
type claudeRequestDocument struct {
	root                 map[string]any
	raw                  []byte
	dirty                bool
	estimateValid        bool
	estimatedInput       int
	estimationMethod     string
	estimateComputations int
}

func newClaudeRequestDocument(raw []byte) (*claudeRequestDocument, error) {
	var root map[string]any
	if errDecode := json.Unmarshal(raw, &root); errDecode != nil {
		return nil, fmt.Errorf("decode Claude request: %w", errDecode)
	}
	if root == nil {
		root = make(map[string]any)
	}
	return &claudeRequestDocument{root: root, raw: raw}, nil
}

func (d *claudeRequestDocument) bytes() ([]byte, error) {
	if d == nil {
		return nil, fmt.Errorf("encode nil Claude request")
	}
	if !d.dirty && d.raw != nil {
		return d.raw, nil
	}
	encoded, errEncode := json.Marshal(d.root)
	if errEncode != nil {
		return nil, fmt.Errorf("encode Claude request: %w", errEncode)
	}
	d.raw = encoded
	d.dirty = false
	return encoded, nil
}

func (d *claudeRequestDocument) model() string { return stringValue(d.root["model"]) }

func (d *claudeRequestDocument) maxTokens() int {
	return intValue(d.root["max_tokens"])
}

func (d *claudeRequestDocument) setModel(model string) bool {
	if d == nil || model == "" || d.model() == model {
		return false
	}
	d.root["model"] = model
	d.invalidate()
	return true
}

func (d *claudeRequestDocument) setMaxTokens(tokens int) bool {
	if d == nil || tokens <= 0 || d.maxTokens() == tokens {
		return false
	}
	d.root["max_tokens"] = tokens
	d.invalidate()
	return true
}

func (d *claudeRequestDocument) invalidate() {
	d.dirty = true
	d.estimateValid = false
	d.raw = nil
}

func (d *claudeRequestDocument) mutate(applied bool) bool {
	if d == nil || !applied {
		return false
	}
	d.invalidate()
	return true
}

func (d *claudeRequestDocument) normalizeModel() bool {
	changed := rewriteClaudeDDModelRoot(d.root)
	if changed {
		d.invalidate()
	}
	changed = d.setModel(claudecompat.RoutedModel(d.model())) || changed
	return changed
}

func (d *claudeRequestDocument) routeClassifier(targetModel string) bool {
	return d.mutate(rewriteClaudeCodeAutoModeClassifierModelRoot(d.root, targetModel))
}

func (d *claudeRequestDocument) repairToolHistory() (claudeToolHistoryRepairResult, error) {
	result, errRepair := repairInterruptedClaudeToolHistoryRoot(d.root)
	d.mutate(result.Applied)
	return result, errRepair
}

func (d *claudeRequestDocument) shapeCompaction(retainedTokenBudget int) claudeCompactionReplayObservation {
	result := applyClaudeCompactionReplayRoot(d.root, retainedTokenBudget)
	d.mutate(result.Applied)
	return result
}

func (d *claudeRequestDocument) applyContextEditing() claudeContextEditResult {
	result := applyClaudeContextEditingRoot(d.root)
	d.mutate(result.Applied)
	return result
}

func (d *claudeRequestDocument) hasContentType(want string) bool {
	if d == nil {
		return false
	}
	messages, _ := d.root["messages"].([]any)
	for _, rawMessage := range messages {
		message, _ := rawMessage.(map[string]any)
		parts, _ := message["content"].([]any)
		for _, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			if stringValue(part["type"]) == want {
				return true
			}
		}
	}
	return false
}

func (d *claudeRequestDocument) hasContextEdits() bool {
	management, _ := d.root["context_management"].(map[string]any)
	edits, _ := management["edits"].([]any)
	return len(edits) > 0
}

var (
	requestDocumentTokenizerOnce sync.Once
	requestDocumentTokenizer     tokenizer.Codec
	requestDocumentTokenizerErr  error
)

func (d *claudeRequestDocument) estimateInputTokens() (int, string) {
	if d.estimateValid {
		return d.estimatedInput, d.estimationMethod
	}
	d.estimateComputations++
	d.estimatedInput, d.estimationMethod = estimateClaudeGPTInputTokensValue(d.root)
	d.estimateValid = true
	return d.estimatedInput, d.estimationMethod
}

func estimateClaudeGPTInputTokensValue(value any) (int, string) {
	images := 0
	sanitized := sanitizeClaudeTokenInput(value, &images)
	encoded, errEncode := json.Marshal(sanitized)
	if errEncode != nil {
		return 0, "bytes_fallback"
	}
	requestDocumentTokenizerOnce.Do(func() {
		requestDocumentTokenizer, requestDocumentTokenizerErr = tokenizer.ForModel(tokenizer.GPT5)
	})
	if requestDocumentTokenizerErr != nil || requestDocumentTokenizer == nil {
		return approximateTokens(encoded) + images*85, "bytes_fallback"
	}
	count, errCount := requestDocumentTokenizer.Count(string(encoded))
	if errCount != nil {
		return approximateTokens(encoded) + images*85, "bytes_fallback"
	}
	return count + images*85, "gpt5_tokenizer"
}
