package executor

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tiktoken-go/tokenizer"
)

const codexCompactionV2RetainedTokenBudget = 64_000

type codexCompactionV2Options struct {
	RetainedTokenBudget  int
	InitialContext       []json.RawMessage
	InjectBeforeLastUser bool
	Model                string
}

type codexCompactionV2Observation struct {
	Applied          bool
	InputItems       int
	RetainedMessages int
	RetainedImages   int
	DroppedItems     int
	RetainedTokens   int
}

func shapeCodexCompactionV2Replay(request []byte, retainedTokenBudget int) ([]byte, codexCompactionV2Observation, error) {
	var root map[string]json.RawMessage
	if json.Unmarshal(request, &root) != nil {
		return request, codexCompactionV2Observation{}, fmt.Errorf("decode compaction replay request")
	}
	var input []json.RawMessage
	if json.Unmarshal(root["input"], &input) != nil {
		return request, codexCompactionV2Observation{}, nil
	}
	compactionIndex := -1
	for index := range input {
		typeName := itemType(input[index])
		if typeName == "compaction" || typeName == "compaction_summary" || typeName == "context_compaction" {
			compactionIndex = index
		}
	}
	if compactionIndex < 0 {
		return request, codexCompactionV2Observation{}, nil
	}
	model := ""
	_ = json.Unmarshal(root["model"], &model)
	history, observation, err := buildCodexCompactionV2History(input[:compactionIndex], input[compactionIndex], codexCompactionV2Options{
		RetainedTokenBudget: retainedTokenBudget,
		Model:               model,
	})
	if err != nil {
		return request, observation, err
	}
	history = append(history, input[compactionIndex+1:]...)
	encodedInput, err := json.Marshal(history)
	if err != nil {
		return request, observation, fmt.Errorf("encode compacted replay input: %w", err)
	}
	root["input"] = encodedInput
	encodedRequest, err := json.Marshal(root)
	if err != nil {
		return request, observation, fmt.Errorf("encode compacted replay request: %w", err)
	}
	observation.Applied = true
	return encodedRequest, observation, nil
}

func buildCodexCompactionV2History(prompt []json.RawMessage, compactionOutput json.RawMessage, options codexCompactionV2Options) ([]json.RawMessage, codexCompactionV2Observation, error) {
	observation := codexCompactionV2Observation{InputItems: len(prompt)}
	if err := validateCodexCompactionOutput(compactionOutput); err != nil {
		return nil, observation, err
	}
	budget := options.RetainedTokenBudget
	if budget <= 0 {
		budget = codexCompactionV2RetainedTokenBudget
	}
	codec, err := tokenizerForCodexModel(options.Model)
	if err != nil {
		return nil, observation, fmt.Errorf("compaction tokenizer: %w", err)
	}

	retainedReversed := make([]json.RawMessage, 0, len(prompt))
	remaining := budget
	for index := len(prompt) - 1; index >= 0; index-- {
		item := prompt[index]
		if !isCodexCompactionRetainedMessage(item) {
			observation.DroppedItems++
			continue
		}
		if remaining == 0 {
			observation.DroppedItems++
			continue
		}
		messageTokens, imageCount := codexCompactionMessageCost(codec, item)
		messageCost := messageTokens
		if messageCost < 1 {
			messageCost = 1
		}
		if messageCost <= remaining {
			retainedReversed = append(retainedReversed, cloneRawMessage(item))
			remaining -= messageCost
			observation.RetainedTokens += messageCost
			observation.RetainedImages += imageCount
			continue
		}
		truncated, truncatedTokens, truncatedImages, ok := truncateCodexCompactionMessage(codec, item, remaining)
		if ok {
			retainedReversed = append(retainedReversed, truncated)
			observation.RetainedTokens += truncatedTokens
			observation.RetainedImages += truncatedImages
		} else {
			observation.DroppedItems++
		}
		remaining = 0
	}

	retained := make([]json.RawMessage, len(retainedReversed))
	for index := range retainedReversed {
		retained[len(retainedReversed)-1-index] = retainedReversed[index]
	}
	if options.InjectBeforeLastUser && len(options.InitialContext) > 0 {
		position := lastCodexUserMessageIndex(retained)
		if position < 0 {
			position = len(retained)
		}
		withContext := make([]json.RawMessage, 0, len(retained)+len(options.InitialContext))
		withContext = append(withContext, retained[:position]...)
		for _, item := range options.InitialContext {
			withContext = append(withContext, cloneRawMessage(item))
		}
		withContext = append(withContext, retained[position:]...)
		retained = withContext
	}
	observation.RetainedMessages = len(retained)
	retained = append(retained, cloneRawMessage(compactionOutput))
	return retained, observation, nil
}

func validateCodexCompactionOutput(raw json.RawMessage) error {
	var item map[string]any
	if json.Unmarshal(raw, &item) != nil {
		return fmt.Errorf("compaction output must be one JSON object")
	}
	itemType, _ := item["type"].(string)
	if itemType != "compaction" && itemType != "compaction_summary" && itemType != "context_compaction" {
		return fmt.Errorf("expected exactly one compaction output, got %q", itemType)
	}
	encrypted, _ := item["encrypted_content"].(string)
	if strings.TrimSpace(encrypted) == "" {
		return fmt.Errorf("compaction output is missing encrypted_content")
	}
	return nil
}

func isCodexCompactionRetainedMessage(raw json.RawMessage) bool {
	var item map[string]any
	if json.Unmarshal(raw, &item) != nil || item["type"] != "message" {
		return false
	}
	role, _ := item["role"].(string)
	return role == "user" || role == "developer" || role == "system"
}

func codexCompactionMessageCost(codec tokenizer.Codec, raw json.RawMessage) (int, int) {
	var item map[string]any
	if json.Unmarshal(raw, &item) != nil {
		return 0, 0
	}
	return codexCompactionContentCost(codec, item["content"])
}

func codexCompactionContentCost(codec tokenizer.Codec, content any) (int, int) {
	tokens, images := 0, 0
	switch typed := content.(type) {
	case string:
		tokens, _ = codec.Count(typed)
	case []any:
		for _, rawPart := range typed {
			part, _ := rawPart.(map[string]any)
			switch part["type"] {
			case "input_image":
				images++
			case "input_text", "output_text":
				text, _ := part["text"].(string)
				count, _ := codec.Count(text)
				tokens += count
			}
		}
	}
	return tokens, images
}

func truncateCodexCompactionMessage(codec tokenizer.Codec, raw json.RawMessage, budget int) (json.RawMessage, int, int, bool) {
	var item map[string]any
	if json.Unmarshal(raw, &item) != nil || budget <= 0 {
		return nil, 0, 0, false
	}
	content, ok := item["content"].([]any)
	if !ok {
		text, isText := item["content"].(string)
		if !isText {
			return nil, 0, 0, false
		}
		truncated, used := truncateCodexText(codec, text, budget)
		if used == 0 {
			return nil, 0, 0, false
		}
		item["content"] = truncated
		encoded, _ := json.Marshal(item)
		return encoded, used, 0, true
	}
	remaining := budget
	kept := make([]any, 0, len(content))
	images := 0
	used := 0
	for _, rawPart := range content {
		part, _ := rawPart.(map[string]any)
		switch part["type"] {
		case "input_image":
			kept = append(kept, rawPart)
			images++
		case "input_text", "output_text":
			text, _ := part["text"].(string)
			truncated, tokenCount := truncateCodexText(codec, text, remaining)
			if tokenCount > 0 {
				copyPart := make(map[string]any, len(part))
				for key, value := range part {
					copyPart[key] = value
				}
				copyPart["text"] = truncated
				kept = append(kept, copyPart)
				used += tokenCount
				remaining -= tokenCount
			}
		}
	}
	if len(kept) == 0 {
		return nil, 0, 0, false
	}
	item["content"] = kept
	encoded, _ := json.Marshal(item)
	return encoded, max(used, 1), images, true
}

func truncateCodexText(codec tokenizer.Codec, text string, budget int) (string, int) {
	if budget <= 0 || text == "" {
		return "", 0
	}
	ids, _, err := codec.Encode(text)
	if err != nil || len(ids) == 0 {
		return "", 0
	}
	if len(ids) > budget {
		ids = ids[:budget]
	}
	decoded, err := codec.Decode(ids)
	if err != nil {
		return "", 0
	}
	return decoded, len(ids)
}

func lastCodexUserMessageIndex(items []json.RawMessage) int {
	for index := len(items) - 1; index >= 0; index-- {
		var item map[string]any
		if json.Unmarshal(items[index], &item) == nil && item["type"] == "message" && item["role"] == "user" {
			return index
		}
	}
	return -1
}

func itemType(raw json.RawMessage) string {
	var item map[string]any
	_ = json.Unmarshal(raw, &item)
	value, _ := item["type"].(string)
	return value
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), raw...)
}
