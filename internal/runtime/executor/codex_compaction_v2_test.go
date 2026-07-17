package executor

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildCodexCompactionV2HistoryRetainsMessagesImagesAndOneOutput(t *testing.T) {
	prompt := rawItems(t,
		`{"type":"message","role":"developer","content":[{"type":"input_text","text":"initial"}]}`,
		`{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"}`,
		`{"type":"function_call_output","call_id":"call-1","output":"large output"}`,
		`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"discard"}]}`,
		`{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AA"},{"type":"input_text","text":"recent"}]}`,
	)
	compaction := json.RawMessage(`{"type":"compaction","encrypted_content":"opaque"}`)
	history, observation, err := buildCodexCompactionV2History(prompt, compaction, codexCompactionV2Options{RetainedTokenBudget: 64_000})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 || itemType(history[2]) != "compaction" {
		t.Fatalf("history = %s", marshalRawItems(t, history))
	}
	if observation.RetainedMessages != 2 || observation.RetainedImages != 1 || observation.DroppedItems != 3 {
		t.Fatalf("observation = %+v", observation)
	}
}

func TestBuildCodexCompactionV2HistoryKeepsNewestWithinBudget(t *testing.T) {
	prompt := rawItems(t,
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"old old old old"}]}`,
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"new"}]}`,
	)
	history, _, err := buildCodexCompactionV2History(prompt, json.RawMessage(`{"type":"compaction","encrypted_content":"opaque"}`), codexCompactionV2Options{RetainedTokenBudget: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || !strings.Contains(string(history[0]), `"new"`) {
		t.Fatalf("newest message not retained: %s", marshalRawItems(t, history))
	}
}

func TestBuildCodexCompactionV2HistoryInjectsInitialContextBeforeLastUser(t *testing.T) {
	prompt := rawItems(t,
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}`,
		`{"type":"message","role":"user","content":[{"type":"input_text","text":"last"}]}`,
	)
	initial := rawItems(t, `{"type":"message","role":"developer","content":[{"type":"input_text","text":"reinjected"}]}`)
	history, _, err := buildCodexCompactionV2History(prompt, json.RawMessage(`{"type":"compaction","encrypted_content":"opaque"}`), codexCompactionV2Options{
		RetainedTokenBudget: 64_000, InitialContext: initial, InjectBeforeLastUser: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	serialized := string(marshalRawItems(t, history))
	if !(strings.Index(serialized, "first") < strings.Index(serialized, "reinjected") && strings.Index(serialized, "reinjected") < strings.Index(serialized, "last")) {
		t.Fatalf("initial context position is wrong: %s", serialized)
	}
}

func TestBuildCodexCompactionV2HistoryRejectsInvalidOutput(t *testing.T) {
	prompt := rawItems(t, `{"type":"message","role":"user","content":"hello"}`)
	for _, output := range []string{
		`{"type":"message","role":"assistant","content":"not compact"}`,
		`[{"type":"compaction","encrypted_content":"one"},{"type":"compaction","encrypted_content":"two"}]`,
		`{"type":"compaction"}`,
	} {
		if _, _, err := buildCodexCompactionV2History(prompt, json.RawMessage(output), codexCompactionV2Options{RetainedTokenBudget: 10}); err == nil {
			t.Fatalf("invalid compaction output accepted: %s", output)
		}
	}
}

func TestShapeCodexCompactionV2ReplayDropsNonessentialPreSummaryHistory(t *testing.T) {
	request := []byte(`{"model":"gpt-5.6-sol","instructions":"initial","input":[
		{"type":"message","role":"user","content":[{"type":"input_text","text":"old"}]},
		{"type":"function_call","call_id":"old-call","name":"shell","arguments":"{}"},
		{"type":"function_call_output","call_id":"old-call","output":"discard"},
		{"type":"compaction","encrypted_content":"opaque"},
		{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}
	]}`)
	shaped, observation, err := shapeCodexCompactionV2Replay(request, 64_000)
	if err != nil {
		t.Fatal(err)
	}
	if !observation.Applied || strings.Contains(string(shaped), "old-call") {
		t.Fatalf("replay was not shaped: observation=%+v body=%s", observation, shaped)
	}
	var body map[string]any
	_ = json.Unmarshal(shaped, &body)
	items, _ := body["input"].([]any)
	if len(items) != 3 {
		t.Fatalf("input items = %d, want retained user + compaction + continuation: %s", len(items), shaped)
	}
}

func TestCompactionReplayAcrossModelSwitchUsesFullShapedRequest(t *testing.T) {
	sess := &codexWebsocketSession{
		lastRequest:    []byte(`{"model":"gpt-5.6-sol","input":[{"type":"message","role":"user","content":"old"}]}`),
		lastResponseID: "resp-sol",
	}
	request := []byte(`{"model":"gpt-5.6-luna","input":[{"type":"message","role":"user","content":"old"},{"type":"compaction","encrypted_content":"opaque"},{"type":"message","role":"user","content":"continue"}]}`)
	shaped, observation := sess.prepareCodexIncrementalRequestObserved(request)
	if observation.incremental || observation.resetReason != "request_mismatch" {
		t.Fatalf("observation = %+v", observation)
	}
	if strings.Contains(string(shaped), "previous_response_id") || !observation.compaction.Applied {
		t.Fatalf("model switch reused old response chain: %s", shaped)
	}
}

func TestCompactionReplayShrinksStateBeforeRetentionLimit(t *testing.T) {
	sess := &codexWebsocketSession{}
	request := []byte(`{"model":"gpt-5.6-sol","input":[{"type":"function_call_output","call_id":"old","output":"` + strings.Repeat("x", codexWebsocketMaxIncrementalStateBytes) + `"},{"type":"compaction","encrypted_content":"opaque"},{"type":"message","role":"user","content":"continue"}]}`)
	shaped, observation := sess.prepareCodexIncrementalRequestObserved(request)
	if observation.resetReason != "no_previous_response" || !observation.compaction.Applied {
		t.Fatalf("observation = %+v", observation)
	}
	if len(shaped) >= codexWebsocketMaxIncrementalStateBytes || len(sess.pendingRequest) != len(shaped) {
		t.Fatalf("shaped state was not bounded: shaped=%d pending=%d", len(shaped), len(sess.pendingRequest))
	}
}

func rawItems(t *testing.T, values ...string) []json.RawMessage {
	t.Helper()
	out := make([]json.RawMessage, len(values))
	for index, value := range values {
		if !json.Valid([]byte(value)) {
			t.Fatalf("invalid fixture: %s", value)
		}
		out[index] = json.RawMessage(value)
	}
	return out
}

func marshalRawItems(t *testing.T, items []json.RawMessage) []byte {
	t.Helper()
	payload, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}
