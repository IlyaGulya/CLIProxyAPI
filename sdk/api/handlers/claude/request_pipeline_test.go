package claude

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestClaudeRequestPipelineRejectsOutOfOrderAndRepeatedTransitions(t *testing.T) {
	pipeline := newClaudeRequestPipeline([]byte(`{"model":"gpt-5.6-sol","messages":[]}`), "")
	if errTransition := pipeline.transition(claudePhaseClassified); !errors.Is(errTransition, errClaudePipelineTransition) {
		t.Fatalf("out-of-order transition error = %v", errTransition)
	}
	if errTransition := pipeline.transition(claudePhaseDecoded); errTransition != nil {
		t.Fatal(errTransition)
	}
	if errTransition := pipeline.transition(claudePhaseDecoded); !errors.Is(errTransition, errClaudePipelineTransition) {
		t.Fatalf("repeated transition error = %v", errTransition)
	}
}

func TestClaudeRequestPipelineRoutesClassifierBeforeBudgeting(t *testing.T) {
	request := []byte(`{"model":"claude-sonnet-5","max_tokens":64,"thinking":{"type":"disabled"},"stop_sequences":["</block>"],"system":[{"type":"text","text":"You are a security monitor for autonomous AI coding agents."}],"messages":[{"role":"user","content":"classify"}]}`)
	pipeline := newClaudeRequestPipeline(request, "gpt-5.6-terra")
	result := pipeline.run(false)
	if result.Err != nil || result.Model != "gpt-5.6-terra" || result.Kind != claudeRequestClassifier {
		t.Fatalf("pipeline result = %+v", result)
	}
	if result.Pressure.EffectiveWindow != 180_000 {
		t.Fatalf("preflight used pre-route model policy: %+v", result.Pressure)
	}
}

func TestClaudePolicyTableCoversRoutedModelsAndKinds(t *testing.T) {
	for _, model := range []string{"sol", "gpt-5.6-sol", "luna", "gpt-5.6-luna", "terra", "gpt-5.6-terra"} {
		for _, kind := range []claudeRequestKind{claudeRequestInteractive, claudeRequestReactiveCompact, claudeRequestClassifier, claudeRequestCountTokens} {
			policy := claudePolicyFor(model, kind)
			if policy.EffectiveWindow != 180_000 || policy.SafetyMargin != 8_192 || policy.MinimumOutput <= 0 {
				t.Fatalf("policy(%s,%s) = %+v", model, kind, policy)
			}
		}
	}
}

func TestClaudeRequestPipelineBoundsSyntheticResumeRecovery(t *testing.T) {
	t.Parallel()
	compactPrompt := "Your task is to create a detailed summary of the conversation so far, paying close attention to the user's explicit requests and your previous actions. Before providing your final summary, wrap your analysis in <analysis> tags. Your entire response must be plain text: an <analysis> block followed by a <summary> block."
	tests := []struct {
		name         string
		targetTokens int
		lastPrompt   string
		wantKind     claudeRequestKind
		wantRejected bool
		wantCompact  bool
		wantAdaptive bool
	}{
		{name: "boundary interactive adapts without compact loop", targetTokens: 160_000, lastPrompt: "Continue after restoring this session.", wantKind: claudeRequestInteractive, wantAdaptive: true},
		{name: "true overflow compact rejects once with bounded reserve", targetTokens: 168_000, lastPrompt: compactPrompt, wantKind: claudeRequestReactiveCompact, wantRejected: true, wantCompact: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := syntheticClaudeResumeRequest(t, test.targetTokens, test.lastPrompt)
			result := newClaudeRequestPipeline(request, "").run(false)
			if result.Err != nil || result.Kind != test.wantKind || result.Rejected != test.wantRejected {
				t.Fatalf("pipeline result = %+v", result)
			}
			if result.CompactBudget.Applied != test.wantCompact || result.AdaptiveBudget.Applied != test.wantAdaptive {
				t.Fatalf("budget observations: compact=%+v adaptive=%+v", result.CompactBudget, result.AdaptiveBudget)
			}
			if test.wantCompact && result.Pressure.ReservedOutput != claudeReactiveCompactMaxTokens {
				t.Fatalf("compact reserve = %d, want %d", result.Pressure.ReservedOutput, claudeReactiveCompactMaxTokens)
			}
			if test.wantRejected {
				message := claudeContextOverflowMessage(result.Pressure)
				for _, field := range []string{"input=", "requested_output=", "safety_margin=", "maximum"} {
					if !strings.Contains(message, field) {
						t.Fatalf("overflow message %q missing %q", message, field)
					}
				}
			}
		})
	}
}

func syntheticClaudeResumeRequest(t *testing.T, targetTokens int, lastPrompt string) []byte {
	t.Helper()
	repeats := targetTokens / 4
	for attempt := 0; attempt < 3; attempt++ {
		history := strings.Repeat("resume-boundary-token ", repeats)
		request := []byte(fmt.Sprintf(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":%q},{"role":"assistant","content":"preserved answer"},{"role":"user","content":%q}]}`, history, lastPrompt))
		document, errDocument := newClaudeRequestDocument(request)
		if errDocument != nil {
			t.Fatal(errDocument)
		}
		estimated, _ := document.estimateInputTokens()
		if estimated >= targetTokens-1_000 && estimated <= targetTokens+1_000 {
			return request
		}
		if estimated <= 0 {
			t.Fatalf("invalid token estimate %d", estimated)
		}
		repeats = repeats * targetTokens / estimated
	}
	t.Fatalf("could not construct request near %d tokens", targetTokens)
	return nil
}

func FuzzClaudeRequestPipelineDeterministic(f *testing.F) {
	f.Add([]byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":"hello"}]}`))
	f.Add([]byte(`{"model":"gpt-5.6-luna","messages":[]}`))
	f.Add([]byte(`not-json`))
	f.Fuzz(func(t *testing.T, input []byte) {
		first := newClaudeRequestPipeline(input, "gpt-5.6-terra").run(false)
		second := newClaudeRequestPipeline(input, "gpt-5.6-terra").run(false)
		if (first.Err == nil) != (second.Err == nil) || first.Rejected != second.Rejected || string(first.Body) != string(second.Body) {
			t.Fatalf("pipeline is non-deterministic:\nfirst=%+v\nsecond=%+v", first, second)
		}
	})
}
