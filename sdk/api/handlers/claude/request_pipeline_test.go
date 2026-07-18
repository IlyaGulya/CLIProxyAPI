package claude

import (
	"errors"
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
