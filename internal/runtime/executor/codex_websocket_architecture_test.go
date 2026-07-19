package executor

import (
	"context"
	"errors"
	"io"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestCodexSessionStateMachineLegalLifecycle(t *testing.T) {
	t.Parallel()
	machine := newCodexSessionStateMachine()
	for _, event := range []codexSessionEvent{
		codexEventDialRequested,
		codexEventConnected,
		codexEventRequestStarted,
		codexEventRequestFinished,
		codexEventDrainRequested,
		codexEventClosed,
	} {
		transition, err := machine.apply(event)
		if err != nil {
			t.Fatalf("apply %s: %v", event, err)
		}
		if transition.From == transition.To || transition.At.IsZero() || transition.Event != event {
			t.Fatalf("invalid transition: %+v", transition)
		}
	}
	if got := machine.state(); got != codexSessionClosed {
		t.Fatalf("state = %s, want closed", got)
	}
}

func TestCodexSessionStateMachineRejectsIllegalTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		path  []codexSessionEvent
		event codexSessionEvent
	}{
		{name: "idle request start", event: codexEventRequestStarted},
		{name: "dialing request start", path: []codexSessionEvent{codexEventDialRequested}, event: codexEventRequestStarted},
		{name: "closed dial", path: []codexSessionEvent{codexEventDrainRequested, codexEventClosed}, event: codexEventDialRequested},
		{name: "draining connected", path: []codexSessionEvent{codexEventDrainRequested}, event: codexEventConnected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := newCodexSessionStateMachine()
			for _, event := range test.path {
				if _, err := machine.apply(event); err != nil {
					t.Fatalf("setup event %s: %v", event, err)
				}
			}
			before := machine.state()
			if _, err := machine.apply(test.event); !errors.Is(err, errCodexSessionTransition) {
				t.Fatalf("transition error = %v", err)
			}
			if got := machine.state(); got != before {
				t.Fatalf("illegal transition mutated state: got %s want %s", got, before)
			}
		})
	}
}

func TestCodexSessionReducerExhaustiveMatrix(t *testing.T) {
	t.Parallel()
	for state := codexSessionIdle; state <= codexSessionClosed; state++ {
		for event := codexEventDialRequested; event <= codexEventClosed; event++ {
			next, actions, errReduce := reduceCodexSession(state, event)
			if errReduce == nil && next == state {
				t.Fatalf("accepted no-op transition: %s + %s", state, event)
			}
			if event == codexEventIdleExpired && errReduce == nil && len(actions) != 2 {
				t.Fatalf("idle expiry actions = %v", actions)
			}
		}
	}
}

func TestCodexRetryPolicyMatrix(t *testing.T) {
	t.Parallel()
	temporary := &temporaryTestError{}
	tests := []struct {
		name  string
		input codexRetryInput
		want  codexRetryDecision
	}{
		{name: "temporary before commit", input: codexRetryInput{Err: temporary}, want: codexRetryDecision{Action: codexRetryReconnect, Reason: codexRetryTemporaryNetwork, Boundary: codexRetryBeforeCommit}},
		{name: "eof before commit", input: codexRetryInput{Err: io.ErrUnexpectedEOF}, want: codexRetryDecision{Action: codexRetryReconnect, Reason: codexRetryAbnormalClose, Boundary: codexRetryBeforeCommit}},
		{name: "committed output", input: codexRetryInput{Err: temporary, DownstreamCommitted: true}, want: codexRetryDecision{Action: codexRetryStop, Reason: codexRetryDownstreamCommitted, Boundary: codexRetryAfterCommit}},
		{name: "budget exhausted", input: codexRetryInput{Err: temporary, Attempts: 1, MaxAttempts: 1}, want: codexRetryDecision{Action: codexRetryStop, Reason: codexRetryBudgetExhausted, Boundary: codexRetryBeforeCommit}},
		{name: "cancelled", input: codexRetryInput{Err: context.Canceled}, want: codexRetryDecision{Action: codexRetryStop, Reason: codexRetryCancelled, Boundary: codexRetryBeforeCommit}},
		{name: "semantic error", input: codexRetryInput{Err: errors.New("invalid response")}, want: codexRetryDecision{Action: codexRetryStop, Reason: codexRetryNotRetryable, Boundary: codexRetryBeforeCommit}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := decideCodexRetry(test.input)
			if got != test.want {
				t.Fatalf("decision = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestCodexStreamBridgeTracksCommitAndToolCompletion(t *testing.T) {
	t.Parallel()
	bridge := newCodexWebsocketStreamBridge(false)
	bridge.observe([]byte(`{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call-1"}}`))
	if got := bridge.snapshot(); got.DownstreamCommitted || got.IncompleteToolCalls != 1 {
		t.Fatalf("before commit snapshot = %+v", got)
	}
	bridge.commit([]byte("data: chunk\n\n"))
	bridge.observe([]byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1"}}`))
	got := bridge.snapshot()
	if !got.DownstreamCommitted || got.IncompleteToolCalls != 0 || got.ToolCallsCompleted != 1 {
		t.Fatalf("completed snapshot = %+v", got)
	}
	bridge.resetUpstreamAttempt()
	got = bridge.snapshot()
	if !got.DownstreamCommitted || got.ToolCallsStarted != 0 {
		t.Fatalf("reset erased commit boundary or retained attempt semantics: %+v", got)
	}
}

func TestCodexStreamBridgeTransactionalLifecycle(t *testing.T) {
	t.Parallel()
	bridge := newCodexWebsocketStreamBridge(true)
	bridge.transaction.maxBytes = 5

	if staged := bridge.stage(cliproxyexecutor.StreamChunk{Payload: []byte("abc")}); !staged.Buffered || staged.Overflow {
		t.Fatalf("first stage = %+v, want buffered", staged)
	}
	if staged := bridge.stage(cliproxyexecutor.StreamChunk{Payload: []byte("def")}); staged.Buffered || !staged.Overflow {
		t.Fatalf("overflow stage = %+v, want overflow without mutation", staged)
	}
	if got := bridge.bufferedBytes(); got != 3 {
		t.Fatalf("buffered bytes = %d, want 3", got)
	}
	drain := bridge.drain()
	if drain.Bytes != 3 || len(drain.Chunks) != 1 || string(drain.Chunks[0].Payload) != "abc" {
		t.Fatalf("drain = %+v", drain)
	}
	bridge.stage(cliproxyexecutor.StreamChunk{Payload: []byte("xy")})
	bridge.resetUpstreamAttempt()
	if bridge.bufferedBytes() != 0 || !bridge.transactionEnabled() {
		t.Fatal("retry reset must discard the attempt while retaining transactional policy")
	}
}

func TestCodexTransactionalPolicyAndTerminalEvents(t *testing.T) {
	t.Parallel()
	if !shouldUseCodexTransactionalStream("claude", "agent", false) {
		t.Fatal("Claude child SSE stream should be transactional")
	}
	for _, input := range []struct {
		source, agent string
		downstream    bool
	}{{"claude", "", false}, {"claude", "agent", true}, {"openai", "agent", false}} {
		if shouldUseCodexTransactionalStream(input.source, input.agent, input.downstream) {
			t.Fatalf("unexpected transactional policy for %+v", input)
		}
	}
	for _, eventType := range []string{"response.completed", "response.done", "response.incomplete"} {
		if !isCodexCompletionEvent(eventType) {
			t.Fatalf("%s must terminate and commit the transaction", eventType)
		}
	}
}

type temporaryTestError struct{}

func (*temporaryTestError) Error() string   { return "temporary" }
func (*temporaryTestError) Timeout() bool   { return true }
func (*temporaryTestError) Temporary() bool { return true }

func BenchmarkCodexRetryPolicy(b *testing.B) {
	errTimeout := &temporaryTestError{}
	input := codexRetryInput{Err: errTimeout, MaxAttempts: 1}
	b.ReportAllocs()
	for b.Loop() {
		_ = decideCodexRetry(input)
	}
}

func BenchmarkCodexSessionStateLifecycle(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		machine := newCodexSessionStateMachine()
		_, _ = machine.apply(codexEventDialRequested)
		_, _ = machine.apply(codexEventConnected)
		_, _ = machine.apply(codexEventRequestStarted)
		_, _ = machine.apply(codexEventRequestFinished)
		_, _ = machine.apply(codexEventDrainRequested)
		_, _ = machine.apply(codexEventClosed)
	}
}
