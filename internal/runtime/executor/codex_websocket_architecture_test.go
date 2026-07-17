package executor

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestCodexSessionStateMachineLegalLifecycle(t *testing.T) {
	t.Parallel()
	machine := newCodexSessionStateMachine()
	for _, next := range []codexSessionState{
		codexSessionDialing,
		codexSessionReady,
		codexSessionBusy,
		codexSessionReady,
		codexSessionDraining,
		codexSessionClosed,
	} {
		transition, err := machine.transition(next)
		if err != nil {
			t.Fatalf("transition to %s: %v", next, err)
		}
		if transition.To != next || transition.From == transition.To || transition.At.IsZero() {
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
		name string
		path []codexSessionState
		next codexSessionState
	}{
		{name: "idle to busy", next: codexSessionBusy},
		{name: "dialing to busy", path: []codexSessionState{codexSessionDialing}, next: codexSessionBusy},
		{name: "closed to dialing", path: []codexSessionState{codexSessionClosed}, next: codexSessionDialing},
		{name: "draining to ready", path: []codexSessionState{codexSessionDraining}, next: codexSessionReady},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			machine := newCodexSessionStateMachine()
			for _, next := range test.path {
				if _, err := machine.transition(next); err != nil {
					t.Fatalf("setup transition to %s: %v", next, err)
				}
			}
			before := machine.state()
			if _, err := machine.transition(test.next); !errors.Is(err, errCodexSessionTransition) {
				t.Fatalf("transition error = %v", err)
			}
			if got := machine.state(); got != before {
				t.Fatalf("illegal transition mutated state: got %s want %s", got, before)
			}
		})
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
	bridge := newCodexWebsocketStreamBridge()
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
		_, _ = machine.transition(codexSessionDialing)
		_, _ = machine.transition(codexSessionReady)
		_, _ = machine.transition(codexSessionBusy)
		_, _ = machine.transition(codexSessionReady)
		_, _ = machine.transition(codexSessionDraining)
		_, _ = machine.transition(codexSessionClosed)
	}
}
