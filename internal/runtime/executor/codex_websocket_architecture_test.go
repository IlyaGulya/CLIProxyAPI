package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
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
	bridge := newCodexWebsocketStreamBridge()
	bridge.observe(classifyCodexStreamEvent([]byte(`{"type":"response.output_item.added","item":{"type":"function_call","call_id":"call-1"}}`)))
	if got := bridge.snapshot(); got.DownstreamCommitted || got.IncompleteToolCalls != 1 {
		t.Fatalf("before commit snapshot = %+v", got)
	}
	bridge.commit([]byte("data: chunk\n\n"))
	bridge.observe(classifyCodexStreamEvent([]byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-1"}}`)))
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

func TestCodexTransactionalStreamBufferLifecycle(t *testing.T) {
	t.Parallel()
	transaction := newCodexTransactionalStream(codexTransactionalStreamPolicy{mode: codexTransactionalChildUntilTerminal, maxBytes: 5})
	transaction.maxBytes = 5

	if staged := transaction.stage(cliproxyexecutor.StreamChunk{Payload: []byte("abc")}); staged != codexStageBuffered {
		t.Fatalf("first stage = %v, want buffered", staged)
	}
	if staged := transaction.stage(cliproxyexecutor.StreamChunk{Payload: []byte("def")}); staged != codexStageOverflow {
		t.Fatalf("overflow stage = %v, want overflow without mutation", staged)
	}
	if got := transaction.bytes; got != 3 {
		t.Fatalf("buffered bytes = %d, want 3", got)
	}
	drain := transaction.drain(codexCommitTerminal)
	if drain.Bytes != 3 || len(drain.Chunks) != 1 || string(drain.Chunks[0].Payload) != "abc" {
		t.Fatalf("drain = %+v", drain)
	}
}

func TestCodexTransactionalRetryResetPreservesOnlyBufferingState(t *testing.T) {
	t.Parallel()
	buffering, err := newCodexStreamDelivery(codexTransactionalStreamPolicy{mode: codexTransactionalChildUntilTerminal, maxBytes: 8}, func(cliproxyexecutor.StreamChunk) bool { return true }, codexStreamDeliveryHooks{})
	if err != nil {
		t.Fatal(err)
	}
	buffering.send(cliproxyexecutor.StreamChunk{Payload: []byte("attempt")})
	if err = buffering.resetUpstreamAttempt(); err != nil {
		t.Fatal(err)
	}
	if got := buffering.state(); got != codexTransactionBuffering || buffering.bufferedBytes() != 0 {
		t.Fatalf("buffering retry reset = %s/%d", got, buffering.bufferedBytes())
	}

	passthrough, err := newCodexStreamDelivery(codexTransactionalStreamPolicy{mode: codexTransactionalChildUntilTerminal, maxBytes: 3}, func(cliproxyexecutor.StreamChunk) bool { return true }, codexStreamDeliveryHooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err = passthrough.transaction.apply(codexTransactionOverflow); err != nil {
		t.Fatal(err)
	}
	if err = passthrough.resetUpstreamAttempt(); err != nil {
		t.Fatal(err)
	}
	if got := passthrough.state(); got != codexTransactionPassthrough {
		t.Fatalf("passthrough retry reset = %s, want passthrough", got)
	}
}

func TestCodexNonTransactionalTerminalErrorDoesNotInventTransactionState(t *testing.T) {
	t.Parallel()
	delivery, err := newCodexStreamDelivery(codexTransactionalStreamPolicy{}, func(cliproxyexecutor.StreamChunk) bool { return true }, codexStreamDeliveryHooks{})
	if err != nil {
		t.Fatal(err)
	}
	if !delivery.send(cliproxyexecutor.StreamChunk{Err: errors.New("terminal")}) {
		t.Fatal("delivery unexpectedly stopped")
	}
	if got := delivery.state(); got != codexTransactionDisabled {
		t.Fatalf("state = %s, want disabled", got)
	}
}

func TestCodexStreamDeliveryOverflowOrdering(t *testing.T) {
	t.Parallel()
	var delivered []string
	var overflowBytes int
	var committed codexTransactionalDrain
	delivery, err := newCodexStreamDelivery(codexTransactionalStreamPolicy{mode: codexTransactionalRootUntilSemanticOutput, maxBytes: 3}, func(chunk cliproxyexecutor.StreamChunk) bool {
		delivered = append(delivered, string(chunk.Payload))
		return true
	}, codexStreamDeliveryHooks{
		Overflowed: func(bytes int) { overflowBytes = bytes },
		Committed:  func(drain codexTransactionalDrain) { committed = drain },
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery.transaction.maxBytes = 3
	if !delivery.send(cliproxyexecutor.StreamChunk{Payload: []byte("abc")}) ||
		!delivery.send(cliproxyexecutor.StreamChunk{Payload: []byte("de")}) {
		t.Fatal("delivery unexpectedly stopped")
	}
	if got := delivery.state(); got != codexTransactionPassthrough {
		t.Fatalf("state = %s, want passthrough", got)
	}
	if overflowBytes != 5 {
		t.Fatalf("overflow bytes = %d, want 5", overflowBytes)
	}
	if committed.Boundary != codexCommitBufferLimit || committed.Bytes != 3 {
		t.Fatalf("overflow commit = %+v, want buffer-limit boundary with 3 bytes", committed)
	}
	if got := strings.Join(delivered, ","); got != "abc,de" {
		t.Fatalf("delivery order = %q, want abc,de", got)
	}
}

func TestCodexStreamDeliveryCompletionCommitsTransaction(t *testing.T) {
	t.Parallel()
	var committed codexTransactionalDrain
	delivery, err := newCodexStreamDelivery(codexTransactionalStreamPolicy{mode: codexTransactionalChildUntilTerminal, maxBytes: 8}, func(cliproxyexecutor.StreamChunk) bool { return true }, codexStreamDeliveryHooks{
		Committed: func(drain codexTransactionalDrain) { committed = drain },
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery.send(cliproxyexecutor.StreamChunk{Payload: []byte("done")})
	if !delivery.flush(codexCommitTerminal) {
		t.Fatal("flush unexpectedly stopped")
	}
	if got := delivery.state(); got != codexTransactionCommitted || committed.Bytes != 4 {
		t.Fatalf("commit = %s/%d", got, committed.Bytes)
	}
}

func TestCodexStreamDeliveryPolicyOwnsOverflowFailure(t *testing.T) {
	t.Parallel()
	var delivered []cliproxyexecutor.StreamChunk
	policy := codexTransactionalStreamPolicy{
		mode: codexTransactionalRootUntilSemanticOutput, maxBytes: 3, overflowAction: codexOverflowFail,
	}
	delivery, err := newCodexStreamDelivery(policy, func(chunk cliproxyexecutor.StreamChunk) bool {
		delivered = append(delivered, chunk)
		return true
	}, codexStreamDeliveryHooks{})
	if err != nil {
		t.Fatal(err)
	}
	if !delivery.send(cliproxyexecutor.StreamChunk{Payload: []byte("abc")}) || !delivery.send(cliproxyexecutor.StreamChunk{Payload: []byte("de")}) {
		t.Fatal("delivery unexpectedly stopped")
	}
	if len(delivered) != 1 || delivered[0].Err == nil || len(delivered[0].Payload) != 0 {
		t.Fatalf("delivered = %+v, want one terminal overflow error and no buffered payload", delivered)
	}
	if got := delivery.state(); got != codexTransactionDiscarded {
		t.Fatalf("state = %s, want discarded", got)
	}
}

func TestCodexTransactionalPolicyAndTerminalEvents(t *testing.T) {
	t.Parallel()
	if !newCodexTransactionalPolicy("claude", "agent", false).enabled() {
		t.Fatal("Claude child SSE stream should be transactional")
	}
	if got := newCodexTransactionalPolicy("claude", "", false); got.mode != codexTransactionalRootUntilSemanticOutput || got.name() != "root_until_semantic_output" || got.maxBytes != codexTransactionalRootMaxBytes {
		t.Fatalf("Claude root policy = %+v, want root-until-semantic-output", got)
	}
	if got := newCodexTransactionalPolicy("claude", "agent", false); got.mode != codexTransactionalChildUntilTerminal || got.name() != "child_until_terminal" || got.maxBytes != codexTransactionalChildMaxBytes {
		t.Fatalf("Claude child policy = %+v, want child-until-terminal", got)
	}
	for _, input := range []struct {
		source, agent string
		downstream    bool
	}{{"claude", "agent", true}, {"openai", "agent", false}} {
		if newCodexTransactionalPolicy(input.source, input.agent, input.downstream).enabled() {
			t.Fatalf("unexpected transactional policy for %+v", input)
		}
	}
	for _, payload := range []string{
		`{"type":"response.output_text.delta","delta":"answer"}`,
		`{"type":"response.output_item.added","item":{"type":"function_call"}}`,
		`{"type":"response.output_item.added","item":{"type":"custom_tool_call"}}`,
	} {
		if decision := newCodexTransactionalPolicy("claude", "", false).decide(classifyCodexStreamEvent([]byte(payload))); !decision.CommitBefore || decision.Boundary != codexCommitSemanticOutput {
			t.Fatalf("expected semantic output boundary: %s", payload)
		}
	}
	for _, payload := range []string{
		`{"type":"response.created"}`,
		`{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`,
		`{"type":"response.output_item.added","item":{"type":"reasoning"}}`,
		`{"type":"response.output_item.added","item":{"type":"message"}}`,
	} {
		if decision := newCodexTransactionalPolicy("claude", "", false).decide(classifyCodexStreamEvent([]byte(payload))); decision.CommitBefore {
			t.Fatalf("unexpected semantic output boundary: %s", payload)
		}
	}
	for _, eventType := range []string{"response.completed", "response.done", "response.incomplete"} {
		if !isCodexCompletionEvent(eventType) {
			t.Fatalf("%s must terminate and commit the transaction", eventType)
		}
	}
}

func TestCodexTransactionDiscardReason(t *testing.T) {
	t.Parallel()
	if got := codexTransactionDiscardReason(context.Canceled); got != "context_done" {
		t.Fatalf("cancellation reason = %q, want context_done", got)
	}
	if got := codexTransactionDiscardReason(errors.New("upstream failed")); got != "terminal_error" {
		t.Fatalf("failure reason = %q, want terminal_error", got)
	}
}

func TestClassifyCodexStreamEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		payload    string
		kind       codexStreamEventKind
		toolChange codexToolTransition
	}{
		{name: "created envelope", payload: `{"type":"response.created"}`, kind: codexStreamEnvelope},
		{name: "reasoning", payload: `{"type":"response.reasoning_summary_text.delta","delta":"thinking"}`, kind: codexStreamReasoning},
		{name: "text", payload: `{"type":"response.output_text.delta","delta":"answer"}`, kind: codexStreamText},
		{name: "tool start", payload: `{"type":"response.output_item.added","item":{"type":"function_call"}}`, kind: codexStreamTool, toolChange: codexToolStarted},
		{name: "tool completion", payload: `{"type":"response.output_item.done","item":{"type":"custom_tool_call"}}`, kind: codexStreamTool, toolChange: codexToolCompleted},
		{name: "terminal", payload: `{"type":"response.completed"}`, kind: codexStreamTerminal},
		{name: "error", payload: `{"type":"error"}`, kind: codexStreamError},
		{name: "unknown", payload: `{"type":"future.event"}`, kind: codexStreamUnknown},
		{name: "malformed", payload: `{`, kind: codexStreamUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := classifyCodexStreamEvent([]byte(test.payload))
			if event.Kind != test.kind || event.ToolTransition != test.toolChange {
				t.Fatalf("event = %+v, want kind=%d toolTransition=%d", event, test.kind, test.toolChange)
			}
		})
	}
}

func TestCodexAttemptReducerHappyPathAndRecovery(t *testing.T) {
	t.Parallel()
	state := codexAttemptPrepared
	steps := []struct {
		event   codexAttemptEvent
		want    codexAttemptState
		actions []codexAttemptAction
	}{
		{codexAttemptConnectRequested, codexAttemptConnecting, []codexAttemptAction{codexAttemptDial}},
		{codexAttemptConnected, codexAttemptReady, nil},
		{codexAttemptReaderActivated, codexAttemptReaderReady, []codexAttemptAction{codexAttemptActivateReader}},
		{codexAttemptRequestSent, codexAttemptActive, []codexAttemptAction{codexAttemptSendRequest}},
		{codexAttemptInterrupted, codexAttemptInterruptedState, []codexAttemptAction{codexAttemptDetachReader, codexAttemptCloseConnection}},
		{codexAttemptRetryApproved, codexAttemptRetrying, nil},
		{codexAttemptConnectRequested, codexAttemptConnecting, []codexAttemptAction{codexAttemptDial}},
		{codexAttemptConnected, codexAttemptReady, nil},
		{codexAttemptRecoveryPrepared, codexAttemptReady, []codexAttemptAction{codexAttemptDiscardBuffer, codexAttemptResetSemantics}},
		{codexAttemptReaderActivated, codexAttemptReaderReady, []codexAttemptAction{codexAttemptActivateReader}},
		{codexAttemptRequestSent, codexAttemptActive, []codexAttemptAction{codexAttemptSendRequest}},
		{codexAttemptTerminalReceived, codexAttemptCompleted, nil},
	}
	for _, step := range steps {
		transition, err := reduceCodexAttempt(state, step.event)
		if err != nil {
			t.Fatalf("reduce(%s, %s): %v", state, step.event, err)
		}
		if transition.To != step.want || !slices.Equal(transition.Actions, step.actions) {
			t.Fatalf("reduce(%s, %s) = %+v, want state=%s actions=%v", state, step.event, transition, step.want, step.actions)
		}
		state = transition.To
	}
}

func TestCodexAttemptReducerRejectsIllegalTransitions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		state codexAttemptState
		event codexAttemptEvent
	}{
		{codexAttemptPrepared, codexAttemptRequestSent},
		{codexAttemptConnecting, codexAttemptTerminalReceived},
		{codexAttemptCompleted, codexAttemptRetryApproved},
		{codexAttemptCancelled, codexAttemptConnectRequested},
	} {
		if _, err := reduceCodexAttempt(test.state, test.event); !errors.Is(err, errCodexAttemptTransition) {
			t.Fatalf("reduce(%s, %s) error = %v, want invalid transition", test.state, test.event, err)
		}
	}
}

func TestCodexAttemptActionRunnerPreservesReducerOrder(t *testing.T) {
	t.Parallel()
	var got []codexAttemptAction
	handler := func(action codexAttemptAction) func() error {
		return func() error {
			got = append(got, action)
			return nil
		}
	}
	actions := []codexAttemptAction{codexAttemptDetachReader, codexAttemptCloseConnection, codexAttemptDiscardBuffer, codexAttemptResetSemantics, codexAttemptDial, codexAttemptActivateReader, codexAttemptSendRequest}
	err := runCodexAttemptActions(actions, codexAttemptActionHandlers{
		Dial: handler(codexAttemptDial), ActivateReader: handler(codexAttemptActivateReader), SendRequest: handler(codexAttemptSendRequest),
		DetachReader: handler(codexAttemptDetachReader), CloseConnection: handler(codexAttemptCloseConnection),
		DiscardBuffer: handler(codexAttemptDiscardBuffer), ResetSemantics: handler(codexAttemptResetSemantics),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, actions) {
		t.Fatalf("action order = %v, want %v", got, actions)
	}
	if err := runCodexAttemptActions([]codexAttemptAction{codexAttemptDial}, codexAttemptActionHandlers{}); err == nil {
		t.Fatal("missing action handler should fail")
	}
}

func TestCodexAttemptMachineObservesAcceptedTransitions(t *testing.T) {
	t.Parallel()
	var observed []codexAttemptTransition
	machine := newCodexAttemptMachine(func(transition codexAttemptTransition) {
		observed = append(observed, transition)
	})
	if _, err := machine.dispatch(codexAttemptConnectRequested, codexAttemptActionHandlers{Dial: func() error { return nil }}); err != nil {
		t.Fatal(err)
	}
	if _, err := machine.commitEvent(codexAttemptRequestSent); !errors.Is(err, errCodexAttemptTransition) {
		t.Fatalf("illegal transition error = %v", err)
	}
	if len(observed) != 1 || observed[0].From != codexAttemptPrepared || observed[0].To != codexAttemptConnecting {
		t.Fatalf("observed transitions = %+v", observed)
	}
}

func TestCodexAttemptCommitEventRejectsActionfulTransition(t *testing.T) {
	t.Parallel()
	machine := newCodexAttemptMachine()
	if _, err := machine.commitEvent(codexAttemptConnectRequested); !errors.Is(err, errCodexAttemptTransition) {
		t.Fatalf("commitEvent actionful error = %v", err)
	}
	if machine.current() != codexAttemptPrepared {
		t.Fatalf("state = %s", machine.current())
	}
}

func TestConnectCodexStreamAttemptOwnsDialStateTransitions(t *testing.T) {
	t.Parallel()
	machine := newCodexAttemptMachine()
	result, err := connectCodexStreamAttempt(machine, func() (codexConnectionLease, error) {
		return codexConnectionLease{conn: &websocket.Conn{}, source: codexWebsocketConnectionCold}, nil
	})
	if err != nil || result.conn == nil || machine.current() != codexAttemptReady {
		t.Fatalf("connection = %+v, state = %s, error = %v", result, machine.current(), err)
	}

	failed := newCodexAttemptMachine()
	if _, err = connectCodexStreamAttempt(failed, func() (codexConnectionLease, error) { return codexConnectionLease{}, errors.New("dial failed") }); err == nil {
		t.Fatal("dial failure was accepted")
	}
	if failed.current() != codexAttemptFailed {
		t.Fatalf("failed state = %s", failed.current())
	}
}

func TestCodexConnectionLeaseValidatesAndTransfersHandshakeOwnership(t *testing.T) {
	t.Parallel()
	response := &http.Response{StatusCode: http.StatusSwitchingProtocols}
	lease := codexConnectionLease{conn: &websocket.Conn{}, response: response, source: codexWebsocketConnectionCold}
	if err := lease.validate(); err != nil {
		t.Fatal(err)
	}
	if got := lease.takeHandshakeResponse(); got != response || lease.response != nil {
		t.Fatalf("handshake transfer = %p, retained=%p", got, lease.response)
	}
	if err := (codexConnectionLease{}).validate(); err == nil {
		t.Fatal("invalid lease was accepted")
	}
}

func TestCodexAttemptDispatchCommitsOnlyAfterActionsSucceed(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		prepare  []codexAttemptEvent
		event    codexAttemptEvent
		handlers codexAttemptActionHandlers
		want     codexAttemptState
	}{
		{name: "dial", event: codexAttemptConnectRequested, handlers: codexAttemptActionHandlers{Dial: func() error { return errors.New("dial") }}, want: codexAttemptPrepared},
		{name: "activate", prepare: []codexAttemptEvent{codexAttemptConnectRequested, codexAttemptConnected}, event: codexAttemptReaderActivated, handlers: codexAttemptActionHandlers{ActivateReader: func() error { return errors.New("activate") }}, want: codexAttemptReady},
		{name: "send", prepare: []codexAttemptEvent{codexAttemptConnectRequested, codexAttemptConnected, codexAttemptReaderActivated}, event: codexAttemptRequestSent, handlers: codexAttemptActionHandlers{SendRequest: func() error { return errors.New("send") }}, want: codexAttemptReaderReady},
	} {
		t.Run(test.name, func(t *testing.T) {
			machine := newCodexAttemptMachine()
			for _, event := range test.prepare {
				transition, err := machine.plan(event)
				if err != nil {
					t.Fatal(err)
				}
				if err = machine.commit(transition); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := machine.dispatch(test.event, test.handlers); err == nil {
				t.Fatal("failed action was accepted")
			}
			if got := machine.current(); got != test.want {
				t.Fatalf("state after failed action = %s, want %s", got, test.want)
			}
		})
	}
}

func TestCodexAttemptCanRecoverAfterRequestWriteFails(t *testing.T) {
	t.Parallel()
	state := codexAttemptReaderReady
	for _, step := range []struct {
		event codexAttemptEvent
		want  codexAttemptState
	}{
		{codexAttemptInterrupted, codexAttemptInterruptedState},
		{codexAttemptRetryApproved, codexAttemptRetrying},
		{codexAttemptConnectRequested, codexAttemptConnecting},
	} {
		transition, err := reduceCodexAttempt(state, step.event)
		if err != nil || transition.To != step.want {
			t.Fatalf("reduce(%s, %s) = %+v, %v", state, step.event, transition, err)
		}
		state = transition.To
	}
}

func TestCodexSessionReaderActivationIsExplicitAndNilFree(t *testing.T) {
	t.Parallel()
	session := &codexWebsocketSession{lifecycle: *newCodexSessionStateMachine()}
	if err := session.activateReader(nil); err == nil {
		t.Fatal("nil reader activation was accepted")
	}
	reader, _ := session.activeReaderSnapshot()
	if reader != nil || session.lifecycle.state() != codexSessionIdle {
		t.Fatalf("nil activation mutated session: channel=%v state=%s", reader, session.lifecycle.state())
	}
}

func TestCodexReaderActivationProjectsAttemptIntoBusySession(t *testing.T) {
	t.Parallel()
	session := &codexWebsocketSession{lifecycle: *newCodexSessionStateMachine()}
	for _, event := range []codexSessionEvent{codexEventDialRequested, codexEventConnected} {
		if err := session.transitionLifecycle(event); err != nil {
			t.Fatal(err)
		}
	}
	attempt := newCodexAttemptMachine()
	for _, event := range []codexAttemptEvent{codexAttemptConnectRequested, codexAttemptConnected} {
		transition, err := attempt.plan(event)
		if err != nil {
			t.Fatal(err)
		}
		if err = attempt.commit(transition); err != nil {
			t.Fatal(err)
		}
	}
	read, err := activateCodexStreamReader(attempt, session)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.current() != codexAttemptReaderReady || session.lifecycle.state() != codexSessionBusy {
		t.Fatalf("attempt=%s session=%s", attempt.current(), session.lifecycle.state())
	}
	session.deactivateReader(read)
	if session.lifecycle.state() != codexSessionReady {
		t.Fatalf("session after deactivation = %s", session.lifecycle.state())
	}
}

func TestCodexLifecycleProjectionRejectsDivergence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		attempt codexAttemptState
		session codexSessionState
		reader  bool
		valid   bool
	}{
		{codexAttemptReady, codexSessionReady, false, true},
		{codexAttemptReaderReady, codexSessionBusy, true, true},
		{codexAttemptActive, codexSessionReady, true, false},
		{codexAttemptReady, codexSessionBusy, false, false},
		{codexAttemptInterruptedState, codexSessionReady, false, true},
	} {
		if got := codexLifecycleProjectionValid(test.attempt, test.session, test.reader); got != test.valid {
			t.Fatalf("projection(%s,%s,%t)=%t", test.attempt, test.session, test.reader, got)
		}
	}
}

func TestProcessCodexStreamEventClassifiesTerminalAndError(t *testing.T) {
	t.Parallel()
	completed := processCodexStreamEvent([]byte(`{"type":"response.completed"}`))
	if !completed.terminal || completed.err != nil || completed.event.Kind != codexStreamTerminal {
		t.Fatalf("completed = %+v", completed)
	}
	failed := processCodexStreamEvent([]byte(`{"type":"error","status":500,"error":{"message":"boom"}}`))
	if !failed.terminal || failed.err == nil {
		t.Fatalf("failed = %+v", failed)
	}
}

func TestCodexStreamExecutionClassifiesFinalAttemptState(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		reason string
		want   codexAttemptState
	}{
		{name: "success", reason: "completed", want: codexAttemptCompleted},
		{name: "cancel", reason: "context_done", want: codexAttemptCancelled},
		{name: "failure", reason: "read_error", want: codexAttemptFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			attempt := newCodexAttemptMachine()
			for _, event := range []codexAttemptEvent{codexAttemptConnectRequested, codexAttemptConnected, codexAttemptReaderActivated, codexAttemptRequestSent} {
				transition, err := attempt.plan(event)
				if err != nil {
					t.Fatal(err)
				}
				if err = attempt.commit(transition); err != nil {
					t.Fatal(err)
				}
			}
			execution := newCodexStreamExecution(attempt)
			execution.fail(test.reason, errors.New(test.reason))
			if test.reason == "completed" {
				execution.err = nil
			}
			execution.finalizeAttempt()
			if got := attempt.current(); got != test.want {
				t.Fatalf("final state = %s, want %s", got, test.want)
			}
		})
	}
}

func TestReduceCodexTransactionRejectsIllegalTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state codexTransactionalState
		event codexTransactionalEvent
	}{
		{codexTransactionDisabled, codexTransactionCommit},
		{codexTransactionCommitted, codexTransactionDiscard},
		{codexTransactionDiscarded, codexTransactionOverflow},
		{codexTransactionPassthrough, codexTransactionCommit},
	}
	for _, test := range tests {
		if _, err := reduceCodexTransaction(test.state, test.event); !errors.Is(err, errCodexTransactionalTransition) {
			t.Fatalf("reduce(%s, %s) error = %v, want invalid transition", test.state, test.event, err)
		}
	}
}

func TestReduceCodexTransactionDefinesLegalTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		state codexTransactionalState
		event codexTransactionalEvent
		want  codexTransactionalState
	}{
		{codexTransactionBuffering, codexTransactionCommit, codexTransactionCommitted},
		{codexTransactionBuffering, codexTransactionDiscard, codexTransactionDiscarded},
		{codexTransactionBuffering, codexTransactionOverflow, codexTransactionPassthrough},
		{codexTransactionBuffering, codexTransactionRetryReset, codexTransactionBuffering},
		{codexTransactionPassthrough, codexTransactionRetryReset, codexTransactionPassthrough},
	}
	for _, test := range tests {
		got, err := reduceCodexTransaction(test.state, test.event)
		if err != nil || got != test.want {
			t.Fatalf("reduce(%s, %s) = %s, %v; want %s", test.state, test.event, got, err, test.want)
		}
	}
}

func TestNewCodexStreamDeliveryRejectsNilDeliverer(t *testing.T) {
	t.Parallel()
	if _, err := newCodexStreamDelivery(codexTransactionalStreamPolicy{mode: codexTransactionalChildUntilTerminal, maxBytes: 8}, nil, codexStreamDeliveryHooks{}); !errors.Is(err, errCodexStreamDelivererRequired) {
		t.Fatalf("error = %v, want required deliverer", err)
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
