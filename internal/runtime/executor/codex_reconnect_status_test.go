package executor

import (
	"testing"
	"time"
)

func TestCodexReconnectStatusPolicy(t *testing.T) {
	tests := []struct {
		name    string
		input   codexReconnectStatusInput
		visible bool
		status  string
	}{
		{name: "first quick recovery suppressed", input: codexReconnectStatusInput{Attempt: 1, MaxAttempts: 2, Elapsed: 100 * time.Millisecond, Recovered: true}, status: "recovered"},
		{name: "first slow recovery visible", input: codexReconnectStatusInput{Attempt: 1, MaxAttempts: 2, Elapsed: time.Second, Recovered: true}, visible: true, status: "recovered"},
		{name: "second attempt visible", input: codexReconnectStatusInput{Attempt: 2, MaxAttempts: 3}, visible: true, status: "reconnecting"},
		{name: "terminal visible", input: codexReconnectStatusInput{Attempt: 1, MaxAttempts: 1, Terminal: true}, visible: true, status: "failed"},
		{name: "http fallback visible", input: codexReconnectStatusInput{CircuitOpen: true}, visible: true, status: "https_fallback"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := decideCodexReconnectStatus(tt.input)
			if decision.Visible != tt.visible || decision.Status != tt.status {
				t.Fatalf("decision = %+v", decision)
			}
		})
	}
}
