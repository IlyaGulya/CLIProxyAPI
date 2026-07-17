package claude

import (
	"errors"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestClaudeFallbackEligibilityMatchesClaudeCodeWireContract(t *testing.T) {
	tests := []struct {
		name   string
		status int
		err    error
		want   string
	}{
		{name: "model unavailable", status: http.StatusNotFound, want: "model_not_found"},
		{name: "authentication", status: http.StatusUnauthorized, want: "authentication"},
		{name: "permission", status: http.StatusForbidden, want: "permission"},
		{name: "rate limited", status: http.StatusTooManyRequests, want: "rate_limited"},
		{name: "overloaded", status: 529, want: "overloaded"},
		{name: "circuit open", status: http.StatusServiceUnavailable, err: &coreauth.Error{Code: "circuit_open", Message: "open", HTTPStatus: http.StatusServiceUnavailable}, want: "circuit_open"},
		{name: "server error", status: http.StatusBadGateway, want: "server_error"},
		{name: "invalid request", status: http.StatusBadRequest},
		{name: "context too large", status: http.StatusRequestEntityTooLarge},
		{name: "refusal is successful response", status: http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.err
			if err == nil {
				err = errors.New(http.StatusText(tt.status))
			}
			got, eligible := claudeFallbackEligibility(&interfaces.ErrorMessage{StatusCode: tt.status, Error: err})
			if eligible != (tt.want != "") || got != tt.want {
				t.Fatalf("eligibility = (%q, %t), want (%q, %t)", got, eligible, tt.want, tt.want != "")
			}
		})
	}
}
