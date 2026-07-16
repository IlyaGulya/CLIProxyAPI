package claudexnext

import (
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

const (
	LGTMContainer = "claudex-next-otel-lgtm"
	LGTMImage     = "grafana/otel-lgtm:0.27.1"
	LGTMEndpoint  = "http://127.0.0.1:4318"
	GrafanaURL    = "http://127.0.0.1:3300"
)

type CommandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type ExecCommandRunner struct{}

func (ExecCommandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

type StackStatus struct {
	Available      bool   `json:"available"`
	Started        bool   `json:"started"`
	Image          string `json:"image"`
	Grafana        string `json:"grafana_url"`
	Endpoint       string `json:"otlp_endpoint"`
	Error          string `json:"error,omitempty"`
	Dashboard      bool   `json:"dashboard_provisioned"`
	DashboardError string `json:"dashboard_error,omitempty"`
}

// EnsureLGTM lazily creates a persistent local dev stack. Failure is returned
// as status rather than an error so observability can never block Claude.
func EnsureLGTM(ctx context.Context, runner CommandRunner, health func(context.Context, string) error) StackStatus {
	status := StackStatus{Image: LGTMImage, Grafana: GrafanaURL, Endpoint: LGTMEndpoint}
	output, errInspect := runner.Run(ctx, "docker", "inspect", "--format", "{{.State.Running}}", LGTMContainer)
	if errInspect == nil && strings.TrimSpace(string(output)) == "true" {
		if errHealth := health(ctx, GrafanaURL+"/api/health"); errHealth == nil {
			status.Available = true
			return status
		}
	}
	if errInspect == nil {
		_, _ = runner.Run(ctx, "docker", "start", LGTMContainer)
	} else {
		output, errRun := runner.Run(ctx, "docker", "run", "-d", "--name", LGTMContainer,
			"--restart", "unless-stopped",
			"-p", "127.0.0.1:3300:3000",
			"-p", "127.0.0.1:4317:4317",
			"-p", "127.0.0.1:4318:4318",
			"-v", "claudex-next-otel-lgtm-data:/data",
			LGTMImage)
		if errRun != nil {
			status.Error = fmt.Sprintf("start LGTM: %v: %s", errRun, strings.TrimSpace(string(output)))
			return status
		}
		status.Started = true
	}
	if errHealth := health(ctx, GrafanaURL+"/api/health"); errHealth != nil {
		status.Error = "LGTM health check: " + errHealth.Error()
		return status
	}
	status.Available = true
	return status
}

func WaitForHTTP(ctx context.Context, url string) error {
	deadline := time.Now().Add(45 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if response, errDo := client.Do(req); errDo == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for %s", url)
}
