package claudexnext

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"net/http"
	"strings"
)

//go:embed grafana/claudex-next-overview.json
var grafanaDashboard []byte

func ProvisionGrafana(ctx context.Context, baseURL string, client *http.Client) error {
	if client == nil {
		client = http.DefaultClient
	}
	request, errRequest := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/dashboards/db", bytes.NewReader(grafanaDashboard))
	if errRequest != nil {
		return fmt.Errorf("create Grafana dashboard request: %w", errRequest)
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth("admin", "admin")
	response, errDo := client.Do(request)
	if errDo != nil {
		return fmt.Errorf("provision Grafana dashboard: %w", errDo)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("provision Grafana dashboard: status %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
