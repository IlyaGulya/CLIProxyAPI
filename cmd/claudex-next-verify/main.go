package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/claudexnext"
)

func main() {
	runDir := ""
	if len(os.Args) > 1 {
		runDir = os.Args[1]
	} else if home, errHome := os.UserHomeDir(); errHome == nil {
		runDir = filepath.Join(home, ".claudex-next", "latest")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, errVerify := claudexnext.VerifyRun(ctx, runDir, "", nil)
	if errVerify != nil {
		_, _ = fmt.Fprintf(os.Stderr, "claudex-next-verify: %v\n", errVerify)
		os.Exit(2)
	}
	_ = claudexnext.WriteVerification(runDir, report)
	payload, _ := json.MarshalIndent(report, "", "  ")
	_, _ = os.Stdout.Write(append(payload, '\n'))
	if !report.Passed {
		os.Exit(1)
	}
}
