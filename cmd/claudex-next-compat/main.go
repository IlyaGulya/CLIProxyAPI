package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/claudexnext"
)

func main() {
	version := flag.String("version", "2.1.212", "Claude Code schema version")
	input := flag.String("input", "", "harness JSONL to summarize (stdin when -)")
	runDir := flag.String("run", "", "claudex-next run directory to verify")
	caseName := flag.String("case", "", "compatibility matrix case to verify")
	flag.Parse()
	var value any = claudexnext.ClaudeHarnessCompatibilityMatrix(*version)
	if *input != "" {
		reader, closeInput, err := openInput(*input)
		if err != nil {
			fatal(err)
		}
		defer closeInput()
		report, err := claudexnext.SummarizeClaudeHarnessJSONL(reader, *version)
		if err != nil {
			fatal(err)
		}
		value = report
	}
	if *runDir != "" || *caseName != "" {
		if *runDir == "" || *caseName == "" {
			fatal(fmt.Errorf("--run and --case must be used together"))
		}
		matrix := claudexnext.ClaudeHarnessCompatibilityMatrix(*version)
		var selected *claudexnext.ClaudeHarnessCompatibilityCase
		for i := range matrix.Cases {
			if matrix.Cases[i].Name == *caseName {
				selected = &matrix.Cases[i]
				break
			}
		}
		if selected == nil {
			fatal(fmt.Errorf("unknown compatibility case %q", *caseName))
		}
		if err := claudexnext.VerifyClaudeHarnessCase(*runDir, *selected); err != nil {
			fatal(err)
		}
		value = map[string]any{"case": *caseName, "passed": true, "run_dir": *runDir}
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		fatal(err)
	}
}

func openInput(path string) (io.Reader, func(), error) {
	if path == "-" {
		return os.Stdin, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open harness stream: %w", err)
	}
	return file, func() { _ = file.Close() }, nil
}

func fatal(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "claudex-next-compat: %v\n", err)
	os.Exit(1)
}
