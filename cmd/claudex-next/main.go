package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/claudexnext"
)

func main() {
	launcher, claudeArgs, errArgs := parseArgs(os.Args[1:])
	if errArgs != nil {
		_, _ = fmt.Fprintf(os.Stderr, "claudex-next: %v\n", errArgs)
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	_, exitCode, errRun := claudexnext.Run(ctx, claudexnext.Options{
		ClaudeArgs: claudeArgs, ClaudeBin: launcher.claudeBinary, ProxyBin: launcher.proxyBinary,
		ConfigPath: launcher.configPath, EnvPath: launcher.envPath, RunsDir: launcher.runsDir,
		Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, Interactive: isTerminal(os.Stdin) && isTerminal(os.Stdout),
	})
	if errRun != nil {
		var exitErr *exec.ExitError
		if !errors.As(errRun, &exitErr) {
			_, _ = fmt.Fprintf(os.Stderr, "claudex-next: %v\n", errRun)
		}
	}
	os.Exit(exitCode)
}

type launcherOptions struct {
	proxyBinary  string
	claudeBinary string
	configPath   string
	envPath      string
	runsDir      string
}

func parseArgs(args []string) (launcherOptions, []string, error) {
	var options launcherOptions
	claudeArgs := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := args[index]
		name, value, hasValue := strings.Cut(arg, "=")
		var destination *string
		switch name {
		case "--next-proxy-binary":
			destination = &options.proxyBinary
		case "--next-claude-binary":
			destination = &options.claudeBinary
		case "--next-proxy-config":
			destination = &options.configPath
		case "--next-env-file":
			destination = &options.envPath
		case "--next-runs-dir":
			destination = &options.runsDir
		default:
			claudeArgs = append(claudeArgs, arg)
			continue
		}
		if !hasValue {
			index++
			if index >= len(args) {
				return options, nil, fmt.Errorf("%s requires a value", name)
			}
			value = args[index]
		}
		*destination = value
	}
	return options, claudeArgs, nil
}

func isTerminal(file *os.File) bool {
	info, errStat := file.Stat()
	return errStat == nil && info.Mode()&os.ModeCharDevice != 0
}
