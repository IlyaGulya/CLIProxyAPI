package main

import (
	"reflect"
	"testing"
)

func TestParseArgsPassesUnknownClaudeFlagsThrough(t *testing.T) {
	t.Parallel()
	opts, claudeArgs, err := parseArgs([]string{"--effort", "xhigh", "--model", "custom", "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.proxyBinary != "" {
		t.Fatalf("unexpected launcher options: %+v", opts)
	}
	want := []string{"--effort", "xhigh", "--model", "custom", "hello"}
	if !reflect.DeepEqual(claudeArgs, want) {
		t.Fatalf("claude args = %#v, want %#v", claudeArgs, want)
	}
}

func TestParseArgsExtractsNamespacedLauncherFlags(t *testing.T) {
	t.Parallel()
	opts, claudeArgs, err := parseArgs([]string{"--next-proxy-binary=/proxy", "--next-claude-binary", "/claude", "--print", "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if opts.proxyBinary != "/proxy" || opts.claudeBinary != "/claude" {
		t.Fatalf("launcher options = %+v", opts)
	}
	want := []string{"--print", "hi"}
	if !reflect.DeepEqual(claudeArgs, want) {
		t.Fatalf("claude args = %#v, want %#v", claudeArgs, want)
	}
}

func TestPrintModeNeverUsesInteractiveTerminalRecorder(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{{"--print", "hello"}, {"-p", "hello"}, {"--print=true", "hello"}} {
		if !isPrintMode(args) {
			t.Errorf("isPrintMode(%#v) = false", args)
		}
	}
	if isPrintMode([]string{"hello"}) {
		t.Fatal("ordinary interactive args detected as print mode")
	}
}
