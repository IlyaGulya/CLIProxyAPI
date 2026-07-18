package claude

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestClaudeCompatibilityLayerOwnership(t *testing.T) {
	t.Parallel()
	entries, errReadDir := os.ReadDir(".")
	if errReadDir != nil {
		t.Fatal(errReadDir)
	}
	forbidden := map[string]bool{
		"applyClaudeReactiveCompactBudget":           true,
		"applyClaudeAdaptiveOutputBudget":            true,
		"applyClaudeAdaptiveOutputBudgetForEstimate": true,
		"claudePreflightTokenizer":                   true,
	}
	seenPipelineAdapter := false
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		parsed, errParse := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, 0)
		if errParse != nil {
			t.Fatalf("parse %s: %v", entry.Name(), errParse)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			identifier, ok := node.(*ast.Ident)
			if !ok {
				return true
			}
			if forbidden[identifier.Name] {
				t.Errorf("legacy compatibility authority %s remains in %s", identifier.Name, entry.Name())
			}
			if entry.Name() == "code_handlers.go" && identifier.Name == "newClaudeRequestPipeline" {
				seenPipelineAdapter = true
			}
			return true
		})
	}
	if !seenPipelineAdapter {
		t.Fatal("code_handlers.go must delegate request mutation to newClaudeRequestPipeline")
	}
}
