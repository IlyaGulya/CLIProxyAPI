package claude

import (
	"fmt"
	"strings"
	"testing"
)

// These versions are the live compatibility envelope exercised by the
// claudex-next harness. Additions require corresponding classifier fixtures.
var characterizedClaudeCodeVersions = []string{"2.1.208", "2.1.210", "2.1.211", "2.1.212", "2.1.214"}

func TestCharacterizedClaudeCodeVersionsAreExplicit(t *testing.T) {
	t.Parallel()
	if len(characterizedClaudeCodeVersions) < 2 {
		t.Fatal("compatibility envelope must cover more than one Claude Code version")
	}
	for _, version := range characterizedClaudeCodeVersions {
		if strings.TrimSpace(version) == "" {
			t.Fatal("empty Claude Code version in compatibility envelope")
		}
	}
}

func BenchmarkClaudeCompatibilityRequest(b *testing.B) {
	for _, size := range []int{10 << 10, 100 << 10, 600 << 10} {
		b.Run(fmt.Sprintf("bytes_%d", size), func(b *testing.B) {
			body := []byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":"` + strings.Repeat("privacy-safe fixture ", size/21) + `"}]}`)
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for range b.N {
				repaired, _, errRepair := repairInterruptedClaudeToolHistory(body)
				if errRepair != nil {
					b.Fatal(errRepair)
				}
				repaired, _ = applyClaudeReactiveCompactBudget(repaired)
				repaired, _ = applyClaudeCompactionReplay(repaired, claudeCompactionV2RetainedTokenBudget)
				repaired, _ = applyClaudeContextEditing(repaired)
				_ = claudeContextPressure(repaired)
			}
		})
	}
}
