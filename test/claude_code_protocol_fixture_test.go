package test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

type claudeProtocolManifest struct {
	Version         string                       `json:"version"`
	Inherits        string                       `json:"inherits,omitempty"`
	Fixtures        []string                     `json:"fixtures"`
	Classifications map[string]map[string]string `json:"classifications"`
}

func TestClaudeCodeProtocolFixturesClassifyEveryField(t *testing.T) {
	t.Parallel()
	base := filepath.Join("testdata", "claude_code_protocol")
	for _, version := range []string{"2.1.210", "2.1.211", "2.1.212"} {
		t.Run(version, func(t *testing.T) {
			root := filepath.Join(base, version)
			manifest := loadProtocolManifest(t, base, version, make(map[string]bool))
			if manifest.Version != version || len(manifest.Fixtures) == 0 {
				t.Fatalf("invalid manifest: %+v", manifest)
			}
			validateProtocolManifest(t, root, base, manifest)
		})
	}
}

func validateProtocolManifest(t *testing.T, root, base string, manifest claudeProtocolManifest) {
	t.Helper()
	allowed := map[string]bool{"passthrough": true, "translated": true, "emulated": true, "rejected": true, "ignored": true}
	for _, name := range manifest.Fixtures {
		fixtureRoot := root
		if _, err := os.Stat(filepath.Join(fixtureRoot, name)); err != nil && manifest.Inherits != "" {
			fixtureRoot = filepath.Join(base, manifest.Inherits)
		}
		var value any
		decodeJSONFile(t, filepath.Join(fixtureRoot, name), &value)
		paths := make(map[string]struct{})
		collectJSONLeafPaths(value, "", paths)
		classified := manifest.Classifications[name]
		for path := range paths {
			classification := classified[path]
			if !allowed[classification] {
				t.Errorf("%s field %q is unclassified (got %q)", name, path, classification)
			}
		}
		for path, classification := range classified {
			if _, exists := paths[path]; !exists {
				t.Errorf("%s classification references missing field %q", name, path)
			}
			if !allowed[classification] {
				t.Errorf("%s field %q has invalid classification %q", name, path, classification)
			}
		}
	}
}

func loadProtocolManifest(t *testing.T, base, version string, visiting map[string]bool) claudeProtocolManifest {
	t.Helper()
	if visiting[version] {
		t.Fatalf("protocol fixture inheritance cycle at %s", version)
	}
	visiting[version] = true
	var manifest claudeProtocolManifest
	decodeJSONFile(t, filepath.Join(base, version, "manifest.json"), &manifest)
	if manifest.Inherits != "" {
		parent := loadProtocolManifest(t, base, manifest.Inherits, visiting)
		if len(manifest.Fixtures) == 0 {
			manifest.Fixtures = parent.Fixtures
		}
		if len(manifest.Classifications) == 0 {
			manifest.Classifications = parent.Classifications
		}
	}
	delete(visiting, version)
	return manifest
}

func TestClaudeCodeProtocolFixturesContainNoPromptContent(t *testing.T) {
	t.Parallel()
	root := filepath.Join("testdata", "claude_code_protocol", "2.1.212")
	entries, errRead := os.ReadDir(root)
	if errRead != nil {
		t.Fatal(errRead)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		payload := string(mustReadFile(t, filepath.Join(root, entry.Name())))
		for _, forbidden := range []string{"npm test", "private-key", "user@example.com", "/Users/"} {
			if strings.Contains(payload, forbidden) {
				t.Errorf("%s contains sensitive fixture content %q", entry.Name(), forbidden)
			}
		}
	}
}

func decodeJSONFile(t *testing.T, path string, destination any) {
	t.Helper()
	if errDecode := json.Unmarshal(mustReadFile(t, path), destination); errDecode != nil {
		t.Fatalf("decode %s: %v", path, errDecode)
	}
}

func collectJSONLeafPaths(value any, path string, out map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			next := key
			if path != "" {
				next = path + "." + key
			}
			collectJSONLeafPaths(typed[key], next, out)
		}
	case []any:
		for _, item := range typed {
			collectJSONLeafPaths(item, path+"[]", out)
		}
	default:
		out[path] = struct{}{}
	}
}
