package chunkdb

import (
	"context"
	"go/format"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var documentationExampleNames = map[string]bool{
	"ExampleClient_SetBlock":    true,
	"ExampleClient_GetChunk":    true,
	"ExampleClient_Transaction": true,
	"ExampleClient_Watch":       true,
	"ExampleClient_CreateSlot":  true,
	"ExampleConnect":            true,
	"ExampleConnectPool":        true,
	"ExampleClient_CreateUser":  true,
	"ExampleParseBits":          true,
	"ExampleClient_Migrate":     true,
	"ExampleClient_Do_backup":   true,
	"ExampleClient_FlushWAL":    true,
}

// Examples without Output comments compile under go test. This test executes
// them in separate processes against real servers and checks the guide output.
func TestIntegrationDocumentationExamples(t *testing.T) {
	serverBinary(t)
	source, err := os.ReadFile("example_test.go")
	if err != nil {
		t.Fatal(err)
	}
	pages, err := filepath.Glob("docs/*.md")
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile("(?s)<!-- go-example: ([A-Za-z_]+) -->\\n```go\\n(.*?)\\n```\\n\\n```output\\n(.*?)\\n```")
	seen := make(map[string]bool)
	for _, page := range pages {
		doc, err := os.ReadFile(page)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range pattern.FindAllStringSubmatch(string(doc), -1) {
			name, input, expected := match[1], match[2], match[3]+"\n"
			seen[name] = true
			t.Run(name, func(t *testing.T) {
				if !documentationExampleNames[name] {
					t.Fatalf("unknown example %s in %s", name, page)
				}
				start := strings.Index(string(source), "// docs:begin "+name+"\n")
				end := strings.Index(string(source), "// docs:end "+name+"\n")
				if start < 0 || end <= start {
					t.Fatalf("missing source markers for %s", name)
				}
				body := string(source)[start+len("// docs:begin "+name+"\n") : end]
				formatBody := func(code string) string {
					formatted, err := format.Source([]byte("package main\nfunc example() {\n" + strings.TrimSpace(code) + "\n}\n"))
					if err != nil {
						t.Fatalf("invalid %s input: %v", name, err)
					}
					return string(formatted)
				}
				if formatBody(input) != formatBody(body) {
					t.Fatalf("%s snippet differs from checked Example function", page)
				}
				server := startServer(t, serverConfig{args: []string{"--backup-dir", t.TempDir()}})
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				client := connectIntegration(t, server, nil)
				t.Logf("server: %s; protocol: %d", client.ServerInfo().ServerVersion, client.ServerInfo().Protocol)
				if err := client.CreateTable(ctx, "world_go", TableSpec{
					Columns:    []ColumnDef{{Name: "tile", Type: TypeUint(8)}, {Name: "label", Type: TypeText(16)}},
					ChunkWidth: 2, ChunkHeight: 2,
				}); err != nil {
					t.Fatal(err)
				}
				for _, x := range []int64{0, 2} {
					if _, err := client.SetBlock(ctx, "world_go", x, 0, Record{"tile": 1, "label": "grass"}); err != nil {
						t.Fatal(err)
					}
				}
				// Release the setup connection before the example opens its own
				// connections; each ordinary connection occupies one worker.
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
				binary, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				command := exec.CommandContext(ctx, binary, "-test.run=^TestDocumentationExampleProcess$", "-test.timeout=10s")
				command.Env = append(os.Environ(), "CHUNKDB_URI="+server.uri, "CHUNKDB_DOCUMENTATION_EXAMPLE="+name)
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("%s failed: %v\n%s", name, err, output)
				}
				actual := strings.TrimSuffix(string(output), "PASS\n")
				if actual != expected {
					t.Fatalf("%s output = %q, want %q", page, actual, expected)
				}
				t.Logf("%s:\n%s", page, actual)
			})
		}
	}
	for name := range documentationExampleNames {
		if !seen[name] {
			t.Errorf("%s has no checked guide block", name)
		}
	}
}
