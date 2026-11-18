package chunkdb

import (
	"context"
	neturl "net/url"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// worldReadmeCommand is the README's run command; the test runs the local
// example in its place, so a change is checked before it is published.
const worldReadmeCommand = "go run github.com/chunkdb/chunkdb-go/v2/examples/world@latest"

// readmeBlock returns the body of the first fenced block of the given kind
// in the README's world section.
func readmeBlock(t *testing.T, kind string) string {
	t.Helper()
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	_, section, found := strings.Cut(string(readme), "## Store and watch blocks\n")
	if !found {
		t.Fatal("README has no world section")
	}
	_, after, found := strings.Cut(section, "```"+kind+"\n")
	if !found {
		t.Fatalf("README has no %s block", kind)
	}
	body, _, found := strings.Cut(after, "```")
	if !found {
		t.Fatalf("README %s block has no closing fence", kind)
	}
	return body
}

// TestWorldReadmeSnippet checks that the README's Migrate snippet is the
// code the world example runs.
func TestWorldReadmeSnippet(t *testing.T) {
	source, err := os.ReadFile("examples/world/main.go")
	if err != nil {
		t.Fatal(err)
	}
	squash := func(text string) string { return strings.Join(strings.Fields(text), " ") }
	snippet := readmeBlock(t, "go")
	if !strings.Contains(snippet, "client.Migrate(") || !strings.Contains(squash(string(source)), squash(snippet)) {
		t.Fatalf("README Go snippet is not in examples/world/main.go:\n%s", snippet)
	}
}

func TestIntegrationWorldExample(t *testing.T) {
	commands := readmeBlock(t, "bash")
	expected := readmeBlock(t, "output")
	if !strings.Contains(commands, "\n"+worldReadmeCommand+"\n") || !strings.Contains(commands, "127.0.0.1:4242") {
		t.Fatalf("README world commands changed:\n%s", commands)
	}
	s := startServer(t, serverConfig{workers: 4})
	script := strings.Replace(commands, worldReadmeCommand, "go run -race ./examples/world", 1)
	script = strings.ReplaceAll(script, "127.0.0.1:4242", s.host+":"+strconv.Itoa(s.port))
	for _, status := range []string{"applied", "skipped"} {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		command := exec.CommandContext(ctx, "sh", "-e", "-c", script)
		// The Quick start password is URI-safe; the test password needs escaping.
		command.Env = append(os.Environ(), "CHUNKDB_PASSWORD="+neturl.QueryEscape(testAdminPassword))
		output, err := command.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("world example: %v\n%s", err, output)
		}
		t.Logf("world example (%s):\n%s", status, output)
		want := strings.Replace(expected, "schema: applied", "schema: "+status, 1)
		if string(output) != want {
			t.Fatalf("world example output = %q, want README output %q", output, want)
		}
	}
	c := connectIntegration(t, s, nil)
	area, err := c.GetArea(t.Context(), "world_go", 0, 0, 1, 1)
	if err != nil || len(area) != 4 {
		t.Fatal(area, err)
	}
	for y := int64(0); y < 4; y++ {
		for x := int64(0); x < 4; x++ {
			want := Record{"tile": uint64(1), "label": "grass"}
			if x == 0 && y == 0 {
				want = Record{"tile": uint64(2), "label": "water"}
			}
			got, err := c.GetBlock(t.Context(), "world_go", x, y)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatal(x, y, got, err)
			}
		}
	}
}
