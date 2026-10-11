package chunkdb

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestIntegrationWorldExample(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	_, after, found := strings.Cut(string(readme), "```output\n")
	if !found {
		t.Fatal("README has no world example output")
	}
	expected, _, found := strings.Cut(after, "```")
	if !found {
		t.Fatal("README world example output has no closing fence")
	}
	s := startServer(t, serverConfig{workers: 4})
	for _, status := range []string{"applied", "skipped"} {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		command := exec.CommandContext(ctx, "go", "run", "-race", "./examples/world")
		command.Env = append(os.Environ(), "CHUNKDB_URI="+s.uri)
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
