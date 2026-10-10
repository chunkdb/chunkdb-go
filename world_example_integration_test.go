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
		for _, want := range []string{"schema: " + status, "area: 4 chunks, 16 blocks written", "block (0,0): tile=1 label=grass", "change (0,0): tile=2 label=water"} {
			if !strings.Contains(string(output), want) {
				t.Fatalf("world example missing %q:\n%s", want, output)
			}
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
