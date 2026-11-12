package chunkdb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// holdingProxy forwards connections to a server until hold is called.
// After that it discards bytes from clients, so acknowledgements never reach the server.
type holdingProxy struct {
	listener net.Listener
	target   string
	holding  atomic.Bool
	mu       sync.Mutex
	conns    []net.Conn
}

func startHoldingProxy(t *testing.T, target string) *holdingProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &holdingProxy{listener: listener, target: target}
	t.Cleanup(p.close)
	go p.serve()
	return p
}

func (p *holdingProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, client, server)
		p.mu.Unlock()
		go func() {
			_, _ = io.Copy(client, server)
			client.Close()
		}()
		go func() {
			buffer := make([]byte, 32*1024)
			for {
				n, err := client.Read(buffer)
				if n > 0 && !p.holding.Load() {
					if _, err := server.Write(buffer[:n]); err != nil {
						break
					}
				}
				if err != nil {
					break
				}
			}
			server.Close()
		}()
	}
}

func (p *holdingProxy) hold() { p.holding.Store(true) }

func (p *holdingProxy) close() {
	p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
}

// collector runs the journal example and records its output.
type collector struct {
	command *exec.Cmd
	lines   chan string
	exited  chan struct{}
	err     error
	mu      sync.Mutex
	output  strings.Builder
}

func startCollector(t *testing.T, binary, uri, journal string, args ...string) *collector {
	t.Helper()
	command := exec.Command(binary, append([]string{"-table", "journal_go", "-file", journal}, args...)...)
	command.Env = append(os.Environ(), "CHUNKDB_URI="+uri)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	// The example prints a few lines per change; the buffer holds every line of this test.
	c := &collector{command: command, lines: make(chan string, 1024), exited: make(chan struct{})}
	command.Stderr = &lockedWriter{c: c}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			c.record(scanner.Text() + "\n")
			c.lines <- scanner.Text()
		}
		close(c.lines)
		c.err = command.Wait()
		close(c.exited)
	}()
	t.Cleanup(func() {
		_ = command.Process.Kill()
		<-c.exited
	})
	return c
}

type lockedWriter struct{ c *collector }

func (w *lockedWriter) Write(b []byte) (int, error) {
	w.c.record(string(b))
	return len(b), nil
}

func (c *collector) record(text string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.output.WriteString(text)
}

func (c *collector) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.output.String()
}

// waitLine returns the next output line, which must start with prefix.
func (c *collector) waitLine(t *testing.T, prefix string) string {
	t.Helper()
	select {
	case line, ok := <-c.lines:
		if !ok || !strings.HasPrefix(line, prefix) {
			t.Fatalf("collector line %q, want prefix %q; output:\n%s", line, prefix, c.text())
		}
		return line
	case <-time.After(30 * time.Second):
		t.Fatalf("no collector line %q; output:\n%s", prefix, c.text())
	}
	return ""
}

// waitStored waits for the stored line of a change; only skipped duplicates and schema lines may come first.
func (c *collector) waitStored(t *testing.T, epoch string, revision uint64) {
	t.Helper()
	want := fmt.Sprintf("stored %s:%d", epoch, revision)
	for {
		if line := c.waitLine(t, ""); line == want {
			return
		} else if !strings.HasPrefix(line, "skipped ") && !strings.HasPrefix(line, "stored schema ") {
			t.Fatalf("collector line %q, want %q; output:\n%s", line, want, c.text())
		}
	}
}

func (c *collector) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-c.exited:
		return c.err
	case <-time.After(30 * time.Second):
		t.Fatalf("collector did not exit; output:\n%s", c.text())
	}
	return nil
}

// journalLines returns the journal as "schema <version>" and "change <revision> schema <version>" lines.
func journalLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	for _, line := range strings.SplitAfter(string(data), "\n") {
		if line == "" {
			continue
		}
		var stored struct {
			Kind          string            `json:"kind"`
			Revision      uint64            `json:"revision"`
			SchemaVersion uint64            `json:"schema_version"`
			Columns       []json.RawMessage `json:"columns"`
			Blocks        []struct {
				After map[string]any `json:"after"`
			} `json:"blocks"`
		}
		if !strings.HasSuffix(line, "\n") || json.Unmarshal([]byte(line), &stored) != nil {
			t.Fatalf("journal line %q", line)
		}
		switch {
		case stored.Kind == "schema" && len(stored.Columns) > 0:
			lines = append(lines, fmt.Sprintf("schema %d", stored.SchemaVersion))
		case stored.Kind == "change" && len(stored.Blocks) == 1:
			lines = append(lines, fmt.Sprintf("change %d schema %d", stored.Revision, stored.SchemaVersion))
		default:
			t.Fatalf("journal line %q", line)
		}
	}
	return lines
}

func TestIntegrationJournalExampleResumesWithoutDuplicates(t *testing.T) {
	s := startServer(t, serverConfig{workers: 6})
	binary := filepath.Join(t.TempDir(), "journal")
	if output, err := exec.CommandContext(t.Context(), "go", "build", "-race", "-o", binary, "./examples/journal").CombinedOutput(); err != nil {
		t.Fatalf("build journal example: %v\n%s", err, output)
	}
	c := connectIntegration(t, s, nil)
	createTable(t, c, "journal_go", TableSpec{Columns: []ColumnDef{{Name: "tile", Type: TypeUint(8)}}, ChunkWidth: 2, ChunkHeight: 2})
	// A change before the slot exists is not in the journal.
	if _, err := c.SetBlock(t.Context(), "journal_go", 0, 0, Record{"tile": 1}); err != nil {
		t.Fatal(err)
	}
	schema, err := c.Describe(t.Context(), "journal_go")
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(t.TempDir(), "journal.jsonl")
	proxy := startHoldingProxy(t, s.address())
	parsed, err := ParseURI(s.uri)
	if err != nil {
		t.Fatal(err)
	}
	host, port, _ := net.SplitHostPort(proxy.listener.Addr().String())
	parsed.Host = host
	if parsed.Port, err = strconv.Atoi(port); err != nil {
		t.Fatal(err)
	}

	first := startCollector(t, binary, parsed.String(), journal)
	first.waitLine(t, "watching journal_go slot journal from ")
	// From here no acknowledgement reaches the server, as if each one were lost to a crash after the store synced.
	proxy.hold()
	var stored []uint64
	want := []string{"schema 1"}
	for x := int64(1); x <= 3; x++ {
		revision, err := c.SetBlock(t.Context(), "journal_go", x, 0, Record{"tile": x})
		if err != nil {
			t.Fatal(err)
		}
		first.waitStored(t, schema.Epoch, revision)
		stored = append(stored, revision)
		want = append(want, fmt.Sprintf("change %d schema 1", revision))
	}
	// The first collector stored and synced each change, but none of its acknowledgements arrived.
	if err := first.command.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = first.wait(t)
	slots, err := c.Slots(t.Context(), "journal_go")
	if err != nil || len(slots) != 1 || slots[0].Acked >= stored[0] {
		t.Fatalf("slot before resume = %+v, %v; want acked below %d", slots, err, stored[0])
	}
	// A crash during a write leaves a last line without its newline, which the resumed run removes.
	torn, err := os.OpenFile(journal, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := torn.WriteString(`{"kind":"change","epoch":"`); err != nil {
		t.Fatal(err)
	}
	if err := torn.Close(); err != nil {
		t.Fatal(err)
	}

	// The server can still hold the killed collector's watch, so the resumed run takes it over.
	second := startCollector(t, binary, s.uri, journal, "-takeover")
	second.waitLine(t, "watching journal_go slot journal from ")
	revision, err := c.SetBlock(t.Context(), "journal_go", 0, 1, Record{"tile": 4})
	if err != nil {
		t.Fatal(err)
	}
	second.waitStored(t, schema.Epoch, revision)
	stored = append(stored, revision)
	want = append(want, fmt.Sprintf("change %d schema 1", revision))
	// A schema change is stored once, before the first change that uses its columns.
	if err := c.AddColumn(t.Context(), "journal_go", ColumnDef{Name: "light", Type: TypeUint(4), Default: 15}); err != nil {
		t.Fatal(err)
	}
	if revision, err = c.SetBlock(t.Context(), "journal_go", 1, 1, Record{"tile": 5}); err != nil {
		t.Fatal(err)
	}
	second.waitStored(t, schema.Epoch, revision)
	stored = append(stored, revision)
	want = append(want, "schema 2", fmt.Sprintf("change %d schema 2", revision))
	if err := second.command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := second.wait(t); err != nil || !strings.HasSuffix(second.text(), "stopped\n") {
		t.Fatalf("second collector: %v\n%s", err, second.text())
	}
	if got := journalLines(t, journal); !reflect.DeepEqual(got, want) {
		t.Fatalf("journal = %q, want each change and schema version once: %q", got, want)
	}
	slots, err = c.Slots(t.Context(), "journal_go")
	if err != nil || len(slots) != 1 || slots[0].Acked != revision {
		t.Fatalf("slot after resume = %+v, %v; want acked %d", slots, err, revision)
	}
}
