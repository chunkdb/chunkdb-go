package chunkdb

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIntegrationBackup(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "plain"
		if secure {
			name = "TLS"
		}
		t.Run(name, func(t *testing.T) {
			backups, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			server := startServer(t, serverConfig{tls: secure, args: []string{"--backup-dir", backups}})
			client := connectIntegration(t, server, func(opts *Options) {
				if secure {
					opts.CA = server.caPEM
				}
			})
			ctx := t.Context()
			if _, err := client.Do(ctx, "CREATE TABLE world (n u8) CHUNK 2 x 2"); err != nil {
				t.Fatal(err)
			}
			written, err := client.Do(ctx, "SET BLOCK 0 0 IN world n = 7")
			version, ok := written.Uint64()
			if err != nil || !ok || version == 0 {
				t.Fatalf("write: %+v, %v", written, err)
			}
			targetName := "it's-backup"
			statement := "BACKUP TO '" + strings.ReplaceAll(targetName, "'", "''") + "'"
			reply, err := client.Do(ctx, statement)
			if err != nil || reply.Kind != ReplyMap {
				t.Fatalf("backup: %+v, %v", reply, err)
			}
			count := func(key string) uint64 {
				value, present := reply.Lookup(key)
				n, valid := value.Uint64()
				if !present || !valid || n == 0 {
					t.Fatalf("backup %s: %+v", key, reply)
				}
				return n
			}
			tables := count("tables")
			count("files")
			count("bytes")
			cuts, present := reply.Lookup("cuts")
			if !present || cuts.Kind != ReplyArray || uint64(len(cuts.Array)) != tables {
				t.Fatalf("backup cuts: %+v", reply)
			}
			found := false
			for _, cut := range cuts.Array {
				table, _ := cut.Lookup("table")
				if cut.Kind != ReplyMap || table.Kind != ReplyBulk {
					t.Fatalf("invalid cut: %+v", cut)
				}
				if string(table.Bulk) == "world" {
					epoch, _ := cut.Lookup("epoch")
					revision, _ := cut.Lookup("revision")
					n, valid := revision.Uint64()
					if found || epoch.Kind != ReplyBulk || len(epoch.Bulk) != 32 || !valid || n < version {
						t.Fatalf("world cut: %+v, write version %d", cut, version)
					}
					found = true
				}
			}
			if !found {
				t.Fatalf("missing world cut: %+v", cuts)
			}
			target := filepath.Join(backups, targetName)
			for _, path := range []string{"chunkdb.backup", "chunkdb.manifest", "chunkdb.users", "tables/world/table.manifest"} {
				info, err := os.Stat(filepath.Join(target, filepath.FromSlash(path)))
				if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
					t.Fatalf("snapshot %s: %v, %v", path, info, err)
				}
			}
			dataFiles := 0
			if err := filepath.WalkDir(filepath.Join(target, "tables", "world"), func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() || (!strings.HasSuffix(path, ".chk") && !strings.HasSuffix(path, ".wal")) {
					return nil
				}
				info, err := entry.Info()
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() || info.Size() == 0 {
					t.Fatalf("empty snapshot data file %s", path)
				}
				dataFiles++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if dataFiles == 0 {
				t.Fatal("snapshot contains no world chunk data")
			}
			if _, err := client.Do(ctx, statement); err == nil {
				t.Fatal("backup reused a nonempty target")
			}
			if err := client.Ping(ctx); err != nil {
				t.Fatalf("Ping after rejected backup: %v", err)
			}
		})
	}
}
