package chunkdb

import (
	"math"
	"testing"
)

func TestIntegrationConnectionControls(t *testing.T) {
	for _, tlsEnabled := range []bool{false, true} {
		name := "plain"
		if tlsEnabled {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			server := startServer(t, serverConfig{workers: 4, tls: tlsEnabled})
			c := connectIntegration(t, server, func(o *Options) {
				o.TLSInsecure = tlsEnabled
				o.SessionOptions = map[string]any{"statement_timeout_ms": uint64(0)}
			})
			info := c.ServerInfo()
			if !info.Supports("connection_controls") || !info.Supports("statement_timeout") {
				t.Fatal("matching server lacks connection controls", info)
			}
			timeout, ok := info.Options["statement_timeout_ms"].Uint64()
			if !ok || timeout != 0 {
				t.Fatal(info.Options)
			}
			rows, err := c.ShowConnections(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, row := range rows {
				if row.ID == info.ConnectionID {
					found = true
					if row.Peer == "" || row.State == "" {
						t.Fatal(row)
					}
				}
			}
			if !found {
				t.Fatal("calling connection missing", rows, info.ConnectionID)
			}
			if yes, err := c.Cancel(t.Context(), info.ConnectionID); err != nil || yes {
				t.Fatal(yes, err)
			}
			if yes, err := c.Cancel(t.Context(), math.MaxUint64); err != nil || yes {
				t.Fatal(yes, err)
			}
			if yes, err := c.KillConnection(t.Context(), info.ConnectionID); err != nil || !yes {
				t.Fatal(yes, err)
			}
		})
	}
}
