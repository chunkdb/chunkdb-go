package chunkdb

import (
	"errors"
	"testing"
)

func TestParseURI(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want URI
	}{
		{
			name: "full",
			raw:  "chunk://chunk-token@127.0.0.1:4242/",
			want: URI{Scheme: "chunk", Host: "127.0.0.1", Port: 4242, Token: "chunk-token", Path: "/"},
		},
		{
			name: "secure",
			raw:  "chunks://chunk-token@chunkdb.local:5000/",
			want: URI{Scheme: "chunks", Secure: true, Host: "chunkdb.local", Port: 5000, Token: "chunk-token", Path: "/"},
		},
		{
			name: "default port and path",
			raw:  "chunk://host",
			want: URI{Scheme: "chunk", Host: "host", Port: DefaultPort, Path: "/"},
		},
		{
			name: "percent-encoded token",
			raw:  "chunk://to%40ken@host:1/",
			want: URI{Scheme: "chunk", Host: "host", Port: 1, Token: "to@ken", Path: "/"},
		},
		{
			name: "ipv6 host",
			raw:  "chunk://token@[::1]:4242/",
			want: URI{Scheme: "chunk", Host: "::1", Port: 4242, Token: "token", Path: "/"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := ParseURI(testCase.raw)
			if err != nil {
				t.Fatalf("ParseURI(%q): %v", testCase.raw, err)
			}
			if got != testCase.want {
				t.Fatalf("got %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestParseURIRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"unsupported scheme": "http://host:4242/",
		"missing host":       "chunk:///",
		"port out of range":  "chunk://host:70000/",
		"non-numeric port":   "chunk://host:abc/",
	}

	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseURI(raw); !errors.Is(err, ErrConnection) {
				t.Fatalf("got %v, want ErrConnection", err)
			}
		})
	}
}

func TestURIString(t *testing.T) {
	cases := []struct {
		name string
		uri  URI
		want string
	}{
		{
			name: "with token",
			uri:  URI{Scheme: "chunk", Host: "127.0.0.1", Port: 4242, Token: "chunk-token", Path: "/"},
			want: "chunk://chunk-token@127.0.0.1:4242/",
		},
		{
			name: "without token",
			uri:  URI{Scheme: "chunk", Host: "127.0.0.1", Port: 4242, Path: "/"},
			want: "chunk://127.0.0.1:4242/",
		},
		{
			name: "secure flag wins over scheme",
			uri:  URI{Scheme: "chunk", Secure: true, Host: "host", Port: 1, Path: "/"},
			want: "chunks://host:1/",
		},
		{
			name: "ipv6 host is bracketed",
			uri:  URI{Scheme: "chunk", Host: "::1", Port: 4242, Path: "/"},
			want: "chunk://[::1]:4242/",
		},
		{
			name: "empty path defaults",
			uri:  URI{Scheme: "chunk", Host: "host", Port: 4242},
			want: "chunk://host:4242/",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.uri.String(); got != testCase.want {
				t.Fatalf("got %q, want %q", got, testCase.want)
			}
		})
	}
}

func TestURIRoundTrip(t *testing.T) {
	const raw = "chunks://chunk-token@example.test:9000/"
	parsed, err := ParseURI(raw)
	if err != nil {
		t.Fatalf("ParseURI: %v", err)
	}
	if got := parsed.String(); got != raw {
		t.Fatalf("got %q, want %q", got, raw)
	}
}

func TestURIAddress(t *testing.T) {
	uri := URI{Host: "::1", Port: 4242}
	if got := uri.Address(); got != "[::1]:4242" {
		t.Fatalf("got %q, want %q", got, "[::1]:4242")
	}
}
