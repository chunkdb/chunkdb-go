package chunkdb

import (
	"net"
	neturl "net/url"
	"strconv"
	"strings"
)

// URI is a parsed chunk:// or chunks:// endpoint.
type URI struct {
	Scheme string
	Secure bool
	Host   string
	Port   int
	Token  string
	Path   string
}

// ParseURI parses a chunk:// or chunks:// endpoint. The port defaults to
// [DefaultPort] and the token is taken from the userinfo component.
func ParseURI(raw string) (URI, error) {
	parsed, err := neturl.Parse(raw)
	if err != nil {
		return URI{}, connectionErrorf("", err, "invalid chunk URI: %s", raw)
	}

	scheme := parsed.Scheme
	if scheme != "chunk" && scheme != "chunks" {
		return URI{}, connectionErrorf("", nil, "unsupported chunk URI scheme: %s", scheme)
	}

	host := parsed.Hostname()
	if host == "" {
		return URI{}, connectionErrorf("", nil, "chunk URI requires a host")
	}

	port := DefaultPort
	if text := parsed.Port(); text != "" {
		port, err = strconv.Atoi(text)
		if err != nil || port <= 0 || port > 65535 {
			return URI{}, connectionErrorf("", nil, "invalid chunk URI port: %s", text)
		}
	}

	token := ""
	if parsed.User != nil {
		token = parsed.User.Username()
	}

	path := parsed.Path
	if path == "" {
		path = "/"
	}

	return URI{
		Scheme: scheme,
		Secure: scheme == "chunks",
		Host:   host,
		Port:   port,
		Token:  token,
		Path:   path,
	}, nil
}

// String formats the URI. The token, when present, is percent-encoded into the
// userinfo component.
func (u URI) String() string {
	scheme := u.Scheme
	if u.Secure {
		scheme = "chunks"
	}
	if scheme == "" {
		scheme = "chunk"
	}

	auth := ""
	if u.Token != "" {
		auth = neturl.User(u.Token).String() + "@"
	}

	host := u.Host
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}

	path := u.Path
	if path == "" {
		path = "/"
	}

	return scheme + "://" + auth + host + ":" + strconv.Itoa(u.Port) + path
}

// Table reports the table the path names: "terrain" for /terrain, empty for
// / (the server's default table). A path with more than one segment is an
// error.
func (u URI) Table() (string, error) {
	return TableFromPath(u.Path)
}

// TableFromPath is [URI.Table] for a bare path.
func TableFromPath(path string) (string, error) {
	name := strings.TrimPrefix(path, "/")
	if strings.Contains(name, "/") {
		return "", connectionErrorf("", nil, "chunk URI path must name one table: %s", path)
	}
	return name, nil
}

// Address is the host:port dial target.
func (u URI) Address() string {
	return net.JoinHostPort(u.Host, strconv.Itoa(u.Port))
}
