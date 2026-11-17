package chunkdb

import (
	"net"
	neturl "net/url"
	"strconv"
	"strings"
)

// uriForm is the endpoint form that URI errors suggest.
const uriForm = "chunk://user:password@host:4242/"

// URI is a parsed chunk:// or chunks:// endpoint.
type URI struct {
	Scheme string
	Secure bool
	Host   string
	Port   int
	// User and Password are the login, percent-decoded. An empty User logs
	// in without a user, which only a server started with --auth none
	// accepts.
	User     string
	Password string
	Path     string
}

// ParseURI parses a chunk:// or chunks:// endpoint, for example
// chunk://user:password@host:4242/table. The port defaults to [DefaultPort];
// the user and password are taken from the userinfo component, where %XX
// escapes let them hold ':', '@' or '/'.
func ParseURI(raw string) (URI, error) {
	if !strings.Contains(raw, "://") {
		return URI{}, connectionErrorf("", nil, "invalid chunk URI: %s has no scheme; add chunk:// (or chunks:// for TLS), for example %s", raw, uriForm)
	}
	parsed, err := neturl.Parse(raw)
	if err != nil {
		return URI{}, connectionErrorf("", err, "invalid chunk URI: %s; use the form %s and percent-encode reserved characters in the user and password", raw, uriForm)
	}

	scheme := parsed.Scheme
	if scheme != "chunk" && scheme != "chunks" {
		return URI{}, connectionErrorf("", nil, "unsupported chunk URI scheme: %s; use chunk:// for a plain listener or chunks:// for TLS", scheme)
	}

	host := parsed.Hostname()
	if host == "" {
		return URI{}, connectionErrorf("", nil, "chunk URI requires a host, for example %s", uriForm)
	}

	port := DefaultPort
	if text := parsed.Port(); text != "" {
		port, err = strconv.Atoi(text)
		if err != nil || port <= 0 || port > 65535 {
			return URI{}, connectionErrorf("", nil, "invalid chunk URI port: %s; use a port from 1 to 65535, for example %s", text, uriForm)
		}
	}

	var user, password string
	if parsed.User != nil {
		user = parsed.User.Username()
		password, _ = parsed.User.Password()
		if user == "" {
			return URI{}, connectionErrorf("", nil, "chunk URI userinfo has no user; use %s, or drop the userinfo for a server started with --auth none", uriForm)
		}
	}

	path := parsed.Path
	if path == "" {
		path = "/"
	}

	return URI{
		Scheme:   scheme,
		Secure:   scheme == "chunks",
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Path:     path,
	}, nil
}

// String formats the URI. The user and password, when present, are
// percent-encoded into the userinfo component.
func (u URI) String() string {
	scheme := u.Scheme
	if u.Secure {
		scheme = "chunks"
	}
	if scheme == "" {
		scheme = "chunk"
	}

	auth := ""
	switch {
	case u.Password != "":
		auth = neturl.UserPassword(u.User, u.Password).String() + "@"
	case u.User != "":
		auth = neturl.User(u.User).String() + "@"
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
// / (no table is selected). A path with more than one
// segment is an error.
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
