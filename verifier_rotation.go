package chunkdb

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"
)

// The bounds of a verifier's salt and iteration count the server accepts.
const (
	minVerifierSaltBytes  = 16
	maxVerifierSaltBytes  = 256
	maxVerifierIterations = 1000000
)

// ComputeVerifierWithSalt computes the SCRAM-SHA-256 verifier of password with
// a given salt and iteration count, as [ComputeVerifier] does with a random
// salt. [Client.AddVerifier] needs a verifier with the salt and iterations of
// the user's current one: a login as the user receives them as s= and i= in
// the SCRAM server-first message, and [ParseVerifier] reads them from a
// verifier.
func ComputeVerifierWithSalt(password string, salt []byte, iterations int) (string, error) {
	if password == "" {
		return "", requestErrorf("", "empty password; a user logs in with a password")
	}
	if len(salt) < minVerifierSaltBytes || len(salt) > maxVerifierSaltBytes {
		return "", requestErrorf("", "a verifier salt has %d to %d bytes, got %d", minVerifierSaltBytes, maxVerifierSaltBytes, len(salt))
	}
	if iterations < DefaultVerifierIterations || iterations > maxVerifierIterations {
		return "", requestErrorf("", "a verifier needs %d to %d iterations, got %d", DefaultVerifierIterations, maxVerifierIterations, iterations)
	}
	return formatVerifier(password, salt, iterations)
}

// ParseVerifier reads the salt and iteration count of a verifier
// SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>.
func ParseVerifier(verifier string) (salt []byte, iterations int, err error) {
	bad := func() ([]byte, int, error) {
		return nil, 0, requestErrorf("", "not a SCRAM-SHA-256 verifier")
	}
	rest, ok := strings.CutPrefix(verifier, "SCRAM-SHA-256$")
	if !ok {
		return bad()
	}
	parameters, keys, ok := strings.Cut(rest, "$")
	if !ok || !strings.Contains(keys, ":") {
		return bad()
	}
	iterationsText, saltText, ok := strings.Cut(parameters, ":")
	if !ok {
		return bad()
	}
	iterations, err = strconv.Atoi(iterationsText)
	if err != nil || iterations < DefaultVerifierIterations || iterations > maxVerifierIterations {
		return bad()
	}
	salt, err = base64.StdEncoding.DecodeString(saltText)
	if err != nil || len(salt) < minVerifierSaltBytes || len(salt) > maxVerifierSaltBytes {
		return bad()
	}
	return salt, iterations, nil
}

// AddVerifier gives user a second verifier (ALTER USER ... ADD VERIFIER), so
// both the old and the new password log in while clients move to the new
// one; then run [Client.DropOldVerifier]. The verifier must use the salt and
// iterations of the user's current one ([ComputeVerifierWithSalt]). A user
// may do this for themselves; another user needs MANAGES USERS.
func (c *Client) AddVerifier(ctx context.Context, user, verifier string) error {
	const command = "ALTER USER"
	if err := checkNames(command, "user", user); err != nil {
		return err
	}
	if _, _, err := ParseVerifier(verifier); err != nil {
		return annotate(command, err)
	}
	return c.userStatement(ctx, "ALTER USER "+user+" ADD VERIFIER $1", []byte(verifier))
}

// DropOldVerifier removes the older of user's two verifiers (ALTER USER ...
// DROP OLD VERIFIER), so only the password of the added one logs in. On a
// user with one verifier it changes nothing.
func (c *Client) DropOldVerifier(ctx context.Context, user string) error {
	if err := checkNames("ALTER USER", "user", user); err != nil {
		return err
	}
	return c.userStatement(ctx, "ALTER USER "+user+" DROP OLD VERIFIER", nil)
}

// AddVerifier runs [Client.AddVerifier] on a leased client.
func (p *Pool) AddVerifier(ctx context.Context, user, verifier string) error {
	_, err := withPooledClient(ctx, p, func(ctx context.Context, c *Client) (struct{}, error) {
		return struct{}{}, c.AddVerifier(ctx, user, verifier)
	})
	return err
}

// DropOldVerifier runs [Client.DropOldVerifier] on a leased client.
func (p *Pool) DropOldVerifier(ctx context.Context, user string) error {
	_, err := withPooledClient(ctx, p, func(ctx context.Context, c *Client) (struct{}, error) {
		return struct{}{}, c.DropOldVerifier(ctx, user)
	})
	return err
}
