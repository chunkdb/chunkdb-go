package chunkdb

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// DefaultVerifierIterations is the PBKDF2 iteration count of the verifiers
// this package computes when none is configured. It is also the least a
// server accepts.
const DefaultVerifierIterations = 4096

const (
	// verifierSaltBytes is the size of the random salt of a new verifier.
	verifierSaltBytes = 16
	// scramNonceBytes is the random part of the client nonce.
	scramNonceBytes = 18
	// scramChannelBinding is base64("n,,"): no channel binding.
	scramChannelBinding = "c=biws"
)

// ComputeVerifier computes the SCRAM-SHA-256 verifier of password, with a new
// random 16-byte salt and iterations PBKDF2 iterations (0 means
// [DefaultVerifierIterations]; fewer are refused). The result has the form
// SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey> with base64 parts,
// which CREATE USER ... VERIFIER and ALTER USER ... VERIFIER take as a
// parameter. The password itself cannot be recovered from it.
func ComputeVerifier(password string, iterations int) (string, error) {
	if iterations == 0 {
		iterations = DefaultVerifierIterations
	}
	if iterations < DefaultVerifierIterations {
		return "", requestErrorf("", "a verifier needs at least %d iterations, got %d", DefaultVerifierIterations, iterations)
	}
	salt := make([]byte, verifierSaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return "", requestErrorf("", "random salt: %v", err)
	}
	return formatVerifier(password, salt, iterations)
}

// formatVerifier computes the verifier of password with a given salt.
func formatVerifier(password string, salt []byte, iterations int) (string, error) {
	keys, err := deriveScramKeys(password, salt, iterations)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iterations, base64.StdEncoding.EncodeToString(salt),
		base64.StdEncoding.EncodeToString(keys.storedKey), base64.StdEncoding.EncodeToString(keys.serverKey)), nil
}

type scramKeys struct {
	clientKey []byte
	storedKey []byte
	serverKey []byte
}

// deriveScramKeys derives the RFC 5802 keys of password: SaltedPassword is
// PBKDF2-HMAC-SHA-256, ClientKey and ServerKey are HMACs of it, StoredKey is
// SHA-256(ClientKey).
func deriveScramKeys(password string, salt []byte, iterations int) (scramKeys, error) {
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return scramKeys{}, requestErrorf("", "PBKDF2: %v", err)
	}
	clientKey := hmacSHA256(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	return scramKeys{clientKey: clientKey, storedKey: stored[:], serverKey: hmacSHA256(salted, "Server Key")}, nil
}

func hmacSHA256(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(message))
	return mac.Sum(nil)
}

// newScramNonce returns a random client nonce: printable and without commas.
func newScramNonce() (string, error) {
	raw := make([]byte, scramNonceBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", connectionErrorf("HELLO", err, "random nonce: %s", err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

// scramLogin is the client side of one SCRAM-SHA-256 login (RFC 5802, RFC
// 7677) without channel binding.
type scramLogin struct {
	nonce     string
	firstBare string
}

func newScramLogin(user, nonce string) scramLogin {
	return scramLogin{nonce: nonce, firstBare: "n=" + user + ",r=" + nonce}
}

// clientFirst is the client-first message HELLO 3 USER carries.
func (s scramLogin) clientFirst() string {
	return "n,," + s.firstBare
}

// clientFinal answers the server-first message: it returns the client-final
// message AUTH carries and the server signature (v=...) the server must reply
// with to prove it holds the user's verifier.
func (s scramLogin) clientFinal(password, serverFirst string) (message, serverSignature string, err error) {
	malformed := func(why string) error {
		return protocolErrorf("HELLO", "malformed SCRAM server-first message %q: %s", serverFirst, why)
	}
	// r=<nonce>,s=<salt>,i=<iterations>[,<extensions>]
	parts := strings.Split(serverFirst, ",")
	if len(parts) < 3 {
		return "", "", malformed("expected r=, s= and i=")
	}
	nonce, okNonce := strings.CutPrefix(parts[0], "r=")
	saltText, okSalt := strings.CutPrefix(parts[1], "s=")
	iterationsText, okIterations := strings.CutPrefix(parts[2], "i=")
	if !okNonce || !okSalt || !okIterations {
		return "", "", malformed("expected r=, s= and i=")
	}
	if !strings.HasPrefix(nonce, s.nonce) || len(nonce) == len(s.nonce) {
		return "", "", malformed("the nonce does not continue the client nonce")
	}
	salt, err := base64.StdEncoding.DecodeString(saltText)
	if err != nil || len(salt) == 0 {
		return "", "", malformed("the salt is not base64")
	}
	iterations, err := strconv.Atoi(iterationsText)
	if err != nil || iterations <= 0 {
		return "", "", malformed("the iteration count is not a positive integer")
	}
	if iterations < DefaultVerifierIterations {
		return "", "", malformed(fmt.Sprintf("%d iterations, fewer than the %d SCRAM-SHA-256 needs", iterations, DefaultVerifierIterations))
	}

	keys, err := deriveScramKeys(password, salt, iterations)
	if err != nil {
		return "", "", annotate("HELLO", err)
	}
	withoutProof := scramChannelBinding + ",r=" + nonce
	authMessage := s.firstBare + "," + serverFirst + "," + withoutProof
	proof := hmacSHA256(keys.storedKey, authMessage)
	for i := range proof {
		proof[i] ^= keys.clientKey[i]
	}
	message = withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	serverSignature = "v=" + base64.StdEncoding.EncodeToString(hmacSHA256(keys.serverKey, authMessage))
	return message, serverSignature, nil
}
