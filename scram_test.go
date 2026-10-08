package chunkdb

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The SCRAM-SHA-256 example of RFC 7677, section 3.
const (
	rfcUser        = "user"
	rfcPassword    = "pencil"
	rfcClientNonce = "rOprNGfwEbeRWgbNEkqO"
	rfcServerFirst = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	rfcClientFinal = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
	rfcSignature   = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
)

func TestScramRFC7677(t *testing.T) {
	login := newScramLogin(rfcUser, rfcClientNonce)
	if got := login.clientFirst(); got != "n,,n=user,r=rOprNGfwEbeRWgbNEkqO" {
		t.Fatalf("got client-first %q", got)
	}
	final, signature, err := login.clientFinal(rfcPassword, rfcServerFirst)
	if err != nil {
		t.Fatalf("clientFinal: %v", err)
	}
	if final != rfcClientFinal {
		t.Fatalf("got client-final %q, want %q", final, rfcClientFinal)
	}
	if signature != rfcSignature {
		t.Fatalf("got server signature %q, want %q", signature, rfcSignature)
	}

	// Another password gives another proof.
	if other, _, err := login.clientFinal("pencil2", rfcServerFirst); err != nil || other == rfcClientFinal {
		t.Fatalf("got %q, %v", other, err)
	}
}

func TestScramRejectsBadServerFirst(t *testing.T) {
	login := newScramLogin(rfcUser, rfcClientNonce)
	for name, serverFirst := range map[string]string{
		"empty":            "",
		"missing parts":    "r=rOprNGfwEbeRWgbNEkqOabc,s=W22ZaJ0SNY7soEsUEjb6gQ==",
		"reordered":        "s=W22ZaJ0SNY7soEsUEjb6gQ==,r=rOprNGfwEbeRWgbNEkqOabc,i=4096",
		"foreign nonce":    "r=abc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
		"unchanged nonce":  "r=rOprNGfwEbeRWgbNEkqO,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
		"bad salt":         "r=rOprNGfwEbeRWgbNEkqOabc,s=***,i=4096",
		"empty salt":       "r=rOprNGfwEbeRWgbNEkqOabc,s=,i=4096",
		"bad iterations":   "r=rOprNGfwEbeRWgbNEkqOabc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=x",
		"too few rounds":   "r=rOprNGfwEbeRWgbNEkqOabc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4095",
		"negative rounds":  "r=rOprNGfwEbeRWgbNEkqOabc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=-4096",
		"mandatory extras": "m=ext,r=rOprNGfwEbeRWgbNEkqOabc,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := login.clientFinal(rfcPassword, serverFirst); !errors.Is(err, ErrProtocol) {
				t.Fatalf("got %v, want ErrProtocol", err)
			}
		})
	}
}

func TestScramNonce(t *testing.T) {
	first, err := newScramNonce()
	if err != nil {
		t.Fatalf("newScramNonce: %v", err)
	}
	second, _ := newScramNonce()
	raw, err := base64.StdEncoding.DecodeString(first)
	if err != nil || len(raw) < 18 || first == second || strings.Contains(first, ",") {
		t.Fatalf("got nonces %q and %q", first, second)
	}
}

var verifierPattern = regexp.MustCompile(`^SCRAM-SHA-256\$(\d+):([A-Za-z0-9+/=]+)\$([A-Za-z0-9+/=]+):([A-Za-z0-9+/=]+)$`)

// parseVerifier splits a verifier into its iterations, salt, StoredKey and
// ServerKey.
func parseVerifier(t *testing.T, verifier string) (int, []byte, []byte, []byte) {
	t.Helper()
	parts := verifierPattern.FindStringSubmatch(verifier)
	if parts == nil {
		t.Fatalf("verifier %q is not SCRAM-SHA-256$<iterations>:<salt>$<StoredKey>:<ServerKey>", verifier)
	}
	iterations, _ := strconv.Atoi(parts[1])
	decoded := make([][]byte, 3)
	for i, part := range parts[2:] {
		var err error
		if decoded[i], err = base64.StdEncoding.DecodeString(part); err != nil {
			t.Fatalf("verifier part %q: %v", part, err)
		}
	}
	return iterations, decoded[0], decoded[1], decoded[2]
}

func TestComputeVerifier(t *testing.T) {
	verifier, err := ComputeVerifier("pencil", 0)
	if err != nil {
		t.Fatalf("ComputeVerifier: %v", err)
	}
	iterations, salt, storedKey, serverKey := parseVerifier(t, verifier)
	if iterations != DefaultVerifierIterations || len(salt) != 16 || len(storedKey) != 32 || len(serverKey) != 32 {
		t.Fatalf("got %d iterations, a %d-byte salt and keys of %d and %d bytes", iterations, len(salt), len(storedKey), len(serverKey))
	}
	again, _ := ComputeVerifier("pencil", 0)
	if again == verifier {
		t.Fatal("two verifiers share a salt")
	}

	if verifier, err := ComputeVerifier("pencil", 10000); err != nil || !strings.HasPrefix(verifier, "SCRAM-SHA-256$10000:") {
		t.Fatalf("got %q, %v", verifier, err)
	}
	if _, err := ComputeVerifier("pencil", 4095); !errors.Is(err, ErrProtocol) {
		t.Fatalf("got %v, want a refusal of fewer than 4096 iterations", err)
	}
}

// A verifier holds what the server needs to check the RFC 7677 login: the
// StoredKey recovers the proof's ClientKey and the ServerKey signs.
func TestVerifierChecksRFC7677Login(t *testing.T) {
	salt, _ := base64.StdEncoding.DecodeString("W22ZaJ0SNY7soEsUEjb6gQ==")
	verifier, err := formatVerifier(rfcPassword, salt, 4096)
	if err != nil {
		t.Fatalf("formatVerifier: %v", err)
	}
	iterations, gotSalt, storedKey, serverKey := parseVerifier(t, verifier)
	if iterations != 4096 || !bytes.Equal(gotSalt, salt) {
		t.Fatalf("got %d iterations and salt %x", iterations, gotSalt)
	}

	withoutProof, proofText, _ := strings.Cut(rfcClientFinal, ",p=")
	proof, _ := base64.StdEncoding.DecodeString(proofText)
	authMessage := "n=user,r=" + rfcClientNonce + "," + rfcServerFirst + "," + withoutProof
	clientKey := hmacSHA256(storedKey, authMessage)
	for i := range clientKey {
		clientKey[i] ^= proof[i]
	}
	if got := sha256.Sum256(clientKey); !bytes.Equal(got[:], storedKey) {
		t.Fatal("the StoredKey does not check the RFC 7677 proof")
	}
	if got := "v=" + base64.StdEncoding.EncodeToString(hmacSHA256(serverKey, authMessage)); got != rfcSignature {
		t.Fatalf("the ServerKey signs %q, want %q", got, rfcSignature)
	}
}
