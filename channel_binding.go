package chunkdb

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"hash"
	"net"
)

// ChannelBinding is how a login made over TLS is bound to its connection
// (SCRAM-SHA-256-PLUS). The proof of a bound login covers the TLS connection
// the client sees, so the login fails when something between the client and
// the server ends TLS, even when the client accepted that party's
// certificate.
type ChannelBinding string

const (
	// ChannelBindingAuto binds every login made over TLS, with tls-exporter
	// on TLS 1.3 and tls-server-end-point on TLS 1.2. A login over plain TCP
	// is not bound. The empty value means the same.
	ChannelBindingAuto ChannelBinding = "auto"
	// ChannelBindingOff never binds a login. Use it when TLS ends at a load
	// balancer or proxy in front of the server.
	ChannelBindingOff ChannelBinding = "off"
	// ChannelBindingRequire binds like [ChannelBindingAuto] and refuses a
	// connection whose login cannot be bound: a client without TLS or without
	// a user fails before it connects.
	ChannelBindingRequire ChannelBinding = "require"
)

const (
	channelBindingTLSExporter       = "tls-exporter"
	channelBindingTLSServerEndPoint = "tls-server-end-point"
	// tlsExporterLabel and tlsExporterBytes are the RFC 9266 exporter.
	tlsExporterLabel = "EXPORTER-Channel-Binding"
	tlsExporterBytes = 32
)

// scramBinding is the channel binding of one login: its type and the data
// the client-final message carries. The zero value is a login that is not
// bound.
type scramBinding struct {
	kind string
	data []byte
}

// header is the GS2 header the client-first message starts with.
func (b scramBinding) header() string {
	if b.kind == "" {
		return "n,,"
	}
	return "p=" + b.kind + ",,"
}

// channelBindingOf returns the binding of a login on netConn under mode. It
// reads the data from the completed TLS handshake, so it runs before HELLO;
// on an error nothing that depends on the password has been sent.
func channelBindingOf(mode ChannelBinding, netConn net.Conn) (scramBinding, error) {
	if mode == ChannelBindingOff {
		return scramBinding{}, nil
	}
	tlsConn, ok := netConn.(*tls.Conn)
	if !ok {
		if mode == ChannelBindingRequire {
			return scramBinding{}, newError(KindTLS, PhaseTLS, "HELLO",
				"ChannelBinding require needs a TLS connection; use a chunks:// URI or Options.TLS", nil)
		}
		return scramBinding{}, nil
	}
	state := tlsConn.ConnectionState()
	if !state.HandshakeComplete {
		return scramBinding{}, newError(KindTLS, PhaseTLS, "HELLO", "channel binding needs a completed TLS handshake", nil)
	}
	if state.Version >= tls.VersionTLS13 {
		data, err := state.ExportKeyingMaterial(tlsExporterLabel, nil, tlsExporterBytes)
		if err != nil {
			return scramBinding{}, newError(KindTLS, PhaseTLS, "HELLO", "tls-exporter channel binding: "+err.Error(), err)
		}
		return scramBinding{kind: channelBindingTLSExporter, data: data}, nil
	}
	if len(state.PeerCertificates) == 0 {
		return scramBinding{}, newError(KindTLS, PhaseTLS, "HELLO",
			"tls-server-end-point channel binding needs the server certificate, and the server sent none", nil)
	}
	certificate := state.PeerCertificates[0]
	newHash := serverEndPointHash(certificate.SignatureAlgorithm)
	if newHash == nil {
		return scramBinding{}, newError(KindTLS, PhaseTLS, "HELLO",
			"tls-server-end-point channel binding is not defined for a server certificate signed with "+
				certificate.SignatureAlgorithm.String()+"; connect over TLS 1.3, or set ChannelBinding to off", nil)
	}
	digest := newHash()
	digest.Write(certificate.Raw)
	return scramBinding{kind: channelBindingTLSServerEndPoint, data: digest.Sum(nil)}, nil
}

// serverEndPointHash is the hash of tls-server-end-point (RFC 5929 section
// 4.1) for a certificate signed with algorithm: the hash of the signature
// algorithm, SHA-256 in place of MD5 and SHA-1. It is nil for an algorithm
// without a single hash, such as Ed25519.
func serverEndPointHash(algorithm x509.SignatureAlgorithm) func() hash.Hash {
	switch algorithm {
	case x509.MD5WithRSA, x509.SHA1WithRSA, x509.DSAWithSHA1, x509.ECDSAWithSHA1,
		x509.SHA256WithRSA, x509.SHA256WithRSAPSS, x509.DSAWithSHA256, x509.ECDSAWithSHA256:
		return sha256.New
	case x509.SHA384WithRSA, x509.SHA384WithRSAPSS, x509.ECDSAWithSHA384:
		return sha512.New384
	case x509.SHA512WithRSA, x509.SHA512WithRSAPSS, x509.ECDSAWithSHA512:
		return sha512.New
	default:
		return nil
	}
}
