package cliutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func addRootCA(certPool *x509.CertPool, caCertPath string) error {
	caCertRaw, err := os.ReadFile(caCertPath)
	if err != nil {
		return err
	}
	if ok := certPool.AppendCertsFromPEM(caCertRaw); !ok {
		return fmt.Errorf("could not add root certificates to pool, path %s", caCertPath)
	}

	return nil
}

// NewHTTP3Client new http3 client with udp PacketConn
// insecureSkipVerify default is true
func NewHTTP3Client(pConn net.PacketConn, insecureSkipVerify bool, caCertPath string) (*http.Client, error) {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	if caCertPath != "" {
		if err := addRootCA(pool, caCertPath); err != nil {
			return nil, err
		}
	}

	dial := func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quic.Config) (quic.EarlyConnection, error) {
		remoteAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		return quic.DialEarlyContext(ctx, pConn, remoteAddr, "localhost", tlsCfg, cfg)
	}

	roundTripper := &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			RootCAs:            pool,
			InsecureSkipVerify: insecureSkipVerify,
		},
		QuicConfig: &quic.Config{},
		Dial:       dial,
	}

	return &http.Client{Transport: roundTripper, Timeout: 30 * time.Second}, nil
}
