package encoding

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"

	"github.com/pires/go-proxyproto"
	"github.com/xtls/xray-core/transport/internet"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func TestRemoteAddrFromContext(t *testing.T) {
	tests := []struct {
		name                  string
		metadata              metadata.MD
		socketSettings        *internet.SocketConfig
		peerIP                string
		expectedRemoteAddress string
	}{
		{
			name:                  "trust X-Forwarded-For when configured",
			metadata:              metadata.Pairs("X-Forwarded-For", "2.2.2.2, 3.3.3.3"),
			socketSettings:        &internet.SocketConfig{TrustedXForwardedFor: []string{"X-Forwarded-For"}},
			expectedRemoteAddress: "2.2.2.2:0",
		},
		{
			name:                  "trust X-Forwarded-For with trusted marker",
			metadata:              metadata.Pairs("X-Forwarded-For", "4.4.4.4", "X-Trusted-CDN", "1"),
			socketSettings:        &internet.SocketConfig{TrustedXForwardedFor: []string{"X-Trusted-CDN"}},
			expectedRemoteAddress: "4.4.4.4:0",
		},
		{
			name:                  "ignore X-Forwarded-For without trusted marker",
			metadata:              metadata.Pairs("X-Forwarded-For", "5.5.5.5"),
			socketSettings:        &internet.SocketConfig{TrustedXForwardedFor: []string{"X-Trusted-CDN"}},
			expectedRemoteAddress: "127.0.0.1:12345",
		},
		{
			name:                  "restore a Cloudflare client address from gRPC metadata",
			metadata:              metadata.Pairs("CF-Connecting-IP", "2001:db8::42", "X-Forwarded-For", "198.51.100.1"),
			socketSettings:        &internet.SocketConfig{CloudflareRealIp: true},
			peerIP:                "172.69.194.176",
			expectedRemoteAddress: "[2001:db8::42]:0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			peerIP := test.peerIP
			if peerIP == "" {
				peerIP = "127.0.0.1"
			}
			ctx := peer.NewContext(metadata.NewIncomingContext(context.Background(), test.metadata), &peer.Peer{
				Addr: &net.TCPAddr{
					IP:   net.ParseIP(peerIP),
					Port: 12345,
				},
			})
			remoteAddr := remoteAddrFromContext(ctx, test.socketSettings)
			if remoteAddr.String() != test.expectedRemoteAddress {
				t.Fatalf("unexpected remote address: %s", remoteAddr.String())
			}
		})
	}

	t.Run("shared listener distinguishes direct Cloudflare peer from PROXY peer", func(t *testing.T) {
		creds := NewProxyProtocolCredentials(insecure.NewCredentials())
		cfg := &internet.SocketConfig{
			CloudflareRealIp:        true,
			AcceptProxyProtocol:     true,
			ProxyProtocolTrustedIps: []string{"198.51.100.10"},
		}
		md := metadata.Pairs("CF-Connecting-IP", "2001:db8::42")
		edgeAddr := &net.TCPAddr{IP: net.ParseIP("172.69.194.176"), Port: 12345}

		// Direct connection (no PROXY header).
		c1, s1 := net.Pipe()
		defer c1.Close()
		defer s1.Close()
		_, directAuth, err := creds.ServerHandshake(s1)
		if err != nil {
			t.Fatalf("ServerHandshake(direct): %v", err)
		}
		directCtx := peer.NewContext(metadata.NewIncomingContext(context.Background(), md), &peer.Peer{
			Addr:     edgeAddr,
			AuthInfo: directAuth,
		})
		if got := remoteAddrFromContext(directCtx, cfg); got.String() != "[2001:db8::42]:0" {
			t.Fatalf("direct Cloudflare peer = %s, want [2001:db8::42]:0", got)
		}

		// Connection with parsed PROXY protocol header.
		c2, s2 := net.Pipe()
		defer c2.Close()
		defer s2.Close()
		proxyConn := proxyproto.NewConn(s2, proxyproto.WithPolicy(proxyproto.REQUIRE))
		go func() {
			_, _ = io.WriteString(c2, "PROXY TCP4 172.69.194.176 127.0.0.1 12345 443\r\nok")
			_ = c2.Close()
		}()
		_, proxyAuth, err := creds.ServerHandshake(proxyConn)
		if err != nil {
			t.Fatalf("ServerHandshake(proxy): %v", err)
		}
		if _, err := io.ReadAll(proxyConn); err != nil {
			t.Fatalf("ReadAll(proxyConn): %v", err)
		}
		proxyCtx := peer.NewContext(metadata.NewIncomingContext(context.Background(), md), &peer.Peer{
			Addr:     edgeAddr,
			AuthInfo: proxyAuth,
		})
		if got := remoteAddrFromContext(proxyCtx, cfg); got.String() != edgeAddr.String() {
			t.Fatalf("proxied peer = %s, want %s", got, edgeAddr)
		}
	})

	t.Run("proxyProtocolAuthInfo preserves TLSInfo Channelz security value and CommonAuthInfo security level", func(t *testing.T) {
		tlsInfo := credentials.TLSInfo{
			State: tls.ConnectionState{
				Version:     tls.VersionTLS13,
				CipherSuite: tls.TLS_AES_128_GCM_SHA256,
			},
			CommonAuthInfo: credentials.CommonAuthInfo{
				SecurityLevel: credentials.PrivacyAndIntegrity,
			},
		}
		wrapped := proxyProtocolAuthInfo{AuthInfo: tlsInfo}
		csi, ok := any(wrapped).(credentials.ChannelzSecurityInfo)
		if !ok {
			t.Fatalf("wrapped AuthInfo does not implement ChannelzSecurityInfo")
		}
		secVal, ok := csi.GetSecurityValue().(*credentials.TLSChannelzSecurityValue)
		if !ok || secVal == nil {
			t.Fatalf("GetSecurityValue() = %T, want non-nil *TLSChannelzSecurityValue", csi.GetSecurityValue())
		}
		if secVal.StandardName != tls.CipherSuiteName(tls.TLS_AES_128_GCM_SHA256) {
			t.Fatalf("StandardName = %q, want %q", secVal.StandardName, tls.CipherSuiteName(tls.TLS_AES_128_GCM_SHA256))
		}
		if err := credentials.CheckSecurityLevel(wrapped, credentials.PrivacyAndIntegrity); err != nil {
			t.Fatalf("CheckSecurityLevel(TLS) error = %v, want nil", err)
		}

		c1, s1 := net.Pipe()
		defer c1.Close()
		defer s1.Close()
		_, insecureAuth, err := NewProxyProtocolCredentials(insecure.NewCredentials()).ServerHandshake(s1)
		if err != nil {
			t.Fatalf("ServerHandshake(insecure): %v", err)
		}
		if err := credentials.CheckSecurityLevel(insecureAuth, credentials.PrivacyAndIntegrity); err == nil {
			t.Fatalf("CheckSecurityLevel(insecure, PrivacyAndIntegrity) = nil, want error")
		}
		if err := credentials.CheckSecurityLevel(insecureAuth, credentials.NoSecurity); err != nil {
			t.Fatalf("CheckSecurityLevel(insecure, NoSecurity) = %v, want nil", err)
		}
	})
}
