package encoding

import (
	"context"
	"net"
	"testing"

	"github.com/xtls/xray-core/transport/internet"
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
}
