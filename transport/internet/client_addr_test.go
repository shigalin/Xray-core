package internet_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/pires/go-proxyproto"
	"github.com/xtls/xray-core/transport/internet"
)

func TestCloudflareClientAddr(t *testing.T) {
	edge4 := &net.TCPAddr{IP: net.ParseIP("104.22.233.68"), Port: 12345}
	edge6 := &net.TCPAddr{IP: net.ParseIP("2606:4700::1"), Port: 12345}
	direct := &net.TCPAddr{IP: net.ParseIP("192.0.2.2"), Port: 12345}
	for _, tt := range []struct {
		name        string
		peer        net.Addr
		visitor     []string
		visitorIPv6 string
		proxy, off  bool
		want        string
	}{
		{name: "IPv4", peer: edge4, visitor: []string{"203.0.113.5"}, want: "203.0.113.5:0"},
		{name: "IPv6", peer: edge6, visitor: []string{"2001:db8::5"}, want: "[2001:db8::5]:0"},
		{name: "pseudo IPv4", peer: edge4, visitor: []string{"240.0.0.5"}, visitorIPv6: "2001:db8::5", want: "[2001:db8::5]:0"},
		{name: "missing pseudo IPv4 original", peer: edge4, visitor: []string{"240.0.0.5"}},
		{name: "missing header", peer: edge4},
		{name: "invalid header", peer: edge4, visitor: []string{"invalid"}},
		{name: "duplicate headers", peer: edge4, visitor: []string{"203.0.113.5", "203.0.113.5"}},
		{name: "IP list", peer: edge4, visitor: []string{"203.0.113.5, 203.0.113.6"}},
		{name: "direct spoofed headers", peer: direct, visitor: []string{"203.0.113.5"}},
		{name: "PROXY protocol", peer: edge4, visitor: []string{"203.0.113.5"}, proxy: true},
		{name: "disabled", peer: edge4, visitor: []string{"203.0.113.5"}, off: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			header["Cf-Connecting-Ip"] = tt.visitor
			header.Set("Cf-Connecting-Ipv6", tt.visitorIPv6)
			header.Set("X-Forwarded-For", "198.51.100.10, 203.0.113.5")
			cfg := &internet.SocketConfig{CloudflareRealIp: !tt.off, AcceptProxyProtocol: tt.proxy}
			got := cfg.ClientAddr(header.Values, tt.peer)
			want := tt.want
			if want == "" {
				want = tt.peer.String()
			}
			if got.String() != want {
				t.Fatalf("client address = %v, want %s", got, want)
			}
		})
	}

	t.Run("shared listener with proxyProtocolTrustedIPs still restores Cloudflare direct peers", func(t *testing.T) {
		header := http.Header{}
		header.Set("CF-Connecting-IP", "203.0.113.5")
		for _, acceptProxy := range []bool{false, true} {
			cfg := &internet.SocketConfig{
				CloudflareRealIp:        true,
				AcceptProxyProtocol:     acceptProxy,
				ProxyProtocolTrustedIps: []string{"198.51.100.10"},
			}
			if got := cfg.ClientAddr(header.Values, edge4); got.String() != "203.0.113.5:0" {
				t.Fatalf("acceptProxy=%v cloudflare peer client address = %v, want 203.0.113.5:0", acceptProxy, got)
			}
			if got := cfg.ClientAddrWithProxyProtocol(header.Values, edge4, false); got.String() != "203.0.113.5:0" {
				t.Fatalf("acceptProxy=%v direct cloudflare peer = %v, want 203.0.113.5:0", acceptProxy, got)
			}
			if got := cfg.ClientAddrWithProxyProtocol(header.Values, edge4, true); got.String() != edge4.String() {
				t.Fatalf("acceptProxy=%v proxied peer = %v, want %s", acceptProxy, got, edge4)
			}
			if got := cfg.ClientAddrWithProxyProtocol(header.Values, direct, false); got.String() != direct.String() {
				t.Fatalf("acceptProxy=%v non-cloudflare peer = %v, want %s", acceptProxy, got, direct)
			}
		}
	})

	t.Run("ConnUsedProxyProtocol and context propagation", func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		proxyConn := proxyproto.NewConn(server, proxyproto.WithPolicy(proxyproto.REQUIRE))
		go func() {
			_, _ = io.WriteString(client, "PROXY TCP4 104.22.233.68 127.0.0.1 12345 443\r\nok")
			_ = client.Close()
		}()
		if _, err := io.ReadAll(proxyConn); err != nil {
			t.Fatalf("ReadAll(proxyConn): %v", err)
		}
		if !internet.ConnUsedProxyProtocol(proxyConn) {
			t.Fatalf("ConnUsedProxyProtocol(proxyConn) = false, want true")
		}
		ctx := internet.ContextWithProxyProtocolConn(context.Background(), proxyConn)
		if !internet.ProxyProtocolUsedFromContext(ctx) {
			t.Fatalf("ProxyProtocolUsedFromContext(proxyConn) = false, want true")
		}

		c2, s2 := net.Pipe()
		defer c2.Close()
		defer s2.Close()
		if internet.ConnUsedProxyProtocol(s2) {
			t.Fatalf("ConnUsedProxyProtocol(plain) = true, want false")
		}
		if internet.ProxyProtocolUsedFromContext(internet.ContextWithProxyProtocolConn(context.Background(), s2)) {
			t.Fatalf("ProxyProtocolUsedFromContext(plain) = true, want false")
		}
	})
}
