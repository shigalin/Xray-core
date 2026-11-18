package internet_test

import (
	"net"
	"net/http"
	"testing"

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
}
