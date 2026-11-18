package internet

import (
	"net/netip"
	"strings"

	"github.com/xtls/xray-core/common/net"
	httpProto "github.com/xtls/xray-core/common/protocol/http"
)

// Cloudflare's published origin-facing ranges: https://www.cloudflare.com/ips/
var cloudflarePrefixes = [...]netip.Prefix{
	netip.MustParsePrefix("103.21.244.0/22"),
	netip.MustParsePrefix("103.22.200.0/22"),
	netip.MustParsePrefix("103.31.4.0/22"),
	netip.MustParsePrefix("104.16.0.0/13"),
	netip.MustParsePrefix("104.24.0.0/14"),
	netip.MustParsePrefix("108.162.192.0/18"),
	netip.MustParsePrefix("131.0.72.0/22"),
	netip.MustParsePrefix("141.101.64.0/18"),
	netip.MustParsePrefix("162.158.0.0/15"),
	netip.MustParsePrefix("172.64.0.0/13"),
	netip.MustParsePrefix("173.245.48.0/20"),
	netip.MustParsePrefix("188.114.96.0/20"),
	netip.MustParsePrefix("190.93.240.0/20"),
	netip.MustParsePrefix("197.234.240.0/22"),
	netip.MustParsePrefix("198.41.128.0/17"),
	netip.MustParsePrefix("2400:cb00::/32"),
	netip.MustParsePrefix("2606:4700::/32"),
	netip.MustParsePrefix("2803:f800::/32"),
	netip.MustParsePrefix("2405:b500::/32"),
	netip.MustParsePrefix("2405:8100::/32"),
	netip.MustParsePrefix("2a06:98c0::/29"),
	netip.MustParsePrefix("2c0f:f248::/32"),
}

var cloudflarePseudoIPv4 = netip.MustParsePrefix("240.0.0.0/4")

// ClientAddr resolves the address exposed to the inbound protocol.
func (c *SocketConfig) ClientAddr(values func(string) []string, peer net.Addr) net.Addr {
	// With PROXY protocol, the listener has already replaced the socket peer.
	if c.GetCloudflareRealIp() && !c.GetAcceptProxyProtocol() && isCloudflarePeer(peer) {
		ip := singleIP(values("CF-Connecting-IP"))
		if cloudflarePseudoIPv4.Contains(ip) {
			ip = singleIP(values("CF-Connecting-IPv6"))
			if !ip.Is6() {
				return peer
			}
		}
		if ip.IsValid() {
			return &net.TCPAddr{IP: ip.AsSlice()}
		}
		return peer
	}
	return httpProto.ApplyTrustedXForwardedForValues(values, c.GetTrustedXForwardedFor(), peer)
}

func isCloudflarePeer(peer net.Addr) bool {
	var ip netip.Addr
	switch addr := peer.(type) {
	case *net.TCPAddr:
		ip = addr.AddrPort().Addr()
	case *net.UDPAddr:
		ip = addr.AddrPort().Addr()
	}
	ip = ip.Unmap()
	for _, prefix := range cloudflarePrefixes {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

func singleIP(values []string) netip.Addr {
	if len(values) != 1 {
		return netip.Addr{}
	}
	ip, err := netip.ParseAddr(strings.TrimSpace(values[0]))
	if err != nil || ip.Zone() != "" {
		return netip.Addr{}
	}
	return ip.Unmap()
}
