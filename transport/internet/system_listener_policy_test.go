package internet

import (
	"net"
	"testing"

	"github.com/pires/go-proxyproto"
)

func TestProxyProtocolTrustedPolicyIPv6Zone(t *testing.T) {
	linkLocal := &net.TCPAddr{
		IP:   net.ParseIP("fe80::1"),
		Port: 12345,
		Zone: "en0",
	}

	t.Run("matches IPv6 link-local with zone against CIDR and preserves zone", func(t *testing.T) {
		policy, err := proxyProtocolTrustedPolicy([]string{"fe80::/10"})
		if err != nil {
			t.Fatalf("proxyProtocolTrustedPolicy: %v", err)
		}
		got, err := policy(proxyproto.ConnPolicyOptions{Upstream: linkLocal})
		if err != nil {
			t.Fatalf("policy(%v) error = %v, want nil", linkLocal, err)
		}
		if got != proxyproto.REQUIRE {
			t.Fatalf("policy(%v) = %v, want REQUIRE", linkLocal, got)
		}
		if linkLocal.Zone != "en0" {
			t.Fatalf("Zone = %q, want %q", linkLocal.Zone, "en0")
		}
	})

	t.Run("skips untrusted IPv6 link-local with zone instead of failing", func(t *testing.T) {
		policy, err := proxyProtocolTrustedPolicy([]string{"198.51.100.0/24"})
		if err != nil {
			t.Fatalf("proxyProtocolTrustedPolicy: %v", err)
		}
		got, err := policy(proxyproto.ConnPolicyOptions{Upstream: linkLocal})
		if err != nil {
			t.Fatalf("policy(%v) error = %v, want nil", linkLocal, err)
		}
		if got != proxyproto.SKIP {
			t.Fatalf("policy(%v) = %v, want SKIP", linkLocal, got)
		}
		if linkLocal.Zone != "en0" {
			t.Fatalf("Zone = %q, want %q", linkLocal.Zone, "en0")
		}
	})

	t.Run("matches IPv4-mapped IPv6 peer against IPv4 allowlist", func(t *testing.T) {
		policy, err := proxyProtocolTrustedPolicy([]string{"127.0.0.1"})
		if err != nil {
			t.Fatalf("proxyProtocolTrustedPolicy: %v", err)
		}
		mapped := &net.TCPAddr{IP: net.ParseIP("::ffff:127.0.0.1"), Port: 12345}
		got, err := policy(proxyproto.ConnPolicyOptions{Upstream: mapped})
		if err != nil {
			t.Fatalf("policy(%v) error = %v, want nil", mapped, err)
		}
		if got != proxyproto.REQUIRE {
			t.Fatalf("policy(%v) = %v, want REQUIRE", mapped, got)
		}
	})

	t.Run("rejects zoned IP in allowlist config", func(t *testing.T) {
		if err := ValidateProxyProtocolTrustedIPs([]string{"fe80::1%en0"}); err == nil {
			t.Fatalf("ValidateProxyProtocolTrustedIPs accepted zoned IP, want error")
		}
	})
}
