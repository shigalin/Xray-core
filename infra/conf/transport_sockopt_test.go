package conf_test

import (
	"testing"

	"github.com/xtls/xray-core/infra/conf"
)

func TestSocketConfigProxyProtocolTrustedIPs(t *testing.T) {
	t.Run("valid IPs and CIDRs do not force AcceptProxyProtocol", func(t *testing.T) {
		cfg := &conf.SocketConfig{
			CloudflareRealIP:        true,
			ProxyProtocolTrustedIPs: []string{"127.0.0.1", "203.0.113.0/24", "2001:db8::/32"},
		}
		built, err := cfg.Build()
		if err != nil {
			t.Fatalf("Build() unexpected error: %v", err)
		}
		if built.GetAcceptProxyProtocol() {
			t.Fatalf("AcceptProxyProtocol = true, want false")
		}
		if !built.HasProxyProtocol() {
			t.Fatalf("HasProxyProtocol() = false, want true")
		}
		if !built.GetCloudflareRealIp() {
			t.Fatalf("CloudflareRealIp = false, want true")
		}
		if got := built.GetProxyProtocolTrustedIps(); len(got) != 3 {
			t.Fatalf("ProxyProtocolTrustedIps len = %d, want 3", len(got))
		}
	})

	t.Run("explicit AcceptProxyProtocol is preserved alongside trusted IPs", func(t *testing.T) {
		cfg := &conf.SocketConfig{
			AcceptProxyProtocol:     true,
			ProxyProtocolTrustedIPs: []string{"10.0.0.1"},
		}
		built, err := cfg.Build()
		if err != nil {
			t.Fatalf("Build() unexpected error: %v", err)
		}
		if !built.GetAcceptProxyProtocol() {
			t.Fatalf("AcceptProxyProtocol = false, want true")
		}
	})

	for _, bad := range [][]string{
		{"not-an-ip"},
		{""},
		{"10.0.0.1/99"},
		{"127.0.0.1", "invalid"},
	} {
		cfg := &conf.SocketConfig{ProxyProtocolTrustedIPs: bad}
		if _, err := cfg.Build(); err == nil {
			t.Fatalf("Build(%v) succeeded, want error", bad)
		}
	}
}
