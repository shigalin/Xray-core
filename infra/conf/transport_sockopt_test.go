package conf_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/infra/conf"
	_ "github.com/xtls/xray-core/main/distro/all"
)

// preflightInbound runs the same JSON build and core.New path as "xray run -test".
func preflightInbound(t *testing.T, raw string) error {
	t.Helper()
	var ic conf.InboundDetourConfig
	if err := json.Unmarshal([]byte(raw), &ic); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	pb, err := (&conf.Config{InboundConfigs: []conf.InboundDetourConfig{ic}}).Build()
	if err != nil {
		return err
	}
	inst, err := core.New(pb)
	if inst != nil {
		_ = inst.Close()
	}
	return err
}

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

	t.Run("inbound rejects combining proxy-protocol finalmask with sockopt or legacy acceptProxyProtocol", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			raw  string
		}{
			{
				name: "sockopt proxyProtocolTrustedIPs",
				raw:  `{"protocol":"socks","port":1080,"streamSettings":{"sockopt":{"proxyProtocolTrustedIPs":["127.0.0.1"]},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`,
			},
			{
				name: "sockopt acceptProxyProtocol",
				raw:  `{"protocol":"socks","port":1080,"streamSettings":{"sockopt":{"acceptProxyProtocol":true},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`,
			},
			{
				name: "legacy tcpSettings acceptProxyProtocol",
				raw:  `{"protocol":"socks","port":1080,"streamSettings":{"network":"tcp","tcpSettings":{"acceptProxyProtocol":true},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`,
			},
			{
				name: "legacy rawSettings acceptProxyProtocol",
				raw:  `{"protocol":"socks","port":1080,"streamSettings":{"network":"raw","rawSettings":{"acceptProxyProtocol":true},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`,
			},
			{
				name: "legacy wsSettings acceptProxyProtocol",
				raw:  `{"protocol":"socks","port":1080,"streamSettings":{"network":"ws","wsSettings":{"acceptProxyProtocol":true},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`,
			},
			{
				name: "legacy httpupgradeSettings acceptProxyProtocol",
				raw:  `{"protocol":"socks","port":1080,"streamSettings":{"network":"httpupgrade","httpupgradeSettings":{"acceptProxyProtocol":true},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := preflightInbound(t, tc.raw)
				if err == nil {
					t.Fatalf("preflight(%s) succeeded, want error", tc.name)
				}
				if !strings.Contains(err.Error(), "proxy-protocol") {
					t.Fatalf("preflight(%s) error = %v, want proxy-protocol conflict error", tc.name, err)
				}
			})
		}
	})

	t.Run("unix inbound rejects proxyProtocolTrustedIPs during preflight", func(t *testing.T) {
		raw := `{"protocol":"dokodemo-door","listen":"@xray-proxy-protocol-test.sock","settings":{"address":"127.0.0.1","port":80,"network":"tcp"},"streamSettings":{"sockopt":{"proxyProtocolTrustedIPs":["127.0.0.1"]}}}`
		err := preflightInbound(t, raw)
		if err == nil || !strings.Contains(err.Error(), "only supported on TCP listeners") {
			t.Fatalf("preflight(unix) error = %v, want TCP-only error", err)
		}
	})

	t.Run("pure UDP tunnel inbound ignores unused TCP proxy-protocol conflict", func(t *testing.T) {
		raw := `{"protocol":"tunnel","port":1080,"settings":{"address":"127.0.0.1","port":53,"network":"udp"},"streamSettings":{"sockopt":{"acceptProxyProtocol":true},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`
		if err := preflightInbound(t, raw); err != nil {
			t.Fatalf("preflight(udp tunnel) unexpected error: %v", err)
		}
	})

	t.Run("freedom outbound still allows proxy-protocol finalmask or legacy proxyProtocol alongside sockopt acceptProxyProtocol", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			raw  string
		}{
			{
				name: "explicit finalmask proxy-protocol",
				raw:  `{"protocol":"freedom","streamSettings":{"sockopt":{"acceptProxyProtocol":true},"finalmask":{"tcp":[{"type":"proxy-protocol"}]}}}`,
			},
			{
				name: "legacy freedom settings proxyProtocol",
				raw:  `{"protocol":"freedom","settings":{"proxyProtocol":2},"streamSettings":{"sockopt":{"acceptProxyProtocol":true}}}`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var oc conf.OutboundDetourConfig
				if err := json.Unmarshal([]byte(tc.raw), &oc); err != nil {
					t.Fatalf("json.Unmarshal: %v", err)
				}
				if _, err := oc.Build(); err != nil {
					t.Fatalf("OutboundDetourConfig.Build(%s) unexpected error: %v", tc.name, err)
				}
			})
		}
	})
}
