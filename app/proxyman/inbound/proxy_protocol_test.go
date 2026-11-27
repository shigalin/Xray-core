package inbound_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	"github.com/xtls/xray-core/app/proxyman/inbound"
	_ "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	fstats "github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/proxy/dokodemo"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask/proxyprotocol"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
)

func TestInboundPreflightProxyProtocolValidation(t *testing.T) {
	newInstance := func(receiver *proxyman.ReceiverConfig, proxyCfg *dokodemo.Config) (*core.Instance, error) {
		return core.New(&core.Config{
			App: []*serial.TypedMessage{
				serial.ToTypedMessage(&dispatcher.Config{}),
				serial.ToTypedMessage(&proxyman.InboundConfig{}),
				serial.ToTypedMessage(&proxyman.OutboundConfig{}),
				serial.ToTypedMessage(&policy.Config{}),
			},
			Inbound: []*core.InboundHandlerConfig{
				{
					Tag:              "test-in",
					ReceiverSettings: serial.ToTypedMessage(receiver),
					ProxySettings:    serial.ToTypedMessage(proxyCfg),
				},
			},
		})
	}

	t.Run("unix inbound with proxyProtocolTrustedIPs fails preflight", func(t *testing.T) {
		inst, err := newInstance(&proxyman.ReceiverConfig{
			Listen: net.NewIPOrDomain(net.DomainAddress("/tmp/xray-preflight-test.sock")),
			StreamSettings: &internet.StreamConfig{
				ProtocolName: "tcp",
				SocketSettings: &internet.SocketConfig{
					ProxyProtocolTrustedIps: []string{"127.0.0.1"},
				},
			},
		}, &dokodemo.Config{
			RewriteAddress:  net.NewIPOrDomain(net.LocalHostIP),
			RewritePort:     80,
			AllowedNetworks: []net.Network{net.Network_TCP},
		})
		if inst != nil {
			_ = inst.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "only supported on TCP listeners") {
			t.Fatalf("core.New(unix) error = %v, want TCP-only error", err)
		}
	})

	t.Run("protobuf TCP inbound with invalid proxyProtocolTrustedIPs fails preflight", func(t *testing.T) {
		inst, err := newInstance(&proxyman.ReceiverConfig{
			PortList: &net.PortList{Range: []*net.PortRange{{From: 10080, To: 10080}}},
			StreamSettings: &internet.StreamConfig{
				ProtocolName: "tcp",
				SocketSettings: &internet.SocketConfig{
					ProxyProtocolTrustedIps: []string{"not-an-ip"},
				},
			},
		}, &dokodemo.Config{
			RewriteAddress:  net.NewIPOrDomain(net.LocalHostIP),
			RewritePort:     80,
			AllowedNetworks: []net.Network{net.Network_TCP},
		})
		if inst != nil {
			_ = inst.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "invalid proxyProtocolTrustedIPs") {
			t.Fatalf("core.New(invalid trusted IP) error = %v, want invalid proxyProtocolTrustedIPs", err)
		}
	})

	t.Run("pure UDP inbound ignores unused TCP proxy-protocol conflict in preflight", func(t *testing.T) {
		inst, err := newInstance(&proxyman.ReceiverConfig{
			PortList: &net.PortList{Range: []*net.PortRange{{From: 10080, To: 10080}}},
			StreamSettings: &internet.StreamConfig{
				ProtocolName: "tcp",
				SocketSettings: &internet.SocketConfig{
					AcceptProxyProtocol: true,
				},
				Tcpmasks: []*serial.TypedMessage{
					serial.ToTypedMessage(&proxyprotocol.Config{Version: 2}),
				},
			},
		}, &dokodemo.Config{
			RewriteAddress:  net.NewIPOrDomain(net.LocalHostIP),
			RewritePort:     53,
			AllowedNetworks: []net.Network{net.Network_UDP},
		})
		if err != nil {
			t.Fatalf("core.New(pure UDP) unexpected error: %v", err)
		}
		_ = inst.Close()
	})
}

func TestInboundValidationFailureDoesNotRegisterStats(t *testing.T) {
	inst, err := core.New(&core.Config{App: []*serial.TypedMessage{
		serial.ToTypedMessage(&dispatcher.Config{}),
		serial.ToTypedMessage(&proxyman.InboundConfig{}),
		serial.ToTypedMessage(&proxyman.OutboundConfig{}),
		serial.ToTypedMessage(&stats.Config{}),
		serial.ToTypedMessage(&policy.Config{System: &policy.SystemPolicy{Stats: &policy.SystemPolicy_Stats{
			InboundUplink:   true,
			InboundDownlink: true,
		}}}),
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	ctx := context.WithValue(context.Background(), core.XrayKey(1), inst)

	_, err = inbound.NewAlwaysOnInboundHandler(ctx, "invalid-in", &proxyman.ReceiverConfig{
		PortList: &net.PortList{Range: []*net.PortRange{{From: 10080, To: 10080}}},
		StreamSettings: &internet.StreamConfig{
			ProtocolName:   "tcp",
			SocketSettings: &internet.SocketConfig{AcceptProxyProtocol: true},
			Tcpmasks: []*serial.TypedMessage{
				serial.ToTypedMessage(&proxyprotocol.Config{Version: 2}),
			},
		},
	}, &dokodemo.Config{
		RewriteAddress:  net.NewIPOrDomain(net.LocalHostIP),
		RewritePort:     80,
		AllowedNetworks: []net.Network{net.Network_TCP},
	})
	if err == nil {
		t.Fatal("NewAlwaysOnInboundHandler succeeded, want proxy-protocol conflict error")
	}
	sm := inst.GetFeature(fstats.ManagerType()).(fstats.Manager)
	for _, name := range []string{
		"inbound>>>invalid-in>>>traffic>>>uplink",
		"inbound>>>invalid-in>>>traffic>>>downlink",
	} {
		if sm.GetCounter(name) != nil {
			t.Errorf("counter %q registered after failed inbound creation", name)
		}
	}
}
