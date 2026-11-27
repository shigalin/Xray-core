package grpc_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/grpc"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func TestListenUnixRejectsProxyProtocolTrustedIPs(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "grpc.sock")
	ln, err := grpc.Listen(context.Background(), net.DomainAddress(sockPath), 0, &internet.MemoryStreamConfig{
		ProtocolName:     "grpc",
		ProtocolSettings: &grpc.Config{},
		SocketSettings: &internet.SocketConfig{
			ProxyProtocolTrustedIps: []string{"127.0.0.1"},
		},
	}, func(stat.Connection) {})
	if err == nil {
		if ln != nil {
			_ = ln.Close()
		}
		t.Fatalf("grpc.Listen on unix socket with proxyProtocolTrustedIPs succeeded, want error")
	}
	if ln != nil {
		t.Fatalf("grpc.Listen returned non-nil listener on error")
	}
}
