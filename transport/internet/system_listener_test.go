package internet_test

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/pires/go-proxyproto"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/finalmask/proxyprotocol"
	"github.com/xtls/xray-core/transport/internet/tcp"
)

func TestRegisterListenerController(t *testing.T) {
	var gotFd uintptr

	common.Must(internet.RegisterListenerController(func(network, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			gotFd = fd
		})
	}))

	conn, err := internet.ListenSystemPacket(context.Background(), &net.UDPAddr{
		IP: net.IPv4zero,
	}, nil)
	common.Must(err)
	common.Must(conn.Close())

	if gotFd == 0 {
		t.Error("expected none-zero fd, but actually 0")
	}
}

func TestListenProxyProtocolTrustedIPs(t *testing.T) {
	dialAndSend := func(addr string, payload string) <-chan error {
		errCh := make(chan error, 1)
		go func() {
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			if _, err := io.WriteString(conn, payload); err != nil {
				errCh <- err
				return
			}
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.CloseWrite()
			}
			errCh <- nil
		}()
		return errCh
	}

	acceptAndRead := func(t *testing.T, ln net.Listener) (net.Addr, string, error) {
		t.Helper()
		conn, err := ln.Accept()
		if err != nil {
			return nil, "", err
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		data, err := io.ReadAll(conn)
		return conn.RemoteAddr(), string(data), err
	}

	t.Run("trusted peer requires PROXY header and rewrites RemoteAddr", func(t *testing.T) {
		ln, err := internet.ListenSystem(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, &internet.SocketConfig{
			ProxyProtocolTrustedIps: []string{"127.0.0.1"},
		})
		if err != nil {
			t.Fatalf("ListenSystem: %v", err)
		}
		defer ln.Close()

		// Valid PROXY v1 header from trusted peer.
		sendErr := dialAndSend(ln.Addr().String(), "PROXY TCP4 203.0.113.9 127.0.0.1 12345 443\r\nhello")
		remoteAddr, body, err := acceptAndRead(t, ln)
		if err != nil {
			t.Fatalf("acceptAndRead valid PROXY: %v", err)
		}
		if err := <-sendErr; err != nil {
			t.Fatalf("dialAndSend valid PROXY: %v", err)
		}
		if remoteAddr.String() != "203.0.113.9:12345" {
			t.Fatalf("RemoteAddr = %v, want 203.0.113.9:12345", remoteAddr)
		}
		if body != "hello" {
			t.Fatalf("body = %q, want %q", body, "hello")
		}

		// Missing PROXY header from trusted peer must be rejected.
		sendErr = dialAndSend(ln.Addr().String(), "plain-direct-traffic-without-proxy-header")
		_, _, err = acceptAndRead(t, ln)
		if !errors.Is(err, proxyproto.ErrNoProxyProtocol) {
			t.Fatalf("missing PROXY header error = %v, want ErrNoProxyProtocol", err)
		}
		<-sendErr
	})

	t.Run("untrusted peer skips PROXY header and cannot spoof RemoteAddr", func(t *testing.T) {
		ln, err := internet.ListenSystem(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, &internet.SocketConfig{
			ProxyProtocolTrustedIps: []string{"198.51.100.0/24"},
		})
		if err != nil {
			t.Fatalf("ListenSystem: %v", err)
		}
		defer ln.Close()

		// Normal direct connection succeeds.
		sendErr := dialAndSend(ln.Addr().String(), "hello-direct")
		remoteAddr, body, err := acceptAndRead(t, ln)
		if err != nil {
			t.Fatalf("acceptAndRead direct: %v", err)
		}
		if err := <-sendErr; err != nil {
			t.Fatalf("dialAndSend direct: %v", err)
		}
		tcpAddr, ok := remoteAddr.(*net.TCPAddr)
		if !ok || !tcpAddr.IP.Equal(net.IPv4(127, 0, 0, 1)) {
			t.Fatalf("RemoteAddr = %v, want 127.0.0.1", remoteAddr)
		}
		if body != "hello-direct" {
			t.Fatalf("body = %q, want %q", body, "hello-direct")
		}

		// Spoofed PROXY header from untrusted peer is treated as raw payload.
		spoofed := "PROXY TCP4 203.0.113.9 127.0.0.1 12345 443\r\nhello"
		sendErr = dialAndSend(ln.Addr().String(), spoofed)
		remoteAddr, body, err = acceptAndRead(t, ln)
		if err != nil {
			t.Fatalf("acceptAndRead spoofed: %v", err)
		}
		if err := <-sendErr; err != nil {
			t.Fatalf("dialAndSend spoofed: %v", err)
		}
		tcpAddr, ok = remoteAddr.(*net.TCPAddr)
		if !ok || !tcpAddr.IP.Equal(net.IPv4(127, 0, 0, 1)) {
			t.Fatalf("RemoteAddr = %v, want 127.0.0.1", remoteAddr)
		}
		if body != spoofed {
			t.Fatalf("body = %q, want raw spoofed payload %q", body, spoofed)
		}
	})

	t.Run("acceptProxyProtocol without trusted IPs requires PROXY header from all peers", func(t *testing.T) {
		ln, err := internet.ListenSystem(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, &internet.SocketConfig{
			AcceptProxyProtocol: true,
		})
		if err != nil {
			t.Fatalf("ListenSystem: %v", err)
		}
		defer ln.Close()

		sendErr := dialAndSend(ln.Addr().String(), "PROXY TCP4 198.51.100.7 127.0.0.1 54321 443\r\nok")
		remoteAddr, body, err := acceptAndRead(t, ln)
		if err != nil {
			t.Fatalf("acceptAndRead: %v", err)
		}
		if err := <-sendErr; err != nil {
			t.Fatalf("dialAndSend: %v", err)
		}
		if remoteAddr.String() != "198.51.100.7:54321" || body != "ok" {
			t.Fatalf("got (%v, %q), want (198.51.100.7:54321, %q)", remoteAddr, body, "ok")
		}

		sendErr = dialAndSend(ln.Addr().String(), "not-a-proxy-header-payload")
		_, _, err = acceptAndRead(t, ln)
		if !errors.Is(err, proxyproto.ErrNoProxyProtocol) {
			t.Fatalf("error = %v, want ErrNoProxyProtocol", err)
		}
		<-sendErr
	})

	t.Run("invalid trusted IPs rejected", func(t *testing.T) {
		_, err := internet.ListenSystem(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, &internet.SocketConfig{
			ProxyProtocolTrustedIps: []string{"bad-ip"},
		})
		if err == nil {
			t.Fatalf("ListenSystem succeeded with invalid ProxyProtocolTrustedIps, want error")
		}
	})

	t.Run("unix listener rejects proxyProtocolTrustedIPs", func(t *testing.T) {
		sockPath := filepath.Join(t.TempDir(), "test.sock")
		_, err := internet.ListenSystem(context.Background(), &net.UnixAddr{Name: sockPath, Net: "unix"}, &internet.SocketConfig{
			ProxyProtocolTrustedIps: []string{"127.0.0.1"},
		})
		if err == nil {
			t.Fatalf("ListenSystem on unix addr succeeded with ProxyProtocolTrustedIps, want error")
		}
	})

	t.Run("ValidateInboundTCP/Unix and FinalMask.Listen reject invalid or conflicting proxy-protocol configs while ToMemoryStreamConfig allows outbounds", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			cfg  *internet.StreamConfig
		}{
			{
				name: "sockopt proxyProtocolTrustedIPs",
				cfg: &internet.StreamConfig{
					ProtocolName: "tcp",
					SocketSettings: &internet.SocketConfig{
						ProxyProtocolTrustedIps: []string{"127.0.0.1"},
					},
					Tcpmasks: []*serial.TypedMessage{
						serial.ToTypedMessage(&proxyprotocol.Config{Version: 2}),
					},
				},
			},
			{
				name: "legacy tcp AcceptProxyProtocol",
				cfg: &internet.StreamConfig{
					ProtocolName: "tcp",
					TransportSettings: []*internet.TransportConfig{
						{
							ProtocolName: "tcp",
							Settings:     serial.ToTypedMessage(&tcp.Config{AcceptProxyProtocol: true}),
						},
					},
					Tcpmasks: []*serial.TypedMessage{
						serial.ToTypedMessage(&proxyprotocol.Config{Version: 2}),
					},
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				mss, err := internet.ToMemoryStreamConfig(tc.cfg)
				if err != nil {
					t.Fatalf("ToMemoryStreamConfig(%s) unexpected error: %v", tc.name, err)
				}
				err = mss.ValidateInboundTCP()
				if err == nil || !strings.Contains(err.Error(), "proxy-protocol") {
					t.Fatalf("ValidateInboundTCP(%s) error = %v, want proxy-protocol conflict error", tc.name, err)
				}
				ln, err := mss.FinalMask.Listen(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if ln != nil {
					_ = ln.Close()
				}
				if err == nil || !strings.Contains(err.Error(), "proxy-protocol") {
					t.Fatalf("FinalMask.Listen(%s) error = %v, want proxy-protocol conflict error", tc.name, err)
				}
			})
		}

		t.Run("ValidateInboundTCP rejects invalid protobuf trusted IP", func(t *testing.T) {
			mss, err := internet.ToMemoryStreamConfig(&internet.StreamConfig{
				ProtocolName: "tcp",
				SocketSettings: &internet.SocketConfig{
					ProxyProtocolTrustedIps: []string{"not-an-ip"},
				},
			})
			if err != nil {
				t.Fatalf("ToMemoryStreamConfig: %v", err)
			}
			err = mss.ValidateInboundTCP()
			if err == nil || !strings.Contains(err.Error(), "invalid proxyProtocolTrustedIPs") {
				t.Fatalf("ValidateInboundTCP error = %v, want invalid proxyProtocolTrustedIPs", err)
			}
		})

		t.Run("ValidateInboundUnix rejects non-empty trusted IPs", func(t *testing.T) {
			mss, err := internet.ToMemoryStreamConfig(&internet.StreamConfig{
				ProtocolName: "tcp",
				SocketSettings: &internet.SocketConfig{
					ProxyProtocolTrustedIps: []string{"127.0.0.1"},
				},
			})
			if err != nil {
				t.Fatalf("ToMemoryStreamConfig: %v", err)
			}
			err = mss.ValidateInboundUnix()
			if err == nil || !strings.Contains(err.Error(), "only supported on TCP listeners") {
				t.Fatalf("ValidateInboundUnix error = %v, want TCP-only error", err)
			}
		})
	})
}
