package splithttp_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/splithttp"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func Test_ListenXHInvalidH2ParamsReleasesResources(t *testing.T) {
	// A leaked listener must not be rescued by garbage collection during the test.
	gcPercent := debug.SetGCPercent(-1)
	t.Cleanup(func() { debug.SetGCPercent(gcPercent) })

	// Keep Unix socket paths below the platform limit, including on macOS.
	dir, err := os.MkdirTemp("", "xhttp-h2-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})

	settings := func(params string) *internet.MemoryStreamConfig {
		return &internet.MemoryStreamConfig{
			ProtocolName: "splithttp",
			ProtocolSettings: &splithttp.Config{
				Path:     "/sh",
				H2Params: []byte(params),
			},
		}
	}
	addConn := func(conn stat.Connection) { _ = conn.Close() }

	for _, network := range []string{"tcp", "unix"} {
		for _, tc := range []struct {
			name   string
			params string
		}{
			{name: "syntax", params: `{"MaxConcurrentStreams":`},
			{name: "type", params: `{"MaxConcurrentStreams":"bad"}`},
		} {
			t.Run(network+"/"+tc.name, func(t *testing.T) {
				address := net.LocalHostIP
				port := tcp.PickPort()
				endpoint := (&net.TCPAddr{IP: address.IP(), Port: int(port)}).String()
				if network == "unix" {
					endpoint = filepath.Join(dir, tc.name+".sock")
					address = net.DomainAddress(endpoint)
					port = 0
				}

				listener, err := splithttp.ListenXH(context.Background(), address, port, settings(tc.params), addConn)
				if listener != nil {
					_ = listener.Close()
					t.Fatal("invalid h2Params returned a listener")
				}
				if err == nil || !strings.Contains(err.Error(), "failed to parse HTTP2Config") {
					t.Fatalf("expected h2Params parsing error, got %v", err)
				}

				if network == "unix" {
					for _, path := range []string{endpoint, endpoint + ".lock"} {
						if _, err := os.Stat(path); !os.IsNotExist(err) {
							t.Fatalf("invalid h2Params left Unix listener resource %q: %v", path, err)
						}
					}
				}
				// Use a plain listener so SO_REUSEPORT cannot hide a leaked TCP socket.
				probe, err := net.Listen(network, endpoint)
				if err != nil {
					t.Fatalf("listener address was not released after invalid h2Params: %v", err)
				}
				if err := probe.Close(); err != nil {
					t.Fatal(err)
				}

				listener, err = splithttp.ListenXH(context.Background(), address, port, settings(`{"MaxConcurrentStreams":1}`), addConn)
				if err != nil {
					t.Fatalf("corrected h2Params could not reuse the address: %v", err)
				}
				t.Cleanup(func() { _ = listener.Close() })
				transport := &http.Transport{
					DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
						return (&net.Dialer{}).DialContext(ctx, network, endpoint)
					},
				}
				t.Cleanup(transport.CloseIdleConnections)
				client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
				resp, err := client.Get("http://localhost/")
				if err != nil {
					t.Fatalf("corrected listener could not serve HTTP: %v", err)
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusNotFound {
					t.Fatalf("unexpected response status: %d", resp.StatusCode)
				}
			})
		}
	}
}
