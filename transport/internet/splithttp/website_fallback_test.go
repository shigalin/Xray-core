package splithttp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"github.com/xtls/xray-core/testing/servers/tcp"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/reality"
	"github.com/xtls/xray-core/transport/internet/stat"
	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

func TestXHTTPWebsiteFallback(t *testing.T) {
	const site = "site.example.test"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%s %s", r.Host, r.URL.RequestURI())
	}))
	defer upstream.Close()
	proxy, err := newWebsiteFallback(strings.TrimPrefix(upstream.URL, "http://"), 443)
	if err != nil {
		t.Fatal(err)
	}
	h := &requestHandler{config: &Config{}, path: "/proxy/", website: proxy, websiteHost: site}
	for _, tc := range []struct {
		name, method, host, sni, path string
		want                          int
	}{
		{"different path", "GET", site, site, "/index?x=1", 200},
		{"matching path without padding", "GET", site + ":8443", site, "/proxy/asset", 200},
		{"trailing-dot host", "GET", "Site.Example.Test.:443", site, "/", 200},
		{"trailing-dot SNI", "GET", site, site + ".", "/", 200},
		{"different SNI", "GET", site, "other.example.test", "/", 404},
		{"different host", "GET", "other.example.test", site, "/", 404},
		{"non-web method", "POST", site, site, "/", 404},
		{"plaintext", "GET", site, "", "/", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "http://"+tc.host+tc.path, nil)
			if tc.sni != "" {
				r.TLS = &tls.ConnectionState{ServerName: tc.sni}
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status = %d, want %d", w.Code, tc.want)
			}
			if tc.want == 200 && w.Body.String() != tc.host+" "+tc.path {
				t.Fatalf("Caddy received unexpected host or path: %q", w.Body.String())
			}
			// Website responses must not carry XHTTP's own response headers.
			if tc.want == 200 && (w.Header().Get("X-Padding") != "" || w.Header().Get("Access-Control-Allow-Origin") != "") {
				t.Fatalf("website response leaked XHTTP headers: %v", w.Header())
			}
		})
	}
}

func TestXHTTPWebsiteFallbackRejectsNonlocalAndSelfTargets(t *testing.T) {
	for _, target := range []string{"example.test:80", "192.0.2.1:80", "127.0.0.1:443", "[::1]:0"} {
		if _, err := newWebsiteFallback(target, 443); err == nil {
			t.Errorf("accepted invalid destination %q", target)
		}
	}
}

func TestListenerCloseEndsWebsiteRequests(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	site := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer site.Close()

	ct, _ := cert.MustGenerate(nil, cert.CommonName("example.com"))
	port := tcp.PickPort()
	listener, err := ListenXH(context.Background(), xnet.LocalHostIP, port, &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/x", HttpFallback: site.Listener.Addr().String()},
		SecurityType:     "tls",
		SecuritySettings: &xtls.Config{ServerName: "example.com", Certificate: []*xtls.Certificate{xtls.ParseCertificate(ct)}},
	}, func(stat.Connection) {})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true, ServerName: "example.com"},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, net.JoinHostPort("127.0.0.1", port.String()))
		},
	}}
	resp, err := client.Get("https://example.com/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	first := make([]byte, 5)
	if _, err := io.ReadFull(resp.Body, first); err != nil || string(first) != "first" {
		t.Fatalf("website did not start streaming: %q %v", first, err)
	}

	listener.Close()
	done := make(chan struct{})
	go func() {
		io.Copy(io.Discard, resp.Body)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("website response kept streaming after the listener was closed")
	}
}

func TestListenXHRejectsInvalidWebsiteFallback(t *testing.T) {
	ct, _ := cert.MustGenerate(nil, cert.CommonName("example.com"))
	tlsSettings := func(serverName string) *xtls.Config {
		return &xtls.Config{ServerName: serverName, Certificate: []*xtls.Certificate{xtls.ParseCertificate(ct)}}
	}
	cases := map[string]struct {
		security string
		settings any
		target   string
	}{
		"no tls":             {"", nil, "127.0.0.1:8080"},
		"tls without sni":    {"tls", tlsSettings(""), "127.0.0.1:8080"},
		"reality":            {"reality", &reality.Config{}, "127.0.0.1:8080"},
		"non-loopback":       {"tls", tlsSettings("example.com"), "192.0.2.1:8080"},
		"own listening port": {"tls", tlsSettings("example.com"), ""},
	}
	for name, c := range cases {
		port := tcp.PickPort()
		target := c.target
		if target == "" {
			target = net.JoinHostPort("127.0.0.1", port.String())
		}
		streamSettings := &internet.MemoryStreamConfig{
			ProtocolName:     "splithttp",
			ProtocolSettings: &Config{Path: "/x", HttpFallback: target},
			SecurityType:     c.security,
		}
		if c.settings != nil {
			streamSettings.SecuritySettings = c.settings
		}
		listener, err := ListenXH(context.Background(), xnet.LocalHostIP, port, streamSettings, func(stat.Connection) {})
		if err == nil {
			listener.Close()
			t.Errorf("%s: expected ListenXH to fail", name)
		}
	}

	port := tcp.PickPort()
	listener, err := ListenXH(context.Background(), xnet.LocalHostIP, port, &internet.MemoryStreamConfig{
		ProtocolName:     "splithttp",
		ProtocolSettings: &Config{Path: "/x", HttpFallback: "127.0.0.1:8080"},
		SecurityType:     "tls",
		SecuritySettings: tlsSettings(" example.com. "),
	}, func(stat.Connection) {})
	if err != nil {
		t.Fatalf("valid fallback rejected: %v", err)
	}
	listener.Close()
}
