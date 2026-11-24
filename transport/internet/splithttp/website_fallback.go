package splithttp

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// websiteFallback reverse-proxies non-XHTTP requests to a local website. Those
// requests are not proxy connections, so nothing else tracks them; close ends
// the ones still in flight when the listener is closed.
type websiteFallback struct {
	proxy     *httputil.ReverseProxy
	transport *http.Transport
	ctx       context.Context
	cancel    context.CancelFunc
}

func newWebsiteFallback(target string, listenPort int) (*websiteFallback, error) {
	host, port, err := net.SplitHostPort(target)
	number, portErr := strconv.Atoi(port)
	if err != nil || !net.ParseIP(host).IsLoopback() || portErr != nil || number < 1 || number > 65535 || number == listenPort {
		return nil, fmt.Errorf("XHTTP HTTP fallback must point to a different loopback port: %q", target)
	}
	upstream := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, strconv.Itoa(number))}
	// A private transport keeps the website off the process-wide connection pool and
	// bounds how long a stuck upstream can hold a request.
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          32,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &websiteFallback{
		proxy: &httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(upstream)
				r.Out.Host = r.In.Host
			},
			Transport: transport,
			ErrorLog:  log.New(websiteLogWriter{}, "", 0),
		},
		transport: transport,
		ctx:       ctx,
		cancel:    cancel,
	}, nil
}

func (f *websiteFallback) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(f.ctx, cancel)
	defer stop()
	f.proxy.ServeHTTP(w, r.WithContext(ctx))
}

func (f *websiteFallback) close() {
	f.cancel()
	f.transport.CloseIdleConnections()
}

// websiteLogWriter routes ReverseProxy errors into the Xray log instead of stderr.
type websiteLogWriter struct{}

func (websiteLogWriter) Write(p []byte) (int, error) {
	errors.LogInfo(context.Background(), "XHTTP HTTP fallback: ", strings.TrimSpace(string(p)))
	return len(p), nil
}

func normalizeWebsiteHost(name string) string {
	return strings.TrimSuffix(strings.TrimSpace(name), ".")
}

func (h *requestHandler) serveWebsite(w http.ResponseWriter, r *http.Request) bool {
	if h.website == nil || (r.Method != http.MethodGet && r.Method != http.MethodHead) || r.TLS == nil {
		return false
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if !strings.EqualFold(normalizeWebsiteHost(r.TLS.ServerName), h.websiteHost) || !strings.EqualFold(normalizeWebsiteHost(host), h.websiteHost) {
		return false
	}
	h.website.ServeHTTP(w, r)
	return true
}
