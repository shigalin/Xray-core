package encoding

import (
	"context"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type proxyProtocolAuthInfo struct {
	credentials.AuthInfo
	conn net.Conn
}

func (a proxyProtocolAuthInfo) AuthType() string {
	if a.AuthInfo != nil {
		return a.AuthInfo.AuthType()
	}
	return "insecure"
}

func (a proxyProtocolAuthInfo) UsedProxyProtocol() bool {
	return internet.ConnUsedProxyProtocol(a.conn)
}

type proxyProtocolCreds struct {
	credentials.TransportCredentials
}

// NewProxyProtocolCredentials wraps base so ServerHandshake preserves the
// underlying connection in peer.AuthInfo for per-connection PROXY protocol checks.
func NewProxyProtocolCredentials(base credentials.TransportCredentials) credentials.TransportCredentials {
	return proxyProtocolCreds{TransportCredentials: base}
}

func (c proxyProtocolCreds) ServerHandshake(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	conn, authInfo, err := c.TransportCredentials.ServerHandshake(rawConn)
	if err != nil {
		return nil, nil, err
	}
	return conn, proxyProtocolAuthInfo{AuthInfo: authInfo, conn: rawConn}, nil
}

func (c proxyProtocolCreds) Clone() credentials.TransportCredentials {
	return proxyProtocolCreds{TransportCredentials: c.TransportCredentials.Clone()}
}

func remoteAddrFromContext(ctx context.Context, socketSettings *internet.SocketConfig) net.Addr {
	var remoteAddr net.Addr
	var proxyProtocolUsed bool
	if pr, ok := peer.FromContext(ctx); ok {
		remoteAddr = pr.Addr
		if pp, ok := pr.AuthInfo.(interface{ UsedProxyProtocol() bool }); ok {
			proxyProtocolUsed = pp.UsedProxyProtocol()
		}
	} else {
		remoteAddr = &net.TCPAddr{
			IP:   []byte{0, 0, 0, 0},
			Port: 0,
		}
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return remoteAddr
	}

	return socketSettings.ClientAddrWithProxyProtocol(md.Get, remoteAddr, proxyProtocolUsed)
}

func localAddrFromContext(ctx context.Context) net.Addr {
	var localAddr net.Addr
	if pr, ok := peer.FromContext(ctx); ok {
		localAddr = pr.LocalAddr
	}
	if localAddr == nil {
		localAddr = &net.TCPAddr{
			IP:   []byte{0, 0, 0, 0},
			Port: 0,
		}
	}
	return localAddr
}
