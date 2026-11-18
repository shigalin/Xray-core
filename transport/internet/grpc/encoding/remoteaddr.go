package encoding

import (
	"context"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

func remoteAddrFromContext(ctx context.Context, socketSettings *internet.SocketConfig) net.Addr {
	var remoteAddr net.Addr
	if pr, ok := peer.FromContext(ctx); ok {
		remoteAddr = pr.Addr
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

	return socketSettings.ClientAddr(md.Get, remoteAddr)
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
