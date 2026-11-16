package xdns

import (
	"errors"
	"io"
	stdnet "net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/finalmask"
)

type resolverTestConn struct {
	net.Conn
	writing   chan struct{}
	closed    chan struct{}
	writeOnce sync.Once
	closeOnce sync.Once
}

func (c *resolverTestConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IP{127, 0, 0, 1}, Port: 53}
}

func (c *resolverTestConn) Write(p []byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writing) })
	return c.Conn.Write(p)
}

func (c *resolverTestConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}

func waitResolverSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func TestTCPResolverCloseInterruptsIO(t *testing.T) {
	for _, readResponse := range []bool{false, true} {
		name := "blocked_write"
		if readResponse {
			name = "blocked_read"
		}
		t.Run(name, func(t *testing.T) {
			connections := make(chan *resolverTestConn, 2)
			requests := make(chan struct{}, 2)
			dialer := &finalmask.Dialer{DialTCP: func(net.Destination) (net.Conn, error) {
				client, server := stdnet.Pipe()
				c := &resolverTestConn{Conn: client, writing: make(chan struct{}), closed: make(chan struct{})}
				t.Cleanup(func() { client.Close(); server.Close() })
				connections <- c
				if readResponse {
					go func() {
						var request [3]byte
						if _, err := io.ReadFull(server, request[:]); err == nil {
							requests <- struct{}{}
						}
					}()
				}
				return c, nil
			}}
			resolver, err := NewTCPResolver(&ResolverProto{Addr: "127.0.0.1:53", Worker: 2}, dialer)
			if err != nil {
				t.Fatal(err)
			}
			r := resolver.(*TCPResolver)
			t.Cleanup(r.Close)
			peers := make([]*resolverTestConn, 0, 2)
			for range 2 {
				sent := make(chan struct{})
				go func() { r.Send([]byte{1}); close(sent) }()
				waitResolverSignal(t, sent, "Send did not reach worker")
				select {
				case c := <-connections:
					peers = append(peers, c)
					waitResolverSignal(t, c.writing, "worker did not start write")
				case <-time.After(2 * time.Second):
					t.Fatal("worker did not dial")
				}
				if readResponse {
					waitResolverSignal(t, requests, "server did not receive request")
				}
			}
			readDone := make(chan struct{})
			go func() {
				defer close(readDone)
				if _, err := r.Read(make([]byte, 4096)); !errors.Is(err, io.ErrClosedPipe) {
					t.Errorf("Read after close: %v", err)
				}
			}()
			sendDone := make(chan struct{})
			go func() { r.Send([]byte{2}); close(sendDone) }()
			r.Close()
			r.Close()
			for _, c := range peers {
				waitResolverSignal(t, c.closed, "Close did not close worker connection")
			}
			waitResolverSignal(t, readDone, "Close did not unblock Read")
			waitResolverSignal(t, sendDone, "Close did not unblock Send")
			select {
			case _, ok := <-r.readCh:
				if ok {
					t.Fatal("unexpected response")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("workers did not exit")
			}
		})
	}
}

func TestTCPResolverClosesLateDial(t *testing.T) {
	client, server := stdnet.Pipe()
	defer client.Close()
	defer server.Close()
	c := &resolverTestConn{Conn: client, writing: make(chan struct{}), closed: make(chan struct{})}
	dialing, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	resolver, err := NewTCPResolver(&ResolverProto{Addr: "127.0.0.1:53", Worker: 1}, &finalmask.Dialer{
		DialTCP: func(net.Destination) (net.Conn, error) { close(dialing); <-release; return c, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	r := resolver.(*TCPResolver)
	defer r.Close()
	go r.Send([]byte{1})
	waitResolverSignal(t, dialing, "dial did not start")
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		if _, err := r.Read(make([]byte, 4096)); !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("Read after close: %v", err)
		}
	}()
	r.Close()
	waitResolverSignal(t, readDone, "Close did not unblock Read during dial")
	unblock()
	waitResolverSignal(t, c.closed, "connection returned after Close was not closed")
	select {
	case <-c.writing:
		t.Fatal("closed resolver wrote to late connection")
	default:
	}
}

func TestXDNSClientCloseInterruptsBlockedWriteTo(t *testing.T) {
	client, server := stdnet.Pipe()
	defer client.Close()
	defer server.Close()
	c := &resolverTestConn{Conn: client, writing: make(chan struct{}), closed: make(chan struct{})}
	packetConn, err := NewClient(&Config{
		Domains:   []*DomainProto{{Name: "example.com", LenLimit: 255, LabelLimit: 63}},
		Resolvers: []*ResolverProto{{Type: "tcp", Addr: "127.0.0.1:53", Worker: 1}},
		ExtraPoll: 1,
	}, &finalmask.Dialer{DialTCP: func(net.Destination) (net.Conn, error) { return c, nil }})
	if err != nil {
		t.Fatal(err)
	}
	// Close the resolver directly on failure to release a blocked client Close.
	defer packetConn.(*xdnsClient).resolvers[0].Close()
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		if _, err := packetConn.WriteTo([]byte{1}, &net.UDPAddr{}); !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("WriteTo interrupted by Close: %v", err)
		}
	}()
	requestRead := make(chan struct{})
	go func() {
		var length [2]byte
		if _, err := io.ReadFull(server, length[:]); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, server, int64(length[0])<<8|int64(length[1])); err != nil {
			return
		}
		close(requestRead)
	}()
	waitResolverSignal(t, requestRead, "server did not receive first request")
	closeDone := make(chan struct{})
	go func() { _ = packetConn.Close(); close(closeDone) }()
	waitResolverSignal(t, closeDone, "client Close waited for blocked WriteTo")
	waitResolverSignal(t, writeDone, "WriteTo did not exit after Close")
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		if _, _, err := packetConn.ReadFrom(make([]byte, 4096)); !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("ReadFrom after Close: %v", err)
		}
	}()
	waitResolverSignal(t, readDone, "client ReadFrom did not exit after Close")
}

func TestXDNSClientCloseInterruptsReadFromDuringDial(t *testing.T) {
	dialing, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	packetConn, err := NewClient(&Config{
		Domains:   []*DomainProto{{Name: "example.com", LenLimit: 255, LabelLimit: 63}},
		Resolvers: []*ResolverProto{{Type: "tcp", Addr: "127.0.0.1:53", Worker: 1}},
	}, &finalmask.Dialer{DialTCP: func(net.Destination) (net.Conn, error) {
		close(dialing)
		<-release
		return nil, io.ErrClosedPipe
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer packetConn.Close()
	if _, err := packetConn.WriteTo([]byte{1}, &net.UDPAddr{}); err != nil {
		t.Fatal(err)
	}
	waitResolverSignal(t, dialing, "dial did not start")
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		if _, _, err := packetConn.ReadFrom(make([]byte, 4096)); !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("ReadFrom after Close: %v", err)
		}
	}()
	_ = packetConn.Close()
	waitResolverSignal(t, readDone, "client ReadFrom waited for blocked dial")
}
