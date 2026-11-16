package xdns

import (
	"errors"
	"io"
	stdnet "net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/finalmask"
)

type resolverTestConn struct {
	net.Conn
	writing    chan struct{}
	closed     chan struct{}
	writeOnce  sync.Once
	closeCalls atomic.Int32
}

func (c *resolverTestConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IP{127, 0, 0, 1}, Port: 53}
}

func (c *resolverTestConn) Write(p []byte) (int, error) {
	c.writeOnce.Do(func() { close(c.writing) })
	return c.Conn.Write(p)
}

func (c *resolverTestConn) Close() error {
	calls := c.closeCalls.Add(1)
	err := c.Conn.Close()
	if calls == 1 {
		close(c.closed)
	}
	return err
}

type resolverTestConnValue struct {
	net.Conn
	_ []byte // A connection returned by a dialer need not be comparable.
}

func waitResolverSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func waitResolverClose(t *testing.T, closeFn func(), message string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		closeFn()
		close(done)
	}()
	waitResolverSignal(t, done, message)
}

func waitTCPResolverWorkers(t *testing.T, r *TCPResolver) {
	t.Helper()
	select {
	case _, ok := <-r.readCh:
		if ok {
			t.Fatal("unexpected response during shutdown")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("workers did not exit")
	}
}

func TestTCPResolverCloseInterruptsIO(t *testing.T) {
	for _, tc := range []struct {
		name        string
		readRequest bool
		valueConn   bool
	}{
		{name: "blocked_write"},
		{name: "blocked_read", readRequest: true},
		{name: "non_comparable_conn", readRequest: true, valueConn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connections := make(chan *resolverTestConn, 2)
			requests := make(chan struct{}, 2)
			dialer := &finalmask.Dialer{DialTCP: func(net.Destination) (net.Conn, error) {
				client, server := stdnet.Pipe()
				c := &resolverTestConn{Conn: client, writing: make(chan struct{}), closed: make(chan struct{})}
				t.Cleanup(func() { client.Close(); server.Close() })
				connections <- c
				if tc.readRequest {
					go func() {
						var request [3]byte
						if _, err := io.ReadFull(server, request[:]); err == nil {
							requests <- struct{}{}
						}
					}()
				}
				if tc.valueConn {
					return resolverTestConnValue{Conn: c}, nil
				}
				return c, nil
			}}
			resolver, err := NewTCPResolver(&ResolverProto{Addr: "127.0.0.1:53", Worker: 2}, dialer)
			if err != nil {
				t.Fatal(err)
			}
			r := resolver.(*TCPResolver)
			t.Cleanup(func() { waitResolverClose(t, r.Close, "resolver cleanup did not return") })
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
				if tc.readRequest {
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
			waitResolverClose(t, r.Close, "Close waited for blocked I/O")
			waitResolverClose(t, r.Close, "repeated Close did not return")
			for _, c := range peers {
				waitResolverSignal(t, c.closed, "Close did not close worker connection")
			}
			waitResolverSignal(t, readDone, "Close did not unblock Read")
			waitResolverSignal(t, sendDone, "Close did not unblock Send")
			waitTCPResolverWorkers(t, r)
			for _, c := range peers {
				if calls := c.closeCalls.Load(); calls != 1 {
					t.Errorf("worker connection closed %d times, want 1", calls)
				}
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
	testDone := t.Context().Done()
	resolver, err := NewTCPResolver(&ResolverProto{Addr: "127.0.0.1:53", Worker: 1}, &finalmask.Dialer{
		DialTCP: func(net.Destination) (net.Conn, error) {
			close(dialing)
			select {
			case <-release:
			case <-testDone:
			}
			return c, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := resolver.(*TCPResolver)
	t.Cleanup(func() { waitResolverClose(t, r.Close, "resolver cleanup did not return") })
	go r.Send([]byte{1})
	waitResolverSignal(t, dialing, "dial did not start")
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		if _, err := r.Read(make([]byte, 4096)); !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("Read after close: %v", err)
		}
	}()
	waitResolverClose(t, r.Close, "Close waited for blocked dial")
	waitResolverSignal(t, readDone, "Close did not unblock Read during dial")
	close(release)
	waitResolverSignal(t, c.closed, "connection returned after Close was not closed")
	waitTCPResolverWorkers(t, r)
	if calls := c.closeCalls.Load(); calls != 1 {
		t.Errorf("late connection closed %d times, want 1", calls)
	}
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
	closeClient := func() { _ = packetConn.Close() }
	t.Cleanup(func() {
		// Release a write that is still waiting on the resolver before closing the client.
		waitResolverClose(t, packetConn.(*xdnsClient).resolvers[0].Close, "resolver cleanup did not return")
		waitResolverClose(t, closeClient, "client cleanup did not return")
	})
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
	waitResolverClose(t, closeClient, "client Close waited for blocked WriteTo")
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
	dialing := make(chan struct{})
	testDone := t.Context().Done()
	packetConn, err := NewClient(&Config{
		Domains:   []*DomainProto{{Name: "example.com", LenLimit: 255, LabelLimit: 63}},
		Resolvers: []*ResolverProto{{Type: "tcp", Addr: "127.0.0.1:53", Worker: 1}},
	}, &finalmask.Dialer{DialTCP: func(net.Destination) (net.Conn, error) {
		close(dialing)
		<-testDone
		return nil, io.ErrClosedPipe
	}})
	if err != nil {
		t.Fatal(err)
	}
	closeClient := func() { _ = packetConn.Close() }
	t.Cleanup(func() { waitResolverClose(t, closeClient, "client cleanup did not return") })
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
	waitResolverClose(t, closeClient, "client Close waited for blocked dial")
	waitResolverSignal(t, readDone, "client ReadFrom waited for blocked dial")
}
