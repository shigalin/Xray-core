package xdns

import (
	"context"
	"io"
	"sync"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/finalmask"
)

type TCPResolver struct {
	dest   net.Destination
	dialer *finalmask.Dialer

	conns   map[int32]net.Conn // active connection by worker ID
	udpAddr *net.UDPAddr
	readCh  chan []byte
	sendCh  chan []byte
	closeCh chan struct{}
	wg      sync.WaitGroup
	mu      sync.Mutex
}

func NewTCPResolver(config *ResolverProto, dialer *finalmask.Dialer) (Resolver, error) {
	dest, err := net.ParseDestination("tcp:" + config.Addr)
	if err != nil {
		return nil, err
	}
	r := &TCPResolver{
		dest:    dest,
		conns:   make(map[int32]net.Conn),
		dialer:  dialer,
		readCh:  make(chan []byte),
		sendCh:  make(chan []byte),
		closeCh: make(chan struct{}),
	}
	go r.run(config.Worker)
	return r, nil
}

func (r *TCPResolver) closed() bool {
	select {
	case <-r.closeCh:
		return true
	default:
		return false
	}
}

func (r *TCPResolver) registerConn(workerID int32, conn net.Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed() {
		return false
	}
	r.conns[workerID] = conn
	switch v := conn.RemoteAddr().(type) {
	case *net.TCPAddr:
		r.udpAddr = &net.UDPAddr{IP: v.IP, Port: v.Port}
	case *net.UDPAddr:
		r.udpAddr = v
	default:
		panic(v)
	}
	return true
}

func (r *TCPResolver) closeConn(workerID int32) {
	r.mu.Lock()
	conn := r.conns[workerID]
	delete(r.conns, workerID)
	r.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (r *TCPResolver) read(reader io.Reader, p []byte) (l int, err error) {
	if _, err := io.ReadFull(reader, p[:2]); err != nil {
		return 0, err
	}
	l = int(p[0])<<8 | int(p[1])
	if l == 0 || l > 4096 {
		return 0, errors.New("err resp len ", l)
	}
	if _, err = io.ReadFull(reader, p[:l]); err != nil {
		return 0, err
	}
	return l, nil
}

func (r *TCPResolver) do(workerID int32, conn *net.Conn, buf *[4096 + 2]byte, p []byte) error {
	var err error
	if *conn == nil {
		*conn, err = r.dialer.DialTCP(r.dest)
		if err != nil {
			*conn = nil
			return err
		}
		if !r.registerConn(workerID, *conn) {
			_ = (*conn).Close()
			*conn = nil
			return io.ErrClosedPipe
		}
	}

	c := *conn
	l := len(p)
	buf[0] = byte(l >> 8)
	buf[1] = byte(l)
	copy(buf[2:], p)
	_, err = c.Write(buf[:2+len(p)])
	if err != nil {
		r.closeConn(workerID)
		*conn = nil
		return err
	}

	b := pool4K.Get().([]byte)
	l, err = r.read(c, b)
	if err != nil {
		r.closeConn(workerID)
		*conn = nil
		pool4K.Put(b[:cap(b)])
		return err
	}
	select {
	case <-r.closeCh:
		pool4K.Put(b[:cap(b)])
	case r.readCh <- b[:l]:
	}
	return nil
}

func (r *TCPResolver) worker(workerID int32) {
	defer r.wg.Done()
	defer r.closeConn(workerID)

	var conn net.Conn
	var buf [4096 + 2]byte
	var p []byte
	for {
		select {
		case <-r.closeCh:
			return
		default:
			select {
			case <-r.closeCh:
				return
			case p = <-r.sendCh:
			}
		}
		if err := r.do(workerID, &conn, &buf, p); err != nil && !r.closed() {
			errors.LogErrorInner(context.Background(), err, "do err")
		}
	}
}

func (r *TCPResolver) run(workers int32) {
	for workerID := range workers {
		r.wg.Add(1)
		go r.worker(workerID)
	}
	r.wg.Wait()
	close(r.readCh)
}

func (r *TCPResolver) Addr() *net.UDPAddr {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.udpAddr
}

func (r *TCPResolver) Read(p []byte) (n int, err error) {
	select {
	case <-r.closeCh:
	case packet, ok := <-r.readCh:
		if ok {
			n = copy(p, packet)
			pool4K.Put(packet[:cap(packet)])
			return n, nil
		}
	}
	return 0, io.ErrClosedPipe
}

func (r *TCPResolver) Send(p []byte) {
	select {
	case <-r.closeCh:
		return
	default:
		b := make([]byte, len(p))
		copy(b, p)
		select {
		case <-r.closeCh:
			return
		case r.sendCh <- b:
		}
	}
}

func (r *TCPResolver) Close() {
	r.mu.Lock()
	if r.closed() {
		r.mu.Unlock()
		return
	}
	close(r.closeCh)
	// Take ownership under the lock so workers cannot close these connections again.
	conns := r.conns
	r.conns = nil
	r.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}
