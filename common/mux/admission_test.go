package mux

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type admissionDispatcher struct {
	rebind func(context.Context, *transport.Link) error
	fresh  *transport.Link
}

func (*admissionDispatcher) Type() interface{} { return routing.DispatcherType() }
func (*admissionDispatcher) Start() error      { return nil }
func (*admissionDispatcher) Close() error      { return nil }
func (d *admissionDispatcher) Dispatch(context.Context, net.Destination) (*transport.Link, error) {
	if d.fresh != nil {
		return d.fresh, nil
	}
	return nil, errors.New("unexpected fresh dispatch")
}
func (*admissionDispatcher) DispatchLink(context.Context, net.Destination, *transport.Link) error {
	return nil
}
func (d *admissionDispatcher) RebindSession(ctx context.Context, link *transport.Link) error {
	return d.rebind(ctx, link)
}

type admissionWriter struct{ writes, closes int }

func (w *admissionWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	w.writes++
	buf.ReleaseMulti(mb)
	return nil
}
func (w *admissionWriter) Close() error { w.closes++; return nil }

func TestXUDPReuseRechecksAdmissionBeforeWriting(t *testing.T) {
	r, _ := pipe.New()
	writer := &admissionWriter{}
	x := &XUDP{GlobalID: [8]byte{201, 37, 2}, Status: Active, Mux: &Session{input: r, output: writer}}
	XUDPManager.Lock()
	XUDPManager.Map[x.GlobalID] = x
	XUDPManager.Unlock()
	t.Cleanup(func() { XUDPManager.Lock(); delete(XUDPManager.Map, x.GlobalID); XUDPManager.Unlock() })
	called := false
	d := &admissionDispatcher{rebind: func(ctx context.Context, link *transport.Link) error {
		called = true
		if writer.writes != 0 {
			t.Fatal("payload written before admission")
		}
		if link.Reader != r || link.Writer != writer {
			t.Fatal("wrong cached session passed to hook")
		}
		if session.InboundFromContext(ctx).User.Email != "test-user" {
			t.Fatal("authenticated identity was lost")
		}
		return errors.New("cooldown")
	}}
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		User:   &protocol.MemoryUser{Email: "test-user"},
		Source: net.TCPDestination(net.LocalHostIP, 10000),
	})
	worker := &ServerWorker{dispatcher: d}
	meta := &FrameMetadata{GlobalID: x.GlobalID, Target: net.UDPDestination(net.LocalHostIP, 53)}
	reader := &buf.BufferedReader{Reader: buf.NewReader(bytes.NewReader([]byte{0, 1, 'x'}))}
	if err := worker.handleStatusNew(ctx, meta, reader); err == nil {
		t.Fatal("denied cached session was reused")
	}
	if !called || writer.writes != 0 || writer.closes != 1 {
		t.Fatalf("hook=%v writes=%d closes=%d", called, writer.writes, writer.closes)
	}
	XUDPManager.Lock()
	_, cached := XUDPManager.Map[x.GlobalID]
	XUDPManager.Unlock()
	if cached {
		t.Fatal("denied XUDP session remained reusable in cache")
	}
}

func TestXUDPReuseAllowedPreservesTransport(t *testing.T) {
	r, _ := pipe.New()
	writer := &admissionWriter{}
	x := &XUDP{Mux: &Session{input: r, output: writer}}
	d := &admissionDispatcher{rebind: func(context.Context, *transport.Link) error { return nil }}
	if reused, err := rebindXUDP(context.Background(), d, x); err != nil || reused != x {
		t.Fatalf("reused=%p want %p, err=%v", reused, x, err)
	}
	if writer.closes != 0 || writer.writes != 0 {
		t.Fatal("successful admission altered the transport")
	}
}

func TestXUDPReuseNotOwnedFallsBackToFreshDispatch(t *testing.T) {
	oldReader, _ := pipe.New()
	oldWriter := &admissionWriter{}
	x := &XUDP{GlobalID: [8]byte{201, 37, 3}, Status: Expiring, Mux: &Session{input: oldReader, output: oldWriter}}
	XUDPManager.Lock()
	XUDPManager.Map[x.GlobalID] = x
	XUDPManager.Unlock()
	t.Cleanup(func() { XUDPManager.Lock(); delete(XUDPManager.Map, x.GlobalID); XUDPManager.Unlock() })

	freshReader, freshPipeWriter := pipe.New()
	freshWriter := &admissionWriter{}
	t.Cleanup(func() { freshPipeWriter.Close() })
	d := &admissionDispatcher{
		rebind: func(context.Context, *transport.Link) error { return routing.ErrSessionNotOwned },
		fresh:  &transport.Link{Reader: freshReader, Writer: freshWriter},
	}
	ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
		User:   &protocol.MemoryUser{Email: "test-user"},
		Source: net.TCPDestination(net.LocalHostIP, 10000),
	})
	_, muxWriter := pipe.New()
	worker := &ServerWorker{dispatcher: d, link: &transport.Link{Writer: muxWriter}, sessionManager: NewSessionManager()}
	meta := &FrameMetadata{GlobalID: x.GlobalID, SessionID: 1, Target: net.UDPDestination(net.LocalHostIP, 53)}
	reader := &buf.BufferedReader{Reader: buf.NewReader(bytes.NewReader([]byte{0, 1, 'x'}))}
	if err := worker.handleStatusNew(ctx, meta, reader); err != nil {
		t.Fatalf("foreign cached session broke the Mux connection: %v", err)
	}
	if oldWriter.writes != 0 || oldWriter.closes != 1 {
		t.Fatalf("foreign link: writes=%d closes=%d", oldWriter.writes, oldWriter.closes)
	}
	if freshWriter.writes != 1 {
		t.Fatalf("payload was not dispatched through a fresh link: writes=%d", freshWriter.writes)
	}
	XUDPManager.Lock()
	cur := XUDPManager.Map[x.GlobalID]
	XUDPManager.Unlock()
	if cur == nil || cur == x || cur.Status != Active || cur.Mux.output != freshWriter {
		t.Fatalf("cache entry was not replaced by the fresh session: %+v", cur)
	}
}
