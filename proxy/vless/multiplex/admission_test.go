package multiplex

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	appstats "github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type testRebinder func(context.Context, *transport.Link) error

func (f testRebinder) RebindSession(ctx context.Context, link *transport.Link) error {
	return f(ctx, link)
}

type testRetiringWriter struct {
	buf.Writer
	retired chan struct{}
}

func (w testRetiringWriter) RetireSession() { close(w.retired) }

func TestXUDPReuseRechecksAdmission(t *testing.T) {
	for _, tc := range []struct {
		name         string
		result       error
		fresh        bool
		keepOld      bool
		closeCarrier bool
	}{
		{name: "reuse"},
		{name: "deny", result: errors.New("cooldown")},
		{name: "foreign", result: routing.ErrSessionNotOwned, fresh: true},
		{name: "foreign user", result: routing.ErrForeignSession, keepOld: true},
		{name: "carrier closed during rebind", closeCarrier: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := [8]byte{201, 37, 2}
			ctx := session.ContextWithInbound(context.Background(), &session.Inbound{
				User:   &protocol.MemoryUser{Email: "test-user"},
				Source: net.TCPDestination(net.LocalHostIP, 10000),
			})
			old := newInboundFlow(ctx, id, true)
			oldCounter := new(appstats.Counter)
			old.SetSessionLink(transport.Link{Reader: old, Writer: buf.Discard})
			old.SetUplinkPolicy(oldCounter, nil)
			if tc.result != nil {
				old.response = buf.MultiBuffer{buf.FromBytes([]byte("cached reply"))}
			}
			XUDPManager.Lock()
			XUDPManager.Map[id] = old
			XUDPManager.Unlock()
			t.Cleanup(func() {
				XUDPManager.Lock()
				delete(XUDPManager.Map, id)
				XUDPManager.Unlock()
				old.Interrupt()
				old.finish(true)
			})
			received := make(chan string, 1)
			freshCounter := new(appstats.Counter)
			called := false
			wireReader, wireWriter := pipe.New()
			defer wireReader.Interrupt()
			w := &ServerWorker{
				carrier: newCarrier(ctx, transport.Link{Writer: wireWriter}, time.Minute, false),
				dispatch: func(_ context.Context, _ net.Destination, link *transport.Link) error {
					flow := link.Reader.(*inboundFlow)
					flow.SetSessionLink(*link)
					flow.SetUplinkPolicy(freshCounter, nil)
					mb, err := flow.ReadMultiBuffer()
					if err != nil {
						return err
					}
					received <- string(mb[0].Bytes())
					buf.ReleaseMulti(mb)
					return nil
				},
			}
			defer w.Close()
			meta := &FrameMetadata{GlobalID: id, SessionID: 1, Target: net.UDPDestination(net.LocalHostIP, 53)}
			w.rebinder = testRebinder(func(ctx context.Context, link *transport.Link) error {
				if called {
					t.Error("a competing rebind reached the admission hook")
					return errors.New("competing rebind")
				}
				called = true
				if link != old.admissionLink || oldCounter.Value() != 0 || session.InboundFromContext(ctx).User.Email != "test-user" {
					t.Error("wrong link, accounting or authenticated identity at admission")
				}
				if tc.closeCarrier {
					w.Close()
				} else if tc.result == nil {
					competing := *meta
					competing.SessionID++
					reader := &buf.BufferedReader{Reader: buf.NewReader(bytes.NewReader([]byte{0, 1, 'y'}))}
					defer func() { buf.ReleaseMulti(reader.Buffer) }()
					if err := w.bindXUDP(ctx, &competing, reader); err != nil {
						t.Errorf("competing rebind displaced the pending one: %v", err)
					}
				}
				return tc.result
			})
			reader := &buf.BufferedReader{Reader: buf.NewReader(bytes.NewReader([]byte{0, 1, 'x'}))}
			defer func() { buf.ReleaseMulti(reader.Buffer) }()
			err := w.bindXUDP(ctx, meta, reader)
			if !called {
				t.Fatal("cached association was used without checking admission")
			}
			XUDPManager.Lock()
			cached := XUDPManager.Map[id]
			XUDPManager.Unlock()
			switch {
			case tc.fresh:
				if err != nil || cached == nil || cached == old || oldCounter.Value() != 0 {
					t.Fatalf("stale flow was not replaced cleanly: flow=%p err=%v", cached, err)
				}
				select {
				case payload := <-received:
					if payload != "x" || freshCounter.Value() != 1 {
						t.Fatalf("fresh payload=%q, uplink=%d", payload, freshCounter.Value())
					}
				case <-time.After(time.Second):
					t.Fatal("first packet was not redispatched")
				}
			case tc.keepOld:
				if !errors.Is(err, tc.result) || cached != old || oldCounter.Value() != 0 {
					t.Fatalf("foreign caller altered owner's flow: flow=%p err=%v uplink=%d", cached, err, oldCounter.Value())
				}
				mb, err := wireReader.ReadMultiBufferTimeout(20 * time.Millisecond)
				size := mb.Len()
				buf.ReleaseMulti(mb)
				if size != 0 || !errors.Is(err, buf.ErrReadTimeout) {
					t.Fatalf("foreign caller received buffered downlink: size=%d err=%v", size, err)
				}
				old.access.Lock()
				buffered := old.response.Len()
				reqErr := old.requestErr
				old.access.Unlock()
				if reqErr != nil || buffered != int32(len("cached reply")) {
					t.Fatalf("owner's flow was damaged: requestErr=%v buffered=%d", reqErr, buffered)
				}
			case tc.result != nil || tc.closeCarrier:
				if !errors.Is(err, tc.result) || cached != nil || oldCounter.Value() != 0 {
					t.Fatalf("denied packet was accepted or cached: flow=%p err=%v", cached, err)
				}
				if mb, err := old.ReadMultiBuffer(); buf.ReleaseMulti(mb) != nil || !errors.Is(err, io.EOF) {
					t.Fatalf("denied flow did not end: %v", err)
				}
				if tc.closeCarrier {
					return // Closing the carrier already interrupts its writer.
				}
				mb, err := wireReader.ReadMultiBufferTimeout(20 * time.Millisecond)
				size := mb.Len()
				buf.ReleaseMulti(mb)
				if size != 0 || !errors.Is(err, buf.ErrReadTimeout) {
					t.Fatalf("denied carrier received buffered downlink: size=%d err=%v", size, err)
				}
			default:
				if err != nil || cached != old || oldCounter.Value() != 1 {
					t.Fatalf("allowed flow was not reused: flow=%p err=%v", cached, err)
				}
				mb, err := old.ReadMultiBuffer()
				defer buf.ReleaseMulti(mb)
				if err != nil || len(mb) != 1 || string(mb[0].Bytes()) != "x" {
					t.Fatalf("reused flow did not receive payload: %v", err)
				}
			}
		})
	}
}

func TestXUDPReplacementOwnsIDDuringRetirement(t *testing.T) {
	ctx := context.Background()
	id := [8]byte{204, 38, 1}
	oldCarrier := newCarrier(ctx, transport.Link{Writer: buf.Discard}, time.Minute, false)
	defer oldCarrier.Close()
	old := newInboundFlow(ctx, id, true)
	retired := make(chan struct{})
	old.SetSessionLink(transport.Link{Reader: old, Writer: testRetiringWriter{buf.Discard, retired}})
	old.SetUplinkPolicy(nil, nil)
	old.binding = &Session{parent: oldCarrier, flow: old, phase: outputOpen}
	XUDPManager.Lock()
	XUDPManager.Map[id] = old
	XUDPManager.Unlock()
	defer func() {
		XUDPManager.Lock()
		delete(XUDPManager.Map, id)
		XUDPManager.Unlock()
	}()

	retiring := make(chan struct{})
	resume := make(chan struct{})
	defer close(resume)
	go func() {
		<-retiring
		<-resume
		oldCarrier.Unlock()
	}()
	received := make(chan string, 1)
	next := &ServerWorker{
		carrier: newCarrier(ctx, transport.Link{Writer: buf.Discard}, time.Minute, false),
		rebinder: testRebinder(func(context.Context, *transport.Link) error {
			oldCarrier.Lock()
			close(retiring)
			return routing.ErrSessionNotOwned
		}),
		dispatch: func(_ context.Context, _ net.Destination, link *transport.Link) error {
			select {
			case <-retired:
			default:
				t.Error("replacement was dispatched before retiring the old lease")
			}
			flow := link.Reader.(*inboundFlow)
			flow.SetSessionLink(*link)
			flow.SetUplinkPolicy(nil, nil)
			mb, err := flow.ReadMultiBuffer()
			if err == nil {
				received <- string(mb[0].Bytes())
			}
			buf.ReleaseMulti(mb)
			return err
		},
	}
	defer next.Close()
	competing := &ServerWorker{
		carrier: newCarrier(ctx, transport.Link{Writer: buf.Discard}, time.Minute, false),
		rebinder: testRebinder(func(context.Context, *transport.Link) error {
			t.Error("competing carrier reached re-admission during retirement")
			return errors.New("unexpected re-admission")
		}),
		dispatch: func(context.Context, net.Destination, *transport.Link) error {
			t.Error("competing carrier dispatched during retirement")
			return errors.New("unexpected dispatch")
		},
	}
	defer competing.Close()
	bind := func(w *ServerWorker, payload byte) error {
		meta := &FrameMetadata{GlobalID: id, SessionID: 1, Target: net.UDPDestination(net.LocalHostIP, 53)}
		reader := &buf.BufferedReader{Reader: buf.NewReader(bytes.NewReader([]byte{0, 1, payload}))}
		defer func() { buf.ReleaseMulti(reader.Buffer) }()
		return w.bindXUDP(ctx, meta, reader)
	}
	bound := make(chan error, 1)
	go func() { bound <- bind(next, 'a') }()
	select {
	case <-retiring:
	case <-time.After(time.Second):
		t.Fatal("old route did not reach retirement")
	}
	deadline := time.Now().Add(time.Second)
	for {
		XUDPManager.Lock()
		current := XUDPManager.Map[id]
		XUDPManager.Unlock()
		if current == nil {
			t.Fatal("association ID was left unowned during retirement")
		}
		if current != old {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement was not reserved before old route closed")
		}
		time.Sleep(time.Millisecond)
	}
	if err := bind(competing, 'b'); err != nil {
		t.Fatal(err)
	}
	// Let the old carrier finish closing, then verify the winner's first packet.
	resume <- struct{}{}
	select {
	case err := <-bound:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement did not finish")
	}
	select {
	case payload := <-received:
		if payload != "a" {
			t.Fatalf("replacement payload = %q, want a", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement lost the first packet")
	}
}

func TestMuxUplinkAccountingAtAdmissionAndRetirement(t *testing.T) {
	for _, name := range []string{"accepted", "rate wait canceled", "retired before policy", "rebind in progress"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			carrier := newCarrier(ctx, transport.Link{Writer: buf.Discard}, time.Minute, false)
			defer carrier.Close()
			flow := newInboundFlow(ctx, [8]byte{1}, true)
			binding := &Session{parent: carrier, flow: flow, phase: outputOpen}
			flow.binding = binding
			counter := new(appstats.Counter)
			flow.SetSessionLink(transport.Link{Reader: flow, Writer: buf.Discard})
			waiting, released := make(chan struct{}), make(chan struct{})
			switch name {
			case "rebind in progress":
				flow.rebinding = true
				flow.SetUplinkPolicy(counter, nil)
			case "retired before policy":
			default:
				flow.SetUplinkPolicy(counter, func(ctx context.Context, _ int) error {
					close(waiting)
					select {
					case <-released:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
			}
			sent := make(chan error, 1)
			go func() {
				data := buf.MultiBuffer{buf.FromBytes([]byte("abc"))}
				err := flow.accept(data, binding)
				if err != nil {
					buf.ReleaseMulti(data)
				}
				sent <- err
			}()
			if name == "accepted" || name == "rate wait canceled" {
				select {
				case <-waiting:
				case <-time.After(time.Second):
					t.Fatal("payload did not reach limiter")
				}
			}
			mb, err := flow.ReadMultiBufferTimeout(15 * time.Millisecond)
			buf.ReleaseMulti(mb)
			if !errors.Is(err, buf.ErrReadTimeout) || counter.Value() != 0 {
				t.Fatalf("read/accounting before acceptance: read=%v counter=%d", err, counter.Value())
			}
			if name == "accepted" {
				close(released)
			} else if name == "rebind in progress" {
				binding.abort()
			} else {
				flow.Interrupt()
			}
			select {
			case err = <-sent:
			case <-time.After(time.Second):
				t.Fatal("accept did not finish")
			}
			if name == "accepted" {
				if err != nil || counter.Value() != 3 {
					t.Fatalf("accepted payload: err=%v counter=%d", err, counter.Value())
				}
				flow.Interrupt()
			} else {
				if !errors.Is(err, io.ErrClosedPipe) || counter.Value() != 0 {
					t.Fatalf("retired payload: err=%v counter=%d", err, counter.Value())
				}
				if name == "retired before policy" {
					flow.SetUplinkPolicy(counter, nil)
					if counter.Value() != 0 {
						t.Fatal("late policy published a counter after retirement")
					}
				}
			}
		})
	}
}

func TestXUDPDownlinkCounterStopsWhenFlowIsInterrupted(t *testing.T) {
	flow := newInboundFlow(context.Background(), [8]byte{1}, false)
	out := &flowOutput{flow}
	counter := new(appstats.Counter)
	out.SetDownlinkCounter(counter)
	if err := out.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("old"))}); err != nil {
		t.Fatal(err)
	}
	out.Interrupt()
	if err := out.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("late"))}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("write to interrupted route = %v", err)
	}
	if got := counter.Value(); got != 3 {
		t.Fatalf("downlink charge = %d, want 3", got)
	}
}

func TestXUDPPendingRetirementWaitsForLeasePublication(t *testing.T) {
	id := [8]byte{204, 38, 2}
	flow := newInboundFlow(context.Background(), id, true)
	XUDPManager.Lock()
	XUDPManager.Map[id] = flow
	flow.access.Lock()
	flow.beginRetirement()
	flow.access.Unlock()
	XUDPManager.Unlock()

	retired := make(chan struct{})
	done := make(chan struct{})
	publish := sync.OnceFunc(func() {
		flow.SetSessionLink(transport.Link{Reader: flow, Writer: testRetiringWriter{buf.Discard, retired}})
	})
	defer publish()
	go func() {
		flow.finishRetirement()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("pending lease was released before it could be published")
	case <-time.After(15 * time.Millisecond):
	}
	XUDPManager.Lock()
	current := XUDPManager.Map[id]
	XUDPManager.Unlock()
	if current != flow {
		t.Fatal("a reconnect could claim the ID before the lease was retired")
	}
	publish()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retirement did not finish")
	}
	select {
	case <-retired:
	default:
		t.Fatal("published lease was not retired")
	}
	XUDPManager.Lock()
	current = XUDPManager.Map[id]
	XUDPManager.Unlock()
	if current != nil {
		t.Fatal("retired association still registered")
	}
}
