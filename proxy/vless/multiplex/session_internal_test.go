package multiplex

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport"
)

func TestXUDPDetachPreservesNativeRoute(t *testing.T) {
	ctx := context.Background()
	carrier := newCarrier(ctx, transport.Link{Writer: buf.Discard}, time.Minute, false)
	t.Cleanup(func() { carrier.Close() })
	flow := newInboundFlow(ctx, [8]byte{1}, false)
	flow.SetUplinkPolicy(nil, nil)
	s := &Session{ID: 1, parent: carrier, flow: flow, phase: outputOpen}
	flow.binding = s
	carrier.sessions[s.ID] = s
	b := buf.New()
	b.WriteByte('x')
	if err := flow.accept(buf.MultiBuffer{b}, s); err != nil {
		b.Release()
		t.Fatal(err)
	}
	s.abort()
	mb, err := flow.ReadMultiBuffer()
	defer buf.ReleaseMulti(mb)
	if err != nil || len(mb) != 1 || string(mb[0].Bytes()) != "x" {
		t.Fatalf("detach discarded accepted input: %v", err)
	}
	flow.expire()
	mb, err = flow.ReadMultiBuffer()
	buf.ReleaseMulti(mb)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expiry did not close the native route: %v", err)
	}
}
