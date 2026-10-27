package dispatcher

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/pipe"
)

type testSessionLifecycle struct{ closed int }

func (s *testSessionLifecycle) DownlinkClosed() { s.closed++ }

func TestGetLinkReportsDownlinkCompletionWithoutWrappingReaders(t *testing.T) {
	observer := &testSessionLifecycle{}
	d := &DefaultDispatcher{}
	in, out := d.getLink(routing.ContextWithSessionLifecycle(context.Background(), observer))
	if _, ok := in.Reader.(*pipe.Reader); !ok {
		t.Fatal("inbound pipe reader was replaced")
	}
	if _, ok := out.Reader.(*pipe.Reader); !ok {
		t.Fatal("outbound pipe reader was replaced")
	}
	common.Close(in.Writer)
	if observer.closed != 0 {
		t.Fatal("upload EOF was reported as completed download")
	}
	common.Close(out.Writer)
	common.Interrupt(out.Writer)
	if observer.closed != 1 {
		t.Fatalf("completion notifications = %d, want 1", observer.closed)
	}
}

type testOutboundObserver struct {
	testSessionLifecycle
	returned int
}

func (o *testOutboundObserver) OutboundReturned() { o.returned++ }

type noHandlerManager struct{ outbound.Manager }

func (noHandlerManager) GetDefaultHandler() outbound.Handler { return nil }

func TestRoutedDispatchReportsOutboundReturn(t *testing.T) {
	observer := &testOutboundObserver{}
	ctx := routing.ContextWithSessionLifecycle(context.Background(), observer)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	d := &DefaultDispatcher{ohm: noHandlerManager{}}
	_, out := d.getLink(ctx)
	d.routedDispatch(ctx, out, net.TCPDestination(net.LocalHostIP, 80))
	if observer.returned != 1 {
		t.Fatalf("outbound return notifications = %d, want 1", observer.returned)
	}
}
