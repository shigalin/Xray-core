package dispatcher

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common"
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
