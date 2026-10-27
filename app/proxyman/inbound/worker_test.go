package inbound

import (
	"errors"
	"testing"

	"github.com/xtls/xray-core/common/net"
)

type fakeHub struct {
	stopErr          error
	closed, stopped  bool
	supportsStopping bool
}

func (h *fakeHub) Close() error   { h.closed = true; return nil }
func (h *fakeHub) Addr() net.Addr { return nil }

type stoppableHub struct{ fakeHub }

func (h *stoppableHub) StopAccepting() error { h.stopped = true; return h.stopErr }

func TestStopOrClosePrefersStoppingOverClosing(t *testing.T) {
	stream := &stoppableHub{}
	if err := (&tcpWorker{hub: stream}).StopAccepting(); err != nil || !stream.stopped || stream.closed {
		t.Fatalf("stream listener: err=%v stopped=%v closed=%v", err, stream.stopped, stream.closed)
	}
	h3 := &stoppableHub{fakeHub: fakeHub{stopErr: errors.New("cannot stop alone")}}
	if err := (&tcpWorker{hub: h3}).StopAccepting(); err != nil || !h3.closed {
		t.Fatalf("listener unable to stop alone was not closed: err=%v closed=%v", err, h3.closed)
	}
	datagram := &fakeHub{}
	if err := (&dsWorker{hub: datagram}).StopAccepting(); err != nil || !datagram.closed {
		t.Fatalf("listener without StopAccepting was not closed: err=%v closed=%v", err, datagram.closed)
	}
	if err := (&tcpWorker{}).StopAccepting(); err != nil {
		t.Fatalf("worker without hub: %v", err)
	}
}
