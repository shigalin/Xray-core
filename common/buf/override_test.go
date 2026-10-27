package buf_test

import (
	"testing"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

type closeRecorder struct{ closed, interrupted int }

func (w *closeRecorder) WriteMultiBuffer(mb MultiBuffer) error { ReleaseMulti(mb); return nil }
func (w *closeRecorder) ReadMultiBuffer() (MultiBuffer, error) { return nil, nil }
func (w *closeRecorder) Close() error                          { w.closed++; return nil }
func (w *closeRecorder) Interrupt()                            { w.interrupted++ }

func TestEndpointOverrideForwardsCloseAndInterrupt(t *testing.T) {
	inner := &closeRecorder{}
	addr := net.ParseAddress("example.com")
	writer := &EndpointOverrideWriter{Writer: inner, Dest: addr, OriginalDest: addr}
	common.Close(writer)
	common.Interrupt(writer)
	reader := &EndpointOverrideReader{Reader: inner, Dest: addr, OriginalDest: addr}
	common.Interrupt(reader)
	if inner.closed != 1 || inner.interrupted != 2 {
		t.Fatalf("close/interrupt forwarded = %d/%d, want 1/2", inner.closed, inner.interrupted)
	}
}
