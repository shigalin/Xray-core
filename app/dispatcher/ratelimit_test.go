package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	bandwidthfeat "github.com/xtls/xray-core/features/bandwidth"
	featurepolicy "github.com/xtls/xray-core/features/policy"
	featurestats "github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
	"golang.org/x/time/rate"
)

type stubWriter struct{ calls int }

func (w *stubWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	w.calls++
	buf.ReleaseMulti(mb)
	return nil
}
func (w *stubWriter) Close() error { return nil }
func (w *stubWriter) Interrupt()   {}

type stubReader struct{}

func (stubReader) ReadMultiBuffer() (buf.MultiBuffer, error) { return nil, nil }
func (stubReader) Interrupt()                                {}

func TestApplyBandwidthLimitWrapsWriterOnly(t *testing.T) {
	bm := bandwidthfeat.New()
	bm.SetUserLimit("user@example.com", 1024)
	writer := &stubWriter{}
	reader := stubReader{}
	user := &protocol.MemoryUser{Email: "user@example.com"}
	wrapped := applyBandwidthLimit(context.Background(), writer, bm, user)
	if _, ok := wrapped.(*RateLimitWriter); !ok {
		t.Fatal("expected writer to be wrapped by RateLimitWriter")
	}
	if _, ok := any(reader).(stubReader); !ok {
		t.Fatal("reader type unexpectedly changed")
	}
}

func TestWrapLinkUsesBandwidthFeature(t *testing.T) {
	inst := new(core.Instance)
	bm := bandwidthfeat.New()
	bm.SetUserLimit("user@example.com", 1024)
	if err := inst.AddFeature(bm); err != nil {
		t.Fatalf("AddFeature(bandwidth) error = %v", err)
	}
	ctx := context.WithValue(context.Background(), core.XrayKey(1), inst)
	ctx = session.ContextWithInbound(ctx, &session.Inbound{User: &protocol.MemoryUser{Email: "user@example.com"}, Source: net.TCPDestination(net.IPAddress([]byte{127, 0, 0, 1}), 1234)})
	link := &transport.Link{Reader: &buf.TimeoutWrapperReader{Reader: stubReader{}}, Writer: &stubWriter{}}
	_ = WrapLink(ctx, featurepolicy.DefaultManager{}, featurestats.NoopManager{}, link)
	if _, ok := link.Writer.(*RateLimitWriter); !ok {
		t.Fatal("expected WrapLink to wrap writer with bandwidth limiter")
	}
	if _, ok := link.Reader.(*buf.TimeoutWrapperReader); !ok {
		t.Fatal("expected reader wrapper to remain TimeoutWrapperReader")
	}
}

func TestRateLimitWriterRespectsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &RateLimitWriter{
		Writer:  &stubWriter{},
		Limiter: rate.NewLimiter(1, 1),
		Context: ctx,
	}
	b := buf.New()
	b.Extend(2)
	if err := w.WriteMultiBuffer(buf.MultiBuffer{b}); err == nil {
		t.Fatal("expected canceled context to abort wait")
	}
}

type testCounter struct{ v int64 }

func (c *testCounter) Value() int64      { return c.v }
func (c *testCounter) Set(v int64) int64 { old := c.v; c.v = v; return old }
func (c *testCounter) Add(v int64) int64 { old := c.v; c.v += v; return old }

type countingStats struct {
	featurestats.Manager
	counter *testCounter
}

func (m countingStats) GetOrRegisterCounter(string) (featurestats.Counter, error) {
	return m.counter, nil
}

type downlinkStatsPolicy struct{ featurepolicy.Manager }

func (downlinkStatsPolicy) ForLevel(uint32) featurepolicy.Session {
	return featurepolicy.Session{Stats: featurepolicy.Stats{UserDownlink: true}}
}

// Splice copy counts user traffic only when the link writer itself is a
// SizeStatWriter, so the bandwidth limiter must sit inside it.
func TestWrapLinkKeepsStatsWriterOutermost(t *testing.T) {
	inst := new(core.Instance)
	bm := bandwidthfeat.New()
	bm.SetUserLimit("user@example.com", 1024)
	if err := inst.AddFeature(bm); err != nil {
		t.Fatalf("AddFeature(bandwidth) error = %v", err)
	}
	ctx := context.WithValue(context.Background(), core.XrayKey(1), inst)
	inbound := &session.Inbound{User: &protocol.MemoryUser{Email: "user@example.com"}, Source: net.TCPDestination(net.IPAddress([]byte{127, 0, 0, 1}), 1234), CanSpliceCopy: 2}
	ctx = session.ContextWithInbound(ctx, inbound)
	link := &transport.Link{Reader: stubReader{}, Writer: &stubWriter{}}
	_ = WrapLink(ctx, downlinkStatsPolicy{}, countingStats{counter: &testCounter{}}, link)
	if inbound.CanSpliceCopy != 3 {
		t.Fatalf("limited user CanSpliceCopy = %d, want 3 (kernel splice bypasses the limiter)", inbound.CanSpliceCopy)
	}
	stat, ok := link.Writer.(*SizeStatWriter)
	if !ok {
		t.Fatalf("outermost writer = %T, want *SizeStatWriter", link.Writer)
	}
	if _, ok := stat.Writer.(*RateLimitWriter); !ok {
		t.Fatalf("writer inside stats = %T, want *RateLimitWriter", stat.Writer)
	}
}

func TestWrapLinkKeepsSpliceForUnlimitedUser(t *testing.T) {
	inst := new(core.Instance)
	if err := inst.AddFeature(bandwidthfeat.New()); err != nil {
		t.Fatalf("AddFeature(bandwidth) error = %v", err)
	}
	ctx := context.WithValue(context.Background(), core.XrayKey(1), inst)
	inbound := &session.Inbound{User: &protocol.MemoryUser{Email: "user@example.com"}, Source: net.TCPDestination(net.IPAddress([]byte{127, 0, 0, 1}), 1234), CanSpliceCopy: 2}
	ctx = session.ContextWithInbound(ctx, inbound)
	link := &transport.Link{Reader: stubReader{}, Writer: &stubWriter{}}
	_ = WrapLink(ctx, downlinkStatsPolicy{}, countingStats{counter: &testCounter{}}, link)
	if inbound.CanSpliceCopy != 2 {
		t.Fatalf("unlimited user CanSpliceCopy = %d, want 2", inbound.CanSpliceCopy)
	}
}

type nativeStubReader struct{ stubReader }

func (nativeStubReader) SetUplinkPolicy(featurestats.Counter, func(context.Context, int) error) {}

func (nativeStubReader) ReadMultiBufferTimeout(time.Duration) (buf.MultiBuffer, error) {
	return nil, buf.ErrReadTimeout
}

func TestWrapMuxLinkDoesNotCountDownlinkRejectedByLimiter(t *testing.T) {
	inst := new(core.Instance)
	bm := bandwidthfeat.New()
	bm.SetUserLimiter("user@example.com", rate.NewLimiter(1, 4))
	if err := inst.AddFeature(bm); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(context.Background(), core.XrayKey(1), inst)
	ctx = session.ContextWithInbound(ctx, &session.Inbound{User: &protocol.MemoryUser{Email: "user@example.com"}})
	counter := &testCounter{}
	writer := &stubWriter{}
	link := WrapLink(ctx, downlinkStatsPolicy{}, countingStats{counter: counter},
		&transport.Link{Reader: nativeStubReader{}, Writer: writer})
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("12345"))}); err == nil {
		t.Fatal("expected oversized batch to be rejected")
	}
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("1234"))}); err != nil {
		t.Fatal(err)
	}
	if counter.Value() != 4 || writer.calls != 1 {
		t.Fatalf("charge=%d writes=%d, want 4 and 1", counter.Value(), writer.calls)
	}
}

func TestWrapTimeoutOnlyMuxLinkSurvivesOuterContextAndStopsOnClose(t *testing.T) {
	inst := new(core.Instance)
	bm := bandwidthfeat.New()
	lim := rate.NewLimiter(1, 4)
	bm.SetUserLimiter("user@example.com", lim)
	if err := inst.AddFeature(bm); err != nil {
		t.Fatal(err)
	}
	baseCtx := context.WithValue(context.Background(), core.XrayKey(1), inst)
	baseCtx = session.ContextWithInbound(baseCtx, &session.Inbound{User: &protocol.MemoryUser{Email: "user@example.com"}})
	outerCtx, outerCancel := context.WithCancel(baseCtx)
	ctx := session.ContextWithTimeoutOnly(outerCtx, true)
	writer := &stubWriter{}
	link := WrapLink(ctx, downlinkStatsPolicy{}, countingStats{counter: &testCounter{}},
		&transport.Link{Reader: nativeStubReader{}, Writer: writer})

	outerCancel()
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("1234"))}); err != nil {
		t.Fatalf("TimeoutOnly downlink failed after outer Mux context ended: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("1234"))})
	}()
	time.Sleep(15 * time.Millisecond)
	link.Writer.(*RateLimitWriter).Interrupt()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("interrupted writer still completed throttled write")
		}
	case <-time.After(time.Second):
		t.Fatal("Interrupt did not unblock RateLimitWriter wait")
	}
}
