package dispatcher

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"golang.org/x/time/rate"
)

type RateLimitWriter struct {
	Writer  buf.Writer
	Limiter *rate.Limiter
	Context context.Context

	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	closed bool
}

func (w *RateLimitWriter) waitContext() context.Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ctx == nil {
		base := w.Context
		if base == nil {
			base = context.Background()
		}
		w.ctx, w.cancel = context.WithCancel(base)
		if w.closed {
			w.cancel()
		}
	}
	return w.ctx
}

func (w *RateLimitWriter) stop() {
	w.mu.Lock()
	w.closed = true
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (w *RateLimitWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if w.Limiter != nil {
		if err := w.Limiter.WaitN(w.waitContext(), int(mb.Len())); err != nil {
			buf.ReleaseMulti(mb)
			return err
		}
	}
	return w.Writer.WriteMultiBuffer(mb)
}

func (w *RateLimitWriter) Close() error {
	w.stop()
	return common.Close(w.Writer)
}

func (w *RateLimitWriter) Interrupt() {
	w.stop()
	common.Interrupt(w.Writer)
}
