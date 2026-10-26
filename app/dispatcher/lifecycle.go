package dispatcher

import (
	"sync"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/routing"
)

// Notify only when this direction ends; EOF on the opposite direction is a
// half-close and must not end the whole session.
type lifecycleWriter struct {
	buf.Writer
	once     sync.Once
	observer routing.SessionLifecycle
}

func (w *lifecycleWriter) Close() error {
	err := common.Close(w.Writer)
	w.once.Do(w.observer.DownlinkClosed)
	return err
}

func (w *lifecycleWriter) Interrupt() {
	common.Interrupt(w.Writer)
	w.once.Do(w.observer.DownlinkClosed)
}
