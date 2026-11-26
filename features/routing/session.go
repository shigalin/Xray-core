package routing

import (
	"context"
	"errors"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/features/stats"
)

// ErrSessionNotOwned is returned by SessionRebinder when a cached link was not
// dispatched by it (e.g. it predates a hot reload). The caller should discard
// the link and dispatch anew rather than treat it as an admission denial.
var ErrSessionNotOwned = errors.New("session not owned by dispatcher")

// ErrForeignSession is returned by SessionRebinder when the caller is not
// allowed to take over a live session owned by another active identity.
// Unlike ErrSessionNotOwned, the caller is rejected while the owner's cached
// session remains intact.
var ErrForeignSession = errors.New("session belongs to another user")

// UplinkPolicyReceiver configures a native link before it accepts payload.
type UplinkPolicyReceiver interface {
	buf.TimeoutReader
	common.Interruptible
	SetUplinkPolicy(stats.Counter, func(context.Context, int) error)
}

// SessionLifecycle lets an embedding dispatcher observe completion without
// wrapping the link's reader, whose type identifies native Mux/XUDP flows
// (see UplinkPolicyReceiver).
type SessionLifecycle interface {
	DownlinkClosed()
}

// OutboundObserver is optionally implemented by a SessionLifecycle to learn
// when the outbound handler has returned. A direct outbound returns after both
// directions finished; one that hands the link to a Mux client returns early.
type OutboundObserver interface {
	OutboundReturned()
}

type sessionLifecycleKey struct{}

func ContextWithSessionLifecycle(ctx context.Context, observer SessionLifecycle) context.Context {
	return context.WithValue(ctx, sessionLifecycleKey{}, observer)
}

func SessionLifecycleFromContext(ctx context.Context) SessionLifecycle {
	observer, _ := ctx.Value(sessionLifecycleKey{}).(SessionLifecycle)
	return observer
}
