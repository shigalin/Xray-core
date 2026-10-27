package routing

import (
	"context"
	"errors"
)

// ErrSessionNotOwned is returned by SessionRebinder when a cached link was not
// dispatched by it (e.g. it predates a hot reload). The caller should discard
// the link and dispatch anew rather than treat it as an admission denial.
var ErrSessionNotOwned = errors.New("session not owned by dispatcher")

// SessionLifecycle lets an embedding dispatcher observe completion without
// replacing pipe.Reader, which Mux/XUDP need to retain as a concrete type.
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
