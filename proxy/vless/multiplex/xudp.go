package multiplex

import (
	"context"
	stderrors "errors"
	"io"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
)

// inboundFlow owns native route input and persistent binding state. Its response
// packet response mailbox retains native bytes until the physical sender claims them.
// Locks nest association map -> flow -> one carrier; I/O is outside them.
type inboundFlow struct {
	access sync.Mutex

	requestWake, responseWake sync.Cond
	request, response         buf.MultiBuffer
	requestErr, responseErr   error
	uplinkCounter             stats.Counter
	downlinkCounter           stats.Counter
	uplinkBound               bool
	uplinkLimiter             func(context.Context, int) error
	ready                     chan struct{}
	readyClosed               bool
	requireAdmission          bool
	allowUnconfiguredRead     bool
	admissionLink             *transport.Link
	admissionDone             chan struct{}
	completeAdmission         sync.Once
	rebinding                 bool
	retiring                  chan struct{}
	limit                     int32
	binding                   *Session
	GlobalID                  [8]byte
	Expire                    time.Time
}

func newInboundFlow(ctx context.Context, id [8]byte, requireAdmission bool) *inboundFlow {
	x := &inboundFlow{
		limit: policy.BufferPolicyFromContext(ctx).PerConnection, GlobalID: id,
		ready: make(chan struct{}), requireAdmission: requireAdmission,
	}
	x.requestWake.L, x.responseWake.L = &x.access, &x.access
	if requireAdmission {
		x.admissionDone = make(chan struct{})
	}
	return x
}

// SetSessionLink publishes the admission link before the dispatcher wraps it
// for routing and statistics. The link is used only for re-admission.
func (x *inboundFlow) SetSessionLink(link transport.Link) {
	if !x.requireAdmission {
		return
	}
	x.access.Lock()
	defer x.access.Unlock()
	if x.admissionLink == nil {
		x.admissionLink = &link
		x.finishAdmission()
		x.notifyReady()
	}
}

func (x *inboundFlow) finishAdmission() {
	if x.admissionDone != nil {
		x.completeAdmission.Do(func() { close(x.admissionDone) })
	}
}

func (x *inboundFlow) SetUplinkPolicy(counter stats.Counter, wait func(context.Context, int) error) {
	x.access.Lock()
	defer x.access.Unlock()
	if !x.uplinkBound {
		x.uplinkCounter, x.uplinkLimiter, x.uplinkBound = counter, wait, true
		x.notifyReady()
	}
}

// notifyReady is called with access held. No payload can enter the flow
// until the account's limiter, counter and lease have all been published.
func (x *inboundFlow) notifyReady() {
	if !x.readyClosed && (x.requestErr != nil || x.uplinkBound && (!x.requireAdmission || x.admissionLink != nil)) {
		x.readyClosed = true
		close(x.ready)
	}
}

func (x *inboundFlow) ReadMultiBuffer() (buf.MultiBuffer, error) {
	x.access.Lock()
	defer x.access.Unlock()
	return x.readLocked(nil)
}

func (x *inboundFlow) ReadMultiBufferTimeout(duration time.Duration) (buf.MultiBuffer, error) {
	x.access.Lock()
	defer x.access.Unlock()
	if len(x.request) > 0 || x.requestErr != nil {
		return x.readLocked(nil)
	}
	timedOut := false
	timer := time.AfterFunc(duration, func() {
		x.access.Lock()
		timedOut = true
		x.requestWake.Broadcast()
		x.access.Unlock()
	})
	defer timer.Stop()
	return x.readLocked(&timedOut)
}

// readLocked has no timeout closure or escaping flag on ordinary native reads.
func (x *inboundFlow) readLocked(timedOut *bool) (buf.MultiBuffer, error) {
	// Raw callbacks can consume a link without going through WrapLink.
	if x.allowUnconfiguredRead && !x.uplinkBound {
		x.uplinkBound = true
		x.notifyReady()
	}
	for len(x.request) == 0 && x.requestErr == nil && (timedOut == nil || !*timedOut) {
		x.requestWake.Wait()
	}
	if len(x.request) > 0 {
		data := x.request
		x.request = nil
		x.requestWake.Broadcast()
		return data, nil
	}
	if x.requestErr != nil {
		return nil, x.requestErr
	}
	return nil, buf.ErrReadTimeout
}

// accept leaves rejected data with the parser so a failed retained route can
// restart with the very same first packet and destination.
func (x *inboundFlow) accept(data buf.MultiBuffer, binding *Session) error {
	select {
	case <-x.ready:
	case <-binding.parent.WaitClosed():
		return io.ErrClosedPipe
	}
	x.access.Lock()
	if !x.waitForBinding(binding) {
		x.access.Unlock()
		return io.ErrClosedPipe
	}
	size := data.Len()
	var waitErr error
	if wait := x.uplinkLimiter; wait != nil {
		if binding.uplinkCtx == nil {
			binding.uplinkCtx, binding.stopUplink = context.WithCancel(binding.parent.ctx)
		}
		ctx := binding.uplinkCtx
		x.access.Unlock()
		// Throttle before admitting and counting the payload. Reads continue to
		// honor their deadlines, and closing a flow need not wait for this call.
		waitErr = wait(ctx, int(size))
		x.access.Lock()
	}
	defer x.access.Unlock()
	for {
		if !x.waitForBinding(binding) {
			return io.ErrClosedPipe
		}
		if waitErr != nil {
			return waitErr
		}
		if x.limit < 0 || x.request.Len() <= x.limit {
			if x.uplinkCounter != nil {
				x.uplinkCounter.Add(int64(size))
			}
			if len(x.request) == 0 {
				x.request = data
			} else {
				x.request, _ = buf.MergeMulti(x.request, data)
			}
			x.requestWake.Broadcast()
			return nil
		}
		x.requestWake.Wait()
	}
}

// A rebind can move the device lease while no flow locks are held. Pause the
// previous carrier until that decision is reflected in the flow.
func (x *inboundFlow) waitForBinding(binding *Session) bool {
	for x.requestErr == nil && x.binding == binding && !binding.parent.Closed() {
		if !x.rebinding {
			return true
		}
		x.requestWake.Wait()
	}
	return false
}

// Interrupt is native request abort. Ordinary wire EOF instead seals this
// input in Session.close, preserving payload already accepted from the parser.
func (x *inboundFlow) Interrupt() {
	x.access.Lock()
	x.requestErr = io.ErrClosedPipe
	x.request = buf.ReleaseMulti(x.request)
	x.stopBindingUplink()
	x.notifyReady()
	x.requestWake.Broadcast()
	x.access.Unlock()
}

type flowOutput struct{ *inboundFlow }

func (w *flowOutput) SetDownlinkCounter(counter stats.Counter) bool {
	w.access.Lock()
	w.downlinkCounter = counter
	w.access.Unlock()
	return true
}

func (w *flowOutput) Interrupt()   { w.finish(true) }
func (w *flowOutput) Close() error { w.finish(false); return nil }
func (x *inboundFlow) finish(abort bool) {
	x.access.Lock()
	if abort {
		x.responseErr = io.ErrClosedPipe
		x.response = buf.ReleaseMulti(x.response)
	} else if x.responseErr == nil {
		x.responseErr = io.EOF
	}
	if s := x.binding; s != nil {
		c := s.parent
		c.Lock()
		if abort && s.phase == outputOpen {
			s.closeLocked(x, true, true)
		} else {
			c.scheduleLocked(s)
		}
		c.Unlock()
	}
	x.responseWake.Broadcast()
	x.access.Unlock()
}

func (x *inboundFlow) expire() {
	x.access.Lock()
	x.sealUplink()
	x.access.Unlock()
	x.finish(true)
}

func (x *inboundFlow) sealUplink() {
	if x.requestErr == nil {
		x.requestErr = io.EOF
		x.notifyReady()
		x.requestWake.Broadcast()
	}
	x.stopBindingUplink()
}

func (x *inboundFlow) retire() {
	x.expire()
	if x.admissionDone != nil {
		<-x.admissionDone
	}
	x.access.Lock()
	link := x.admissionLink
	x.access.Unlock()
	if link != nil {
		if writer, ok := link.Writer.(interface{ RetireSession() }); ok {
			writer.RetireSession()
		}
	}
}

// beginRetirement is called with both the association and flow locks held.
func (x *inboundFlow) beginRetirement() {
	x.retiring = make(chan struct{})
	x.sealUplink()
}

func (x *inboundFlow) finishRetirement() {
	x.retire()
	XUDPManager.Lock()
	if XUDPManager.Map[x.GlobalID] == x {
		delete(XUDPManager.Map, x.GlobalID)
	}
	close(x.retiring)
	XUDPManager.Unlock()
}

func (x *inboundFlow) stopBindingUplink() {
	if s := x.binding; s != nil && s.stopUplink != nil {
		s.stopUplink()
	}
}

// WriteMultiBuffer admits the whole raw offer under the original soft policy.
// Response-capacity waiters are separate from request readers.
func (w *flowOutput) WriteMultiBuffer(data buf.MultiBuffer) error {
	x := w.inboundFlow
	x.access.Lock()
	defer x.access.Unlock()
	for x.responseErr == nil && x.limit >= 0 && x.response.Len() > x.limit {
		x.responseWake.Wait()
	}
	if x.responseErr != nil {
		buf.ReleaseMulti(data)
		return io.ErrClosedPipe
	}
	if data.IsEmpty() {
		buf.ReleaseMulti(data)
		return nil
	}
	if x.downlinkCounter != nil {
		x.downlinkCounter.Add(int64(data.Len()))
	}
	if len(x.response) == 0 {
		x.response = data
	} else {
		x.response, _ = buf.MergeMulti(x.response, data)
	}
	if s := x.binding; s != nil {
		s.parent.Lock()
		s.parent.scheduleLocked(s)
		s.parent.Unlock()
	}
	return nil
}

// packetReady is checked with the flow and carrier locked.
func (s *Session) packetReady() bool {
	x := s.flow
	return x != nil && x.binding == s &&
		(s.phase == outputOpen && (len(x.response) != 0 || x.responseErr != nil) ||
			s.phase == outputStarting && x.requestErr == nil && len(x.response) != 0)
}

// One gated actor claims packet data or retires one exact terminal identity.
// It returns custody only; the sender encodes after both logical locks release.
func (c *carrier) responseTurn(id uint16, s *Session) (buf.MultiBuffer, bool) {
	if s != nil && s.flow != nil {
		s.flow.access.Lock()
		defer s.flow.access.Unlock()
	}
	c.Lock()
	defer c.Unlock()
	if c.Closed() {
		return nil, false
	}
	if s == nil {
		return nil, c.invalidateUnknownLocked(id)
	}
	if c.sessions[id] != s {
		return nil, false
	}
	x := s.flow
	ready := s.packetReady()
	if ready && len(x.response) == 0 {
		s.closeLocked(x, x.responseErr == io.ErrClosedPipe, true)
	}
	if s.phase == outputDone {
		delete(c.ready, s.ID)
		delete(c.sessions, s.ID)
		return nil, true
	}
	if !ready {
		delete(c.ready, s.ID)
		return nil, false
	}
	n := min(4, len(x.response))
	data := x.response[:n:n]
	x.response = x.response[n:]
	if len(x.response) == 0 {
		x.response = nil
	}
	x.responseWake.Broadcast()
	// A terminal follow-up cannot run until this DATA turn completes physically.
	if !s.packetReady() {
		delete(c.ready, s.ID)
	}
	return data, false
}

// reserveReplacement is called with XUDPManager locked. The first caller
// keeps ownership of the replacement while it closes the old route.
func (w *ServerWorker) reserveReplacement(ctx context.Context, id [8]byte) *inboundFlow {
	replacement := w.newFlow(ctx, id)
	replacement.rebinding = true
	XUDPManager.Map[id] = replacement
	return replacement
}

func (w *ServerWorker) bindXUDP(ctx context.Context, meta *FrameMetadata, reader *buf.BufferedReader) error {
	data, err := readPacket(reader, meta.Target)
	if err != nil {
		return err
	}
	ctx = session.ContextWithTimeoutOnly(ctx, true)
	var reserved *inboundFlow
	XUDPManager.Lock()
	for {
		x := XUDPManager.Map[meta.GlobalID]
		fresh := x == nil || x == reserved
		if x == nil {
			x = w.newFlow(ctx, meta.GlobalID)
		}
		x.access.Lock()
		if retiring := x.retiring; retiring != nil {
			x.access.Unlock()
			XUDPManager.Unlock()
			select {
			case <-retiring:
				XUDPManager.Lock()
				continue
			case <-w.WaitClosed():
				buf.ReleaseMulti(data)
				return w.Err()
			}
		}
		if !fresh && (x.rebinding || !x.readyClosed) {
			x.access.Unlock()
			XUDPManager.Unlock()
			buf.ReleaseMulti(data)
			return nil
		}
		if x.admissionLink != nil && w.rebinder == nil {
			x.access.Unlock()
			XUDPManager.Unlock()
			buf.ReleaseMulti(data)
			return errors.New("XUDP flow requires session admission")
		}
		if previous := x.binding; previous != nil {
			previous.parent.Lock()
			if previous.phase == outputStarting {
				previous.parent.Unlock()
				x.access.Unlock()
				XUDPManager.Unlock()
				buf.ReleaseMulti(data)
				errors.LogWarningInner(ctx, errors.New("conflict"), "XUDP hit ", meta.GlobalID)
				return nil
			}
			previous.parent.Unlock()
		}
		if !fresh && w.rebinder != nil {
			// The callback may evict this flow via the device lease.
			x.rebinding = true
			link := x.admissionLink
			x.access.Unlock()
			XUDPManager.Unlock()
			rebindErr := routing.ErrSessionNotOwned
			if link != nil {
				rebindErr = w.rebinder.RebindSession(ctx, link)
			}
			XUDPManager.Lock()
			x.access.Lock()
			x.rebinding = false
			x.requestWake.Broadcast()
			if current := XUDPManager.Map[meta.GlobalID]; current != x {
				x.sealUplink()
				x.access.Unlock()
				XUDPManager.Unlock()
				buf.ReleaseMulti(data)
				if rebindErr != nil && !stderrors.Is(rebindErr, routing.ErrSessionNotOwned) {
					return rebindErr
				}
				return nil
			}
			if rebindErr == nil && x.requestErr != nil {
				rebindErr = routing.ErrSessionNotOwned
			}
			if rebindErr != nil || w.Closed() {
				if stderrors.Is(rebindErr, routing.ErrForeignSession) && x.requestErr == nil {
					x.access.Unlock()
					XUDPManager.Unlock()
					buf.ReleaseMulti(data)
					return rebindErr
				}
				replace := stderrors.Is(rebindErr, routing.ErrSessionNotOwned) && !w.Closed()
				if replace {
					reserved = w.reserveReplacement(ctx, meta.GlobalID)
					x.sealUplink()
				} else {
					x.beginRetirement()
				}
				x.access.Unlock()
				XUDPManager.Unlock()
				if replace {
					x.retire()
					XUDPManager.Lock()
					continue
				}
				x.finishRetirement()
				buf.ReleaseMulti(data)
				return rebindErr
			}
		}
		if previous := x.binding; previous != nil {
			previous.parent.Lock()
			previous.closeLocked(x, true, true)
			previous.parent.Unlock()
		}
		w.Lock()
		if w.Closed() || w.sessions[meta.SessionID] != nil {
			// The lease may already have moved to the new source IP.
			if x == reserved {
				delete(XUDPManager.Map, meta.GlobalID)
			} else if !fresh && w.rebinder != nil {
				x.beginRetirement()
			}
			w.Unlock()
			x.access.Unlock()
			XUDPManager.Unlock()
			if !fresh && w.rebinder != nil {
				x.finishRetirement()
			}
			buf.ReleaseMulti(data)
			return errors.New("failed to add new session")
		}
		s := &Session{
			parent: w.carrier, ID: meta.SessionID, phase: outputStarting,
			target: net.Destination{Network: net.Network_UDP}, flow: x,
		}
		w.invalidateUnknownLocked(s.ID)
		w.sessions[s.ID] = s
		XUDPManager.Map[meta.GlobalID] = x
		x.rebinding = false
		x.binding, x.Expire = s, time.Time{}
		w.scheduleLocked(s)
		w.Unlock()
		x.access.Unlock()
		XUDPManager.Unlock()
		if fresh {
			go w.dispatchFlow(ctx, meta.Target, x, &flowOutput{x})
			select {
			case <-x.ready:
			case <-w.WaitClosed():
				buf.ReleaseMulti(data)
				// A prepared route may already belong to a new carrier.
				XUDPManager.Lock()
				x.access.Lock()
				pending := !x.readyClosed && XUDPManager.Map[x.GlobalID] == x && x.retiring == nil
				if pending {
					x.beginRetirement()
				}
				x.access.Unlock()
				XUDPManager.Unlock()
				if pending {
					x.finishRetirement()
				}
				return w.Err()
			}
		}
		err = x.accept(data, s)

		XUDPManager.Lock()
		x.access.Lock()
		w.Lock()
		live := !w.Closed() && w.sessions[s.ID] == s && x.binding == s && s.phase == outputStarting
		current := XUDPManager.Map[meta.GlobalID]
		restart := stderrors.Is(err, io.ErrClosedPipe) && !fresh
		retry := restart && live && (current == x || current == nil)
		admitted := live && current == x
		if restart {
			s.closeLocked(x, false, false)
			if retry {
				x.responseErr = io.ErrClosedPipe
				x.response = buf.ReleaseMulti(x.response)
				x.responseWake.Broadcast()
				reserved = w.reserveReplacement(ctx, meta.GlobalID)
			}
		} else if admitted {
			w.count++
			if err == nil && (x.responseErr == nil || len(x.response) != 0) {
				s.phase = outputOpen
				w.active++
			} else {
				s.closeLocked(x, err != nil || x.responseErr == io.ErrClosedPipe, true)
			}
			w.scheduleLocked(s)
		}
		w.Unlock()
		x.access.Unlock()
		if retry {
			XUDPManager.Unlock()
			x.retire()
			XUDPManager.Lock()
			continue
		}
		XUDPManager.Unlock()
		if err != nil {
			buf.ReleaseMulti(data)
		}
		if restart {
			if !live {
				return nil
			}
			x.expire()
		}
		if restart || !admitted {
			s.abort()
			return errors.New("failed to add new session")
		}
		return nil
	}
}
