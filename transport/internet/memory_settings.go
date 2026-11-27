package internet

import (
	"context"
	"reflect"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/transport/internet/finalmask"
	"github.com/xtls/xray-core/transport/internet/finalmask/proxyprotocol"
)

// MemoryStreamConfig is a parsed form of StreamConfig. It is used to reduce the number of Protobuf parses.
type MemoryStreamConfig struct {
	Destination          *net.Destination
	ProtocolName         string
	ProtocolSettings     interface{}
	SecurityType         string
	SecuritySettings     interface{}
	FinalMask            *finalmask.FinalMask
	QuicParams           *QuicParams
	SocketSettings       *SocketConfig
	DownloadSettings     *MemoryStreamConfig
	hasProxyProtocolMask bool
}

func (m *MemoryStreamConfig) hasInboundProxyProtocol() bool {
	if m == nil {
		return false
	}
	if m.SocketSettings.HasProxyProtocol() {
		return true
	}
	if pp, ok := m.ProtocolSettings.(interface{ GetAcceptProxyProtocol() bool }); ok && pp.GetAcceptProxyProtocol() {
		return true
	}
	return false
}

// ValidateInboundTCP checks inbound stream settings that will create a TCP listener.
func (m *MemoryStreamConfig) ValidateInboundTCP() error {
	if m == nil {
		return nil
	}
	if trustedIPs := m.SocketSettings.GetProxyProtocolTrustedIps(); len(trustedIPs) > 0 {
		if err := ValidateProxyProtocolTrustedIPs(trustedIPs); err != nil {
			return errors.New("invalid proxyProtocolTrustedIPs").Base(err)
		}
	}
	if m.hasProxyProtocolMask && m.hasInboundProxyProtocol() {
		return errors.New("finalmask proxy-protocol cannot be combined with acceptProxyProtocol or proxyProtocolTrustedIPs")
	}
	return nil
}

// ValidateInboundUnix checks inbound stream settings that will create a Unix domain socket listener.
func (m *MemoryStreamConfig) ValidateInboundUnix() error {
	if m == nil {
		return nil
	}
	if len(m.SocketSettings.GetProxyProtocolTrustedIps()) > 0 {
		return errors.New("proxyProtocolTrustedIPs is only supported on TCP listeners")
	}
	if m.hasProxyProtocolMask && m.hasInboundProxyProtocol() {
		return errors.New("finalmask proxy-protocol cannot be combined with acceptProxyProtocol or proxyProtocolTrustedIPs")
	}
	return nil
}

// ToMemoryStreamConfig converts a StreamConfig to MemoryStreamConfig. It returns a default non-nil MemoryStreamConfig for nil input.
func ToMemoryStreamConfig(s *StreamConfig) (*MemoryStreamConfig, error) {
	ets, err := s.GetEffectiveTransportSettings()
	if err != nil {
		return nil, err
	}

	mss := &MemoryStreamConfig{
		ProtocolName:     s.GetEffectiveProtocol(),
		ProtocolSettings: ets,
	}

	if s != nil {
		if s.Address != nil {
			mss.Destination = &net.Destination{
				Address: s.Address.AsAddress(),
				Port:    net.Port(s.Port),
				Network: net.Network_TCP,
			}
		}
		mss.SocketSettings = s.SocketSettings
	}

	if s != nil && s.HasSecuritySettings() {
		ess, err := s.GetEffectiveSecuritySettings()
		if err != nil {
			return nil, err
		}
		mss.SecurityType = s.SecurityType
		mss.SecuritySettings = ess
	}
	if s != nil && (len(s.Tcpmasks) != 0 || len(s.Udpmasks) != 0) {
		var tcpMasks []finalmask.TCPMask
		var udpMasks []finalmask.UDPMask

		for i := range s.Tcpmasks {
			instance := common.Must2(s.Tcpmasks[i].GetInstance())
			if _, ok := instance.(*proxyprotocol.Config); ok {
				mss.hasProxyProtocolMask = true
			}
			tcpMasks = append(tcpMasks, instance.(finalmask.TCPMask))
		}
		for i := range s.Udpmasks {
			instance := common.Must2(s.Udpmasks[i].GetInstance())
			udpMasks = append(udpMasks, instance.(finalmask.UDPMask))
		}

		dialTCP := func(ctx context.Context, dest net.Destination) (net.Conn, error) {
			return DialSystem(ctx, dest, mss.SocketSettings)
		}
		listen := func(ctx context.Context, addr net.Addr) (net.Listener, error) {
			if _, ok := addr.(*net.TCPAddr); ok {
				if err := mss.ValidateInboundTCP(); err != nil {
					return nil, err
				}
			} else {
				if err := mss.ValidateInboundUnix(); err != nil {
					return nil, err
				}
			}
			return ListenSystem(ctx, addr, mss.SocketSettings)
		}
		dialUDP := func(ctx context.Context, dest net.Destination) (net.PacketConn, net.Addr, error) {
			conn, err := DialSystem(ctx, dest, mss.SocketSettings)
			if err != nil {
				return nil, nil, err
			}
			var newConn net.PacketConn
			var udpAddr net.Addr
			switch c := conn.(type) {
			case *net.PacketConnWrapper:
				newConn = c.PacketConn
				udpAddr = conn.RemoteAddr()
			case *cnc.Connection:
				newConn = &FakePacketConn{Conn: c}
				udpAddr = &net.UDPAddr{IP: []byte{0, 0, 0, 0}, Port: 0}
			default:
				panic(reflect.TypeOf(c))
			}
			return newConn, udpAddr, nil
		}
		listenPacket := func(ctx context.Context, addr net.Addr) (net.PacketConn, error) {
			return ListenSystemPacket(ctx, addr, mss.SocketSettings)
		}
		mss.FinalMask = finalmask.NewFinalMask(tcpMasks, udpMasks, dialTCP, listen, dialUDP, listenPacket)
	}

	if s != nil && s.QuicParams != nil {
		mss.QuicParams = s.QuicParams
	}

	return mss, nil
}
