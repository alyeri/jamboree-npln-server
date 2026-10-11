package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/turn/v4"
)

const jamboreeTURNRealm = "nextendo-jamboree-diagnostic"

type observedTURNConn struct {
	net.PacketConn
	p              *serverContext
	window         time.Time
	count          int
	responseMu     sync.Mutex
	responseWindow time.Time
	responseCount  int
}

func turnResponseStatus(packet []byte) (int, int, bool) {
	if len(packet) < 20 || binary.BigEndian.Uint32(packet[4:8]) != stunCookie || int(binary.BigEndian.Uint16(packet[2:4])) != len(packet)-20 {
		return 0, 0, false
	}

	kind := int(binary.BigEndian.Uint16(packet[:2]))
	code := 0

	for offset := 20; offset < len(packet); {
		if offset+4 > len(packet) {
			return 0, 0, false
		}

		attribute := binary.BigEndian.Uint16(packet[offset : offset+2])
		length := int(binary.BigEndian.Uint16(packet[offset+2 : offset+4]))
		start := offset + 4

		if start+length > len(packet) {
			return 0, 0, false
		}

		if attribute == 9 && length >= 4 {
			code = int(packet[start+2]&7)*100 + int(packet[start+3])
		}

		offset = start + (length+3)&^3

		if offset > len(packet) {
			return 0, 0, false
		}
	}

	return kind, code, true
}

func (c *observedTURNConn) WriteTo(packet []byte, address net.Addr) (int, error) {
	wirePacket := unpaddedTURNChannelData(packet)
	n, err := c.PacketConn.WriteTo(wirePacket, address)

	if kind, code, ok := turnResponseStatus(packet); ok {
		c.responseMu.Lock()

		if time.Since(c.responseWindow) >= time.Second {
			c.responseWindow, c.responseCount = time.Now(), 0
		}

		c.responseCount++

		if c.responseCount <= 32 {
			c.p.event(map[string]any{
				"event":        "turn_response_sent",
				"message_type": kind,
				"status":       code,
				"bytes":        n,
				"write_failed": err != nil,
			})
		}

		c.responseMu.Unlock()
	}

	if err == nil && n == len(wirePacket) {
		return len(packet), nil
	}

	return n, err
}

// UDP ChannelData padding is optional. Jamboree's connection test expects
// exactly the four-byte header plus the declared payload, without padding.
func unpaddedTURNChannelData(packet []byte) []byte {
	if len(packet) < 4 {
		return packet
	}

	channel := binary.BigEndian.Uint16(packet[:2])
	length := 4 + int(binary.BigEndian.Uint16(packet[2:4]))

	if channel < 0x4000 || channel > 0x7fff || length > len(packet) || (length+3)&^3 != len(packet) {
		return packet
	}

	return packet[:length]
}

func (c *observedTURNConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)

	if err == nil {
		if time.Since(c.window) >= time.Second {
			c.window, c.count = time.Now(), 0
		}

		c.count++

		if c.count <= 4 {
			kind := -1

			if n >= 2 {
				kind = int(binary.BigEndian.Uint16(b[:2]))
			}

			c.p.event(map[string]any{
				"event":        "turn_packet_received",
				"bytes":        n,
				"message_type": kind,
			})
		}
	}

	return n, addr, err
}

func turnPassword(username string, key []byte) string {
	mac := hmac.New(sha256.New, key)

	mac.Write([]byte("jamboree-turn\x00" + username))

	return base64.RawStdEncoding.EncodeToString(mac.Sum(nil))
}

func newTURNCredentials(key []byte) (string, string, error) {
	nonce := make([]byte, 8)

	if _, e := rand.Read(nonce); e != nil {
		return "", "", e
	}

	user := fmt.Sprintf("%d:%s", time.Now().Add(15*time.Minute).Unix(), hex.EncodeToString(nonce))

	return user, turnPassword(user, key), nil
}

func validTURNUsername(user string) bool {
	p := strings.Split(user, ":")

	if len(p) != 2 || len(p[1]) != 16 {
		return false
	}

	_, e := hex.DecodeString(p[1])

	if e != nil {
		return false
	}

	exp, e := strconv.ParseInt(p[0], 10, 64)

	return e == nil && exp > time.Now().Unix() && exp <= time.Now().Add(16*time.Minute).Unix()
}

type restrictedRelayPorts struct {
	*turn.RelayAddressGeneratorPortRange
}

func (r *restrictedRelayPorts) AllocatePacketConn(network string, port int) (net.PacketConn, net.Addr, error) {
	if port != 0 && (port < int(r.MinPort) || port > int(r.MaxPort)) {
		return nil, nil, fmt.Errorf("relay port outside diagnostic range")
	}

	return r.RelayAddressGeneratorPortRange.AllocatePacketConn(network, port)
}

func startJamboreeTURN(p *serverContext, listen, relayIP string, allowLocal bool) (*turn.Server, net.Addr, error) {
	a, e := net.ResolveUDPAddr("udp4", listen)

	if e != nil {
		return nil, nil, e
	}

	ip := net.ParseIP(relayIP).To4()

	if ip == nil || len(p.identityKey) != 32 {
		return nil, nil, fmt.Errorf("invalid relay configuration")
	}

	conn, e := net.ListenUDP("udp4", a)

	if e != nil {
		return nil, nil, e
	}

	server, e := turn.NewServer(turn.ServerConfig{
		Realm: jamboreeTURNRealm,
		AuthHandler: func(user, realm string, _ net.Addr) ([]byte, bool) {
			if realm != jamboreeTURNRealm || !validTURNUsername(user) {
				return nil, false
			}

			return turn.GenerateAuthKey(user, realm, turnPassword(user, p.identityKey)), true
		},
		PacketConnConfigs: []turn.PacketConnConfig{
			{
				PacketConn: &observedTURNConn{
					PacketConn: conn,
					p:          p,
				},
				RelayAddressGenerator: &restrictedRelayPorts{
					&turn.RelayAddressGeneratorPortRange{
						RelayAddress: ip,
						Address:      ip.String(),
						MinPort:      17500,
						MaxPort:      17599,
					},
				},
				PermissionHandler: func(_ net.Addr, peer net.IP) bool {
					return allowLocal || peer.IsGlobalUnicast() && !peer.IsPrivate() && !peer.IsLoopback() && !peer.IsLinkLocalUnicast()
				},
			},
		},
		EventHandler: turn.EventHandler{
			OnAuth: func(_, _ net.Addr, protocol, _, _, method string, verdict bool) {
				p.event(map[string]any{
					"event":    "turn_auth",
					"protocol": protocol,
					"method":   method,
					"accepted": verdict,
				})
			},
			OnAllocationCreated: func(_, _ net.Addr, protocol, _, _ string, _ net.Addr, _ int) {
				p.event(map[string]any{
					"event":    "turn_allocation_created",
					"protocol": protocol,
				})
			},
			OnAllocationDeleted: func(_, _ net.Addr, _, _, _ string) {
				p.event(map[string]any{
					"event": "turn_allocation_deleted",
				})
			},
		},
	})

	if e != nil {
		conn.Close()

		return nil, nil, e
	}

	p.event(map[string]any{
		"event":          "turn_start",
		"listen":         listen,
		"relay_min_port": 17500,
		"relay_max_port": 17599,
	})

	return server, conn.LocalAddr(), nil
}

func serveJamboreeTURN(ctx context.Context, p *serverContext, listen string) error {
	ip, _, e := net.SplitHostPort(listen)

	if e != nil {
		return e
	}

	s, _, e := startJamboreeTURN(p, listen, ip, false)

	if e != nil {
		return e
	}
	defer s.Close()

	<-ctx.Done()

	return nil
}
