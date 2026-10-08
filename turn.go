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
	"time"

	"github.com/pion/turn/v4"
)

const jamboreeTURNRealm = "nextendo-jamboree-diagnostic"

type observedTURNConn struct {
	net.PacketConn
	p      *serverContext
	window time.Time
	count  int
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
				PacketConn: &observedTURNConn{PacketConn: conn, p: p},
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
