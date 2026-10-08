package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"net"
	"time"
)

const stunCookie uint32 = 0x2112a442

// Basic unauthenticated UDP Binding, RFC 8489. No TURN allocation is advertised.
func stunBindingResponse(req []byte, peer *net.UDPAddr) []byte {
	if len(req) < 20 ||
		len(req) > 512 ||
		binary.BigEndian.Uint16(req) != 1 ||
		binary.BigEndian.Uint32(req[4:]) != stunCookie ||
		int(binary.BigEndian.Uint16(req[2:])) != len(req)-20 ||
		len(req)%4 != 0 ||
		peer.Port < 1 ||
		peer.Port > 65535 {
		return nil
	}

	unknown := []byte{}

	for offset := 20; offset < len(req); {
		if offset+4 > len(req) {
			return nil
		}

		kind := binary.BigEndian.Uint16(req[offset:])
		length := int(binary.BigEndian.Uint16(req[offset+2:]))
		end := offset + 4 + length

		if end > len(req) {
			return nil
		}

		if kind == 0x8028 {
			if length != 4 || end != len(req) || binary.BigEndian.Uint32(req[offset+4:]) != crc32.ChecksumIEEE(req[:offset])^0x5354554e {
				return nil
			}
		} else if kind < 0x8000 {
			unknown = binary.BigEndian.AppendUint16(unknown, kind)
		}

		offset += 4 + (length+3)&^3
	}

	out := make([]byte, 20)
	copy(out[4:], req[4:20])
	binary.BigEndian.PutUint16(out, 0x0101)
	attribute := func(kind uint16, value []byte) {
		out = binary.BigEndian.AppendUint16(out, kind)
		out = binary.BigEndian.AppendUint16(out, uint16(len(value)))
		out = append(out, value...)

		for len(out)%4 != 0 {
			out = append(out, 0)
		}
	}

	if len(unknown) > 0 {
		binary.BigEndian.PutUint16(out, 0x0111)
		attribute(0x0009, append([]byte{0, 0, 4, 20}, []byte("Unknown Attribute")...))
		attribute(0x000a, unknown)
	} else {
		ip := peer.IP.To4()
		family := byte(1)

		if ip == nil {
			ip = peer.IP.To16()
			family = 2
		}

		if ip == nil {
			return nil
		}

		value := []byte{0, family}
		value = binary.BigEndian.AppendUint16(value, uint16(peer.Port)^uint16(stunCookie>>16))
		mask := req[4:20]

		for i, b := range ip {
			value = append(value, b^mask[i])
		}

		attribute(0x0020, value)
	}

	binary.BigEndian.PutUint16(out[2:], uint16(len(out)-20+8))
	sum := crc32.ChecksumIEEE(out) ^ 0x5354554e
	attribute(0x8028, binary.BigEndian.AppendUint32(nil, sum))

	return out
}

func serveSTUN(address string, p *serverContext) error {
	a, e := net.ResolveUDPAddr("udp4", address)
	if e != nil {
		return e
	}

	conn, e := net.ListenUDP("udp4", a)
	if e != nil {
		return e
	}
	defer conn.Close()

	p.event(map[string]any{"event": "stun_start", "listen": address})
	buf := make([]byte, 513)
	window := time.Now()
	count := 0

	for {
		n, peer, e := conn.ReadFromUDP(buf)
		if e != nil {
			return e
		}

		if time.Since(window) >= time.Second {
			window = time.Now()
			count = 0
		}

		count++

		if count > 128 {
			continue
		}

		reply := stunBindingResponse(buf[:n], peer)

		if len(reply) == 0 {
			if count <= 4 {
				p.event(map[string]any{
					"event":         "stun_packet_rejected",
					"request_bytes": n,
				})
			}

			continue
		}

		if _, e = conn.WriteToUDP(reply, peer); e == nil {
			p.event(map[string]any{
				"event":          "stun_binding_response",
				"request_bytes":  n,
				"response_bytes": len(reply),
				"success":        bytes.Equal(reply[:2], []byte{1, 1}),
			})
		}
	}
}
