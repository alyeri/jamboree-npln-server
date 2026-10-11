package main

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"net"
	"testing"
)

func TestSTUNBindingIPv4IPv6AndValidation(t *testing.T) {
	req := make([]byte, 20)

	binary.BigEndian.PutUint16(req, 1)

	binary.BigEndian.PutUint32(req[4:], stunCookie)

	copy(req[8:], []byte("test-transid"))

	for _, host := range []string{"198.51.100.7", "2001:db8::7"} {
		peer := &net.UDPAddr{
			IP:   net.ParseIP(host),
			Port: 23456,
		}
		reply := stunBindingResponse(req, peer)

		if len(reply) == 0 || binary.BigEndian.Uint16(reply) != 0x0101 || !bytes.Equal(reply[8:20], req[8:20]) {
			t.Fatal("bad binding response")
		}

		if int(binary.BigEndian.Uint16(reply[2:])) != len(reply)-20 {
			t.Fatal("bad message length")
		}

		if binary.BigEndian.Uint16(reply[26:])^uint16(stunCookie>>16) != uint16(peer.Port) {
			t.Fatal("bad mapped port")
		}

		size := 4
		ip := peer.IP.To4()

		if ip == nil {
			size = 16
			ip = peer.IP.To16()
		}

		for i := 0; i < size; i++ {
			if reply[28+i]^req[4+i] != ip[i] {
				t.Fatal("bad mapped IP")
			}
		}

		fp := len(reply) - 8

		if binary.BigEndian.Uint32(reply[fp+4:]) != crc32.ChecksumIEEE(reply[:fp])^0x5354554e {
			t.Fatal("bad response fingerprint")
		}
	}

	malformed := append([]byte{}, req...)
	malformed[2] = 1

	if stunBindingResponse(malformed, &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 1234,
	}) != nil {
		t.Fatal("accepted invalid length")
	}

	unknown := append(append([]byte{}, req...), []byte{
		0x12,
		0x34,
		0,
		0,
	}...)

	binary.BigEndian.PutUint16(unknown[2:], 4)

	r := stunBindingResponse(unknown, &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 1234,
	})

	if binary.BigEndian.Uint16(r) != 0x0111 {
		t.Fatal("unknown mandatory attribute accepted")
	}
}
