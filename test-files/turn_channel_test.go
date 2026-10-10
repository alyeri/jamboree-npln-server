package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

func TestTURNChannelConnectionTestWireLength(t *testing.T) {
	sender, err := net.ListenPacket("udp4", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	defer sender.Close()

	receiver, err := net.ListenPacket("udp4", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	defer receiver.Close()

	if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte{0x5a}, 41)
	packet := make([]byte, 48)
	binary.BigEndian.PutUint16(packet[:2], 0x4001)
	binary.BigEndian.PutUint16(packet[2:4], 41)
	copy(packet[4:], payload)
	original := bytes.Clone(packet)
	var logs bytes.Buffer
	conn := &observedTURNConn{
		PacketConn: sender,
		p:          &serverContext{writer: &logs},
	}

	if n, err := conn.WriteTo(packet, receiver.LocalAddr()); err != nil || n != len(packet) {
		t.Fatal("packet write failed", n, err)
	}

	buffer := make([]byte, 128)
	n, _, err := receiver.ReadFrom(buffer)

	if err != nil || n != 45 {
		t.Fatal("native connection test requires exactly 45 UDP bytes", n, err)
	}

	if !bytes.Equal(buffer[:4], original[:4]) || !bytes.Equal(buffer[4:n], payload) || !bytes.Equal(packet, original) {
		t.Fatal("channel header, payload or input changed")
	}
}

func TestTURNChannelPaddingBoundaries(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 4, 41, 1501} {
		packet := make([]byte, (4+size+3)&^3)
		binary.BigEndian.PutUint16(packet[:2], 0x4000)
		binary.BigEndian.PutUint16(packet[2:4], uint16(size))

		if len(unpaddedTURNChannelData(packet)) != size+4 {
			t.Fatal("payload length changed", size)
		}
	}

	for _, packet := range [][]byte{
		{0x40},
		{0x40, 0, 0, 8, 1},
		// truncated payload
		{0x80, 0, 0, 1, 1, 0, 0, 0},
		// invalid channel
		{0x40, 0, 0, 1, 1, 0, 0, 0, 0},
		// extra data beyond padding
		{0, 1, 0, 1, 1, 0, 0, 0},
		// STUN, not ChannelData
		{0x40, 0, 0, 1, 1},
		// already unpadded
	} {
		if !bytes.Equal(unpaddedTURNChannelData(packet), packet) {
			t.Fatal("non-padded or malformed datagram changed")
		}
	}
}
