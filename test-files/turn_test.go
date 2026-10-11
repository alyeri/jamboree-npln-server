package main

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/pion/turn/v4"
)

func TestJamboreeTURNAllocationAndRelayRoundTrip(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{7}, 32)
	username, password, err := newTURNCredentials(key)

	if err != nil {
		t.Fatal(err)
	}

	server, address, err := startJamboreeTURN(&serverContext{
		writer:      &logs,
		identityKey: key,
	}, "127.0.0.1:0", "127.0.0.1", true)

	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	clientConn, err := net.ListenPacket("udp4", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}

	client, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: address.String(),
		TURNServerAddr: address.String(),
		Conn:           clientConn,
		Username:       username,
		Password:       password,
		Realm:          jamboreeTURNRealm,
	})

	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer clientConn.Close()

	if err = client.Listen(); err != nil {
		t.Fatal(err)
	}

	relay, err := client.Allocate()

	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()

	peer, err := net.ListenPacket("udp4", "127.0.0.1:0")

	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	go func() {
		buffer := make([]byte, 32)
		n, from, readErr := peer.ReadFrom(buffer)

		if readErr == nil {
			_, _ = peer.WriteTo(append([]byte("reply:"), buffer[:n]...), from)
		}
	}()

	if _, err = relay.WriteTo([]byte("wonder"), peer.LocalAddr()); err != nil {
		t.Fatal(err)
	}

	if err = relay.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}

	buffer := make([]byte, 32)
	n, _, err := relay.ReadFrom(buffer)

	if err != nil {
		t.Fatal(err)
	}

	if got := string(buffer[:n]); got != "reply:wonder" {
		t.Fatalf("relayed response = %q", got)
	}

	if bytes.Contains(logs.Bytes(), []byte(password)) {
		t.Fatal("TURN password logged")
	}
}

func TestTURNCredentialExpiry(t *testing.T) {
	if validTURNUsername("1:0123456789abcdef") || validTURNUsername("9999999999:0123456789abcdef") || validTURNUsername("invalid") {
		t.Fatal("invalid TURN lifetime accepted")
	}

	key := bytes.Repeat([]byte{7}, 32)
	u, p, e := newTURNCredentials(key)

	if e != nil || !validTURNUsername(u) || p != turnPassword(u, key) || p == turnPassword(u, bytes.Repeat([]byte{8}, 32)) {
		t.Fatal("TURN credential binding failed")
	}
}
