package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func TestDeliveryDiagnosticsDistinguishFailureAndHideSecrets(t *testing.T) {
	var logs bytes.Buffer

	s := &gamesyncServer{
		p: &serverContext{
			writer: &logs,
		},
	}
	doc := &gs.Document{Name: "docs/__pgn/All/__stu/private-recipient", Fields: &common.MapValue{Fields: map[string]*common.Value{
		"pl": {
			ValueType: &common.Value_BytesValue{
				BytesValue: []byte("private-payload"),
			},
		},
	}}}
	sub := &gsSubscriber{
		proof: "private-token-binding",
	}

	for _, err := range []error{
		nil,
		status.Error(codes.Unavailable, "private-error"),
	} {
		logs.Reset()

		s.logDelivery(sub, doc, "UPDATED", err)

		var event map[string]any

		if json.Unmarshal(logs.Bytes(), &event) != nil || event["submitted"] != (err == nil) {
			t.Fatal("delivery outcome lost")
		}

		if err != nil && event["code"] != "Unavailable" {
			t.Fatal("failure code lost")
		}

		for _, secret := range []string{
			"private-recipient",
			"private-payload",
			"private-token-binding",
			"private-error",
		} {
			if strings.Contains(logs.String(), secret) {
				t.Fatal("delivery diagnostic exposed secret")
			}
		}
	}
}

func TestProtocolDiagnosticsHideCredentialsAndIdentity(t *testing.T) {
	fields := &common.MapValue{
		Fields: map[string]*common.Value{
			"uid":                  textValue("private-user-id"),
			"pw":                   textValue("private-password"),
			"sdp":                  textValue("a=ice-pwd:private-turn-password"),
			"private-field-secret": textValue("private-value"),
			"upcsid":               intValue(3),
		},
	}
	encoded, e := json.Marshal(summarizeFields(fields, 0))

	if e != nil {
		t.Fatal(e)
	}

	for _, secret := range []string{
		"private-user-id",
		"private-password",
		"private-turn-password",
		"private-field-secret",
		"private-value",
	} {
		if strings.Contains(string(encoded), secret) {
			t.Fatal("diagnostic exposed private payload")
		}
	}

	if !strings.Contains(string(encoded), `"value":3`) {
		t.Fatal("protocol integer omitted")
	}
}

func TestCommonPacketDiagnostics(t *testing.T) {
	p := make([]byte, 78)

	binary.BigEndian.PutUint32(p[:4], 0x32ab9864)

	p[4], p[5] = 0x8f, 0x22

	copy(p[29:], []byte("private-ciphertext"))

	got := summarizePIAHeader(p)

	if got["format"] != "common_packet" || got["encrypted"] != true || got["body_bytes"] != 48 || got["padding_bytes"] != byte(2) {
		t.Fatal("native common packet header mismatch", got)
	}

	encoded, _ := json.Marshal(got)

	if strings.Contains(string(encoded), "private") {
		t.Fatal("body exposed")
	}

	for n := 0; n < 30; n++ {
		if summarizePIAHeader(p[:n]) != nil {
			t.Fatal("short header accepted", n)
		}
	}

	p[4] = 0x8e

	if summarizePIAHeader(p) != nil {
		t.Fatal("unsupported version accepted")
	}
}

func TestPIAHeaderBoundsAndRedaction(t *testing.T) {
	packet := make([]byte, 78)
	packet[0] = 1
	packet[1] = 18
	packet[3] = 73
	packet[0x26] = 2

	copy(packet[45:], []byte("private-payload-credentials"))

	got := summarizePIAHeader(packet)

	if got["message_type"] != byte(18) {
		t.Fatal("native header layout not decoded", got)
	}

	encoded, _ := json.Marshal(got)

	if strings.Contains(string(encoded), "private") {
		t.Fatal("packet body leaked")
	}

	packet[3] = 255

	if summarizePIAHeader(packet) != nil {
		t.Fatal("inconsistent size accepted")
	}

	for n := 0; n < 5; n++ {
		if summarizePIAHeader(packet[:n]) != nil {
			t.Fatal("short packet accepted")
		}
	}
}
