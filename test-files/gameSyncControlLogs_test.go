package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func TestControlDebugCorrelatesRejectedWriteWithoutSecrets(t *testing.T) {
	var output bytes.Buffer

	s := &gamesyncServer{
		p: &serverContext{
			writer: &output,
		},
	}
	fields := &common.MapValue{Fields: map[string]*common.Value{
		"cp": {
			ValueType: &common.Value_BooleanValue{
				BooleanValue: false,
			},
		},
		"prp": {ValueType: &common.Value_MapValue{MapValue: &common.MapValue{Fields: map[string]*common.Value{
			"_Bex_PropIsHost": textValue("private-player-name"),
			"searchWord":      textValue("private-room-code"),
			"gameMode":        intValue(-1),
		}}}},
		"pw": textValue("private-password"),
	}}
	op := &gs.WriteOperation{OperationType: &gs.WriteOperation_UpdateDocument{UpdateDocument: &gs.UpdateDocumentRequest{
		Document: &gs.Document{
			Name:   "docs/__gs/m",
			Fields: fields,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"`prp`", "`cp`"},
		},
	}}}
	ctx := context.WithValue(context.Background(), rpcKey{}, &rpcState{
		id: 42,
	})

	s.diagnosticControlWrite(ctx, "private-user|private-session", "private-room", "private-owner", "denied", op, nil)

	for _, secret := range []string{
		"private-player-name",
		"private-room-code",
		"private-password",
		"private-user",
		"private-session",
		"private-room",
		"private-owner",
	} {
		if strings.Contains(output.String(), secret) {
			t.Fatalf("debug leaked %q", secret)
		}
	}

	var event map[string]any

	if err := json.Unmarshal(output.Bytes(), &event); err != nil {
		t.Fatal(err)
	}

	if event["rpc_id"] != float64(42) || event["decision"] != "denied" || event["caller_digest"] != identityDigest("private-user") || event["room_digest"] != identityDigest("private-room") {
		t.Fatalf("missing correlation fields: %v", event)
	}

	if !strings.Contains(output.String(), "_Bex_PropIsHost") || !strings.Contains(output.String(), "gameMode") || !strings.Contains(output.String(), "-1") {
		t.Fatal("control shape missing")
	}
}
