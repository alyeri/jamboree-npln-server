package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/types/known/timestamppb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestPrivateRPCCapturePairsFieldsAndLimits(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{9}, 32)

	var logs bytes.Buffer

	p := &serverContext{
		identityKey: key,
		writer:      &logs,
		capture: &rpcCapture{
			dir: dir,
		},
	}
	arm := rpcCaptureArm{
		ExpiresAt: time.Now().Add(time.Minute),
		CallerDigests: []string{
			identityDigest("guest"),
		},
		MaxMessages: 3,
	}
	writeArm := func() {
		data, _ := json.Marshal(arm)

		if err := os.WriteFile(filepath.Join(dir, "arm.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}

	writeArm()

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("guest", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	ctx = p.TagRPC(ctx, &stats.RPCTagInfo{
		FullMethodName: "/nn.npln.matchmaking.v1.GameSessionService/JoinGameSession",
	})

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: &mm.JoinGameSessionRequest{
			Name:                "room-example",
			Password:            "PRIVATE_PASSWORD",
			IncludeIdTokenUsers: []string{"guest"},
		},
	})

	p.HandleRPC(ctx, &stats.OutPayload{
		Payload: &mm.JoinGameSessionResponse{
			GameSession: &mm.GameSession{
				Name:                    "room-example",
				CurrentParticipantCount: 2,
			},
			MatchedUserSessions: []*mm.MatchedUserSession{
				{
					MatchmakingIdToken: "PRIVATE_MATCH_TOKEN",
				},
			},
		},
	})

	p.HandleRPC(ctx, &stats.End{})

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: &mm.JoinGameSessionRequest{
			Name: "OVER_QUOTA",
		},
	})

	files, _ := filepath.Glob(filepath.Join(dir, "capture-*.jsonl"))

	if len(files) != 1 {
		t.Fatal("expected one capture")
	}

	data, _ := os.ReadFile(files[0])

	for _, secret := range []string{
		token,
		"PRIVATE_PASSWORD",
		"PRIVATE_MATCH_TOKEN",
		"OVER_QUOTA",
	} {
		if strings.Contains(string(data), secret) || strings.Contains(logs.String(), secret) {
			t.Fatal("secret or over-quota payload leaked")
		}
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	if len(lines) != 3 {
		t.Fatal("incorrect capture limit")
	}

	for index, line := range lines {
		var record map[string]any

		if json.Unmarshal([]byte(line), &record) != nil || record["rpc_id"] != float64(1) {
			t.Fatal("lost RPC correlation")
		}

		if index == 0 && record["fields"].(map[string]any)["name"] != "room-example" {
			t.Fatal("request fields missing")
		}

		if index == 1 && record["fields"].(map[string]any)["game_session"].(map[string]any)["current_participant_count"] != float64(2) {
			t.Fatal("response fields missing")
		}

		if index == 2 && record["grpc_status"] != "OK" {
			t.Fatal("status missing")
		}
	}

	arm.ExpiresAt = time.Now().Add(-time.Second)

	writeArm()

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: &mm.JoinGameSessionRequest{
			Name: "EXPIRED",
		},
	})

	after, _ := os.ReadFile(files[0])

	if !bytes.Equal(data, after) {
		t.Fatal("expired capture wrote data")
	}
}

func TestPrivateRPCCapturePreservesNonJSONWireTimestamp(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	p := &serverContext{
		identityKey: key,
		writer:      &logs,
		capture: &rpcCapture{
			dir: dir,
		},
	}
	arm, _ := json.Marshal(rpcCaptureArm{
		ExpiresAt: time.Now().Add(time.Minute),
		CallerDigests: []string{
			identityDigest("guest"),
		},
		MaxMessages: 2,
	})

	if err := os.WriteFile(filepath.Join(dir, "arm.json"), arm, 0600); err != nil {
		t.Fatal(err)
	}

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("guest|tenants/t-adf89f68-lp1/gameSessions/example/userSessions/guest", "gamesync-access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
	ctx = p.TagRPC(ctx, &stats.RPCTagInfo{
		FullMethodName: "/nn.npln.gamesync.v1.Gamesync/WriteDocuments",
	})
	stamp := &timestamppb.Timestamp{
		Seconds: 0,
		Nanos:   -1,
	}
	doc := &gs.Document{Name: "docs/example/guest", UpdateTime: stamp, Fields: &common.MapValue{Fields: map[string]*common.Value{
		"rs": {
			ValueType: &common.Value_BytesValue{
				BytesValue: []byte("PRIVATE_SEED"),
			},
		},
		"pw": textValue("PRIVATE_PASSWORD"),
	}}}
	request := &gs.WriteDocumentsRequest{
		WriteOperations: []*gs.WriteOperation{
			{
				OperationType: &gs.WriteOperation_UpdateDocument{
					UpdateDocument: &gs.UpdateDocumentRequest{
						Document: doc,
					},
				},
			},
		},
	}

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: request,
	})

	files, _ := filepath.Glob(filepath.Join(dir, "capture-*.jsonl"))

	if len(files) != 1 {
		t.Fatal("wire request silently omitted")
	}

	data, _ := os.ReadFile(files[0])

	var record map[string]any

	if err := json.Unmarshal(bytes.TrimSpace(data), &record); err != nil {
		t.Fatal(err)
	}

	if record["serialization_mode"] != "protobuf_fields" {
		t.Fatal("missing fallback provenance")
	}

	if !strings.Contains(string(data), `"nanos":-1`) || strings.Contains(string(data), "PRIVATE_PASSWORD") || strings.Contains(string(data), "UFJJVkFURV9TRUVE") {
		t.Fatal("wire fields missing or secrets leaked")
	}

	if doc.UpdateTime.Nanos != -1 || doc.Fields.Fields["pw"].GetStringValue() != "PRIVATE_PASSWORD" {
		t.Fatal("capture altered handler input")
	}
}

func TestPrivateRPCCaptureGamesyncIdentityAndBinaryRedaction(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{8}, 32)

	var logs bytes.Buffer

	p := &serverContext{
		identityKey: key,
		writer:      &logs,
		capture: &rpcCapture{
			dir: dir,
		},
	}
	arm, _ := json.Marshal(rpcCaptureArm{
		ExpiresAt: time.Now().Add(time.Minute),
		CallerDigests: []string{
			identityDigest("guest"),
		},
		MaxMessages: 4,
	})

	if err := os.WriteFile(filepath.Join(dir, "arm.json"), arm, 0600); err != nil {
		t.Fatal(err)
	}

	proof := "guest|tenants/t-adf89f68-lp1/gameSessions/example/userSessions/guest"
	token, _ := (&authServer{
		key: key,
	}).signSessionToken(proof, "gamesync-access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	method := "/nn.npln.gamesync.v1.Gamesync/GetDocument"
	ctx = p.TagRPC(ctx, &stats.RPCTagInfo{
		FullMethodName: method,
	})

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: &gs.GetDocumentRequest{
			Name: "docs/test/guest",
		},
	})

	p.HandleRPC(ctx, &stats.OutPayload{Payload: &gs.Document{Name: "docs/test/guest", Fields: &common.MapValue{Fields: map[string]*common.Value{
		"cm": {
			ValueType: &common.Value_ArrayValue{
				ArrayValue: &common.ArrayValue{},
			},
		},
		"rs": {
			ValueType: &common.Value_BytesValue{
				BytesValue: []byte("PRIVATE_SEED"),
			},
		},
	}}}})

	files, _ := filepath.Glob(filepath.Join(dir, "capture-*.jsonl"))

	if len(files) != 1 {
		t.Fatal("gamesync caller was not captured")
	}

	data, _ := os.ReadFile(files[0])

	if strings.Contains(string(data), token) || strings.Contains(string(data), "UFJJVkFURV9TRUVE") {
		t.Fatal("binary or authorization leaked")
	}

	if !strings.Contains(string(data), `"bytes_value":"\u003credacted\u003e"`) || !strings.Contains(string(data), `"cm"`) {
		t.Fatal("redaction lost application document shape")
	}

	wrong, _ := (&authServer{
		key: key,
	}).signSessionToken(proof, "access", time.Minute)
	bad := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", wrong))

	if _, err := captureCaller(bad, method, key); err == nil {
		t.Fatal("wrong-purpose token authorized capture")
	}

	other, _ := (&authServer{
		key: key,
	}).signSessionToken("other|tenants/t-adf89f68-lp1/gameSessions/example/userSessions/other", "gamesync-access", time.Minute)
	bad = metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", other))
	bad = p.TagRPC(bad, &stats.RPCTagInfo{
		FullMethodName: method,
	})

	p.HandleRPC(bad, &stats.InPayload{
		Payload: &gs.GetDocumentRequest{
			Name: "OTHER_DOCUMENT",
		},
	})

	after, _ := os.ReadFile(files[0])

	if !bytes.Equal(data, after) {
		t.Fatal("other gamesync caller captured")
	}
}

func TestPrivateRPCCaptureRejectsOtherCallerAndAuth(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{4}, 32)

	var logs bytes.Buffer

	p := &serverContext{
		identityKey: key,
		writer:      &logs,
		capture: &rpcCapture{
			dir: dir,
		},
	}
	arm := rpcCaptureArm{
		ExpiresAt: time.Now().Add(time.Minute),
		CallerDigests: []string{
			identityDigest("guest"),
		},
		MaxMessages: 3,
	}
	data, _ := json.Marshal(arm)

	if err := os.WriteFile(filepath.Join(dir, "arm.json"), data, 0600); err != nil {
		t.Fatal(err)
	}

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("other-client", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	ctx = p.TagRPC(ctx, &stats.RPCTagInfo{
		FullMethodName: "/nn.npln.matchmaking.v1.GameSessionService/JoinGameSession",
	})

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: &mm.JoinGameSessionRequest{
			Name: "OTHER_PRIVATE_ROOM",
		},
	})

	if captureMethod("/nn.npln.auth.v1.Auth/IssueToken") {
		t.Fatal("authentication must be excluded")
	}

	files, _ := filepath.Glob(filepath.Join(dir, "capture-*.jsonl"))

	if len(files) != 0 {
		t.Fatal("other caller captured")
	}
}

func TestPrivateRPCCaptureDiscoveryAndCreationBoundary(t *testing.T) {
	for _, method := range []string{
		"/nn.npln.matchmaking.v1.GameSessionService/CreateGameSessionCreationTicket",
		"/nn.npln.matchmaking.v1.GameSessionService/TrackGameSessionCreationTicket",
		"/nn.npln.matchmaking.v1.GameSessionService/BatchGetGameSessions",
		"/nn.npln.friends.v1.Friends/SubscribeFriendUsers",
		"/nn.npln.friends.v1.Friends/ListFriendUsers",
	} {
		if !captureMethod(method) {
			t.Fatalf("discovery capture missing: %s", method)
		}
	}

	if captureMethod("/nn.npln.gamesync.v1.Gamesync/KeepUserSession") {
		t.Fatal("bulk participant traffic must stay outside discovery capture")
	}
}
