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
	"google.golang.org/protobuf/encoding/protowire"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func TestPrivateCaptureIncludesSubscriptionPathsWithoutBulkTraffic(t *testing.T) {
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
	method := "/nn.npln.gamesync.v1.Gamesync/KeepUserSession"
	arm, err := json.Marshal(rpcCaptureArm{
		ExpiresAt: time.Now().Add(time.Minute),
		CallerDigests: []string{
			identityDigest("guest"),
		},
		MaxMessages: 4,
		Methods: []string{
			method,
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(dir, "arm.json"), arm, 0600); err != nil {
		t.Fatal(err)
	}

	proof := "guest|tenants/t-adf89f68-lp1/gameSessions/example/userSessions/guest"
	token, err := (&authServer{
		key: key,
	}).signSessionToken(proof, "gamesync-access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	ctx = p.TagRPC(ctx, &stats.RPCTagInfo{
		FullMethodName: method,
	})
	path := "docs/example_collection/start_data"
	request := &gs.KeepUserSessionRequest{
		RequestType: &gs.KeepUserSessionRequest_UpdateTarget{
			UpdateTarget: &gs.UpdateTargetRequest{
				Target: &gs.Target{
					Name: "targets/example",
					TargetType: &gs.Target_Documents{
						Documents: &gs.DocumentsTarget{
							Documents: []string{
								path,
							},
						},
					},
				},
			},
		},
	}

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: request,
	})

	files, err := filepath.Glob(filepath.Join(dir, "capture-*.jsonl"))

	if err != nil || len(files) != 1 {
		t.Fatal("subscription was not captured", err)
	}

	data, err := os.ReadFile(files[0])

	if err != nil || !strings.Contains(string(data), path) || strings.Contains(string(data), token) {
		t.Fatal("unsafe or missing subscription path", err)
	}

	p.HandleRPC(ctx, &stats.InPayload{
		Payload: &gs.KeepUserSessionRequest{},
	})

	p.HandleRPC(ctx, &stats.OutPayload{
		Payload: &gs.KeepUserSessionResponse{},
	})

	p.HandleRPC(ctx, &stats.End{})

	after, err := os.ReadFile(files[0])

	if err != nil || !bytes.Equal(data, after) {
		t.Fatal("bulk stream traffic captured", err)
	}
}

func TestAppconfigWireIdentifiesOnlyKnownNetworkSelector(t *testing.T) {
	for _, selector := range []string{"Network", "PRIVATE_UNKNOWN_SELECTOR"} {
		wire := protowire.AppendTag(nil, 2, protowire.BytesType)
		wire = protowire.AppendString(wire, selector)
		fields, valid := appconfigWireShape(wire, 0)

		if !valid || len(fields) != 1 {
			t.Fatal("selector shape missing")
		}

		if selector == "Network" && fields[0]["selector"] != "Network" {
			t.Fatal("known selector unidentified")
		}

		encoded, err := json.Marshal(fields)

		if err != nil {
			t.Fatal(err)
		}

		if selector != "Network" && bytes.Contains(encoded, []byte(selector)) {
			t.Fatal("unknown selector disclosed")
		}
	}
}
