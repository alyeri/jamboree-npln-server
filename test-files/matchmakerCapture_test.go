package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestMatchmakerUnsupportedRequestCapture(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{9}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
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

	lis := bufconn.Listen(1 << 20)
	server := newServer(p)

	go server.Serve(lis)
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("guest", "access", time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	req := &mm.CreateMatchmakingTicketRequest{
		Parent: "tenants/current",
		MatchmakingTicket: &mm.MatchmakingTicket{
			MatchmakingConfig: "tenants/current/matchmakingConfigs/test",
		},
		UserDelegationTokens: []string{"PRIVATE_DELEGATION"},
	}
	_, err = mm.NewMatchmakerClient(conn).CreateMatchmakingTicket(ctx, req)

	if status.Code(err) != codes.Unimplemented {
		t.Fatal("handler changed", err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "capture-*.jsonl"))

	if len(files) != 1 {
		t.Fatal("capture absent")
	}

	data, _ := os.ReadFile(files[0])

	if bytes.Contains(data, []byte(token)) || bytes.Contains(data, []byte("PRIVATE_DELEGATION")) {
		t.Fatal("token leaked")
	}

	if !strings.Contains(string(data), "matchmaking_ticket") || !strings.Contains(string(data), "matchmakingConfigs/test") {
		t.Fatal("request not captured")
	}
}
