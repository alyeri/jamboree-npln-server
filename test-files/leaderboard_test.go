package main

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

func TestLeaderboardDefaultHolderPersistenceOwnershipAndMasks(t *testing.T) {
	key := bytes.Repeat([]byte{8}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	path := filepath.Join(t.TempDir(), "holders.json")
	s, e := newLeaderboardServer(p, path)

	if e != nil {
		t.Fatal(e)
	}

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()

	lb.RegisterLeaderboardServiceServer(server, s)

	go server.Serve(listener)
	defer server.Stop()

	conn, e := grpc.NewClient("passthrough:///rankings", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))

	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()

	client := lb.NewLeaderboardServiceClient(conn)
	ctxFor := func(owner string) context.Context {
		token, e := (&authServer{
			key: key,
		}).signSessionToken(owner, "access", time.Minute)

		if e != nil {
			t.Fatal(e)
		}

		return metadata.NewOutgoingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	}
	a, b := ctxFor("tenants/current/users/a"), ctxFor("tenants/current/users/b")
	req := &lb.UpdateHolderRequest{
		Holder: &lb.Holder{
			Name:        "tenants/current/holders/default",
			DisplayName: "A",
			Type:        7,
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"display_name", "type"},
		},
	}
	h, e := client.UpdateHolder(a, req)

	if e != nil {
		t.Fatal(e)
	}

	if h.Owner != "tenants/t-adf89f68-lp1/users/a" || h.CreateTime == nil || h.UpdateTime == nil || h.DisplayName != "A" {
		t.Fatal("wrong holder result")
	}

	hb, e := client.UpdateHolder(b, req)

	if e != nil || hb.GetName() == h.Name {
		t.Fatal("default alias shared across callers", e)
	}

	_, e = client.UpdateHolder(b, &lb.UpdateHolderRequest{
		Holder: &lb.Holder{
			Name:        h.Name,
			DisplayName: "intruder",
		},
	})

	if status.Code(e) != codes.PermissionDenied {
		t.Fatal("cross-account update allowed", e)
	}

	_, e = client.UpdateHolder(a, &lb.UpdateHolderRequest{
		Holder: &lb.Holder{
			Name:        h.Name,
			DisplayName: "partial",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"display_name", "owner"},
		},
	})

	if status.Code(e) != codes.InvalidArgument {
		t.Fatal(e)
	}

	req.Holder.Type = 99
	req.Holder.DisplayName = "updated"
	req.UpdateMask.Paths = []string{"display_name"}
	updated, e := client.UpdateHolder(a, req)

	if e != nil || updated.GetType() != 7 || updated.GetDisplayName() != "updated" || !updated.CreateTime.AsTime().Equal(h.CreateTime.AsTime()) {
		t.Fatal("mask or timestamps invalid", e)
	}

	loaded, e := newLeaderboardServer(p, path)

	if e != nil {
		t.Fatal(e)
	}

	if loaded.holders[h.Name].DisplayName != "updated" || len(loaded.holders) != 2 {
		t.Fatal("holder persistence failed")
	}

	_, e = client.UpdateHolder(context.Background(), req)

	if status.Code(e) != codes.PermissionDenied {
		t.Fatal("unauthenticated request accepted", e)
	}

	_, e = client.UpdateHolder(a, &lb.UpdateHolderRequest{
		Holder: &lb.Holder{
			Name: "tenants/other/holders/default",
		},
	})

	if status.Code(e) != codes.InvalidArgument {
		t.Fatal("other tenant accepted", e)
	}
}

func TestLeaderboardPersistenceFailureDoesNotCommit(t *testing.T) {
	key := bytes.Repeat([]byte{8}, 32)
	s, _ := newLeaderboardServer(&serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}, "")
	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/a", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	s.path = filepath.Join(t.TempDir(), "missing", "holders.json")
	_, e := s.UpdateHolder(ctx, &lb.UpdateHolderRequest{
		Holder: &lb.Holder{
			Name: "tenants/current/holders/default",
		},
	})

	if status.Code(e) != codes.Internal || len(s.holders) != 0 {
		t.Fatal("failed write committed", e)
	}
}
