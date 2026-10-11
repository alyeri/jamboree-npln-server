package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	fr "github.com/alyeri/jamboree-npln-server/proto/friends/v1"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func TestGSListPaginationMaskAndQueryBinding(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	room := "tenants/t-adf89f68-lp1/gameSessions/test"
	token, _ := (&authServer{
		key: key,
	}).signSessionToken("owner|"+room+"/userSessions/host", "gamesync-access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
	s := &gamesyncServer{
		p: &serverContext{
			writer:      &logs,
			identityKey: key,
		},
		documents: map[string]map[string]*gs.Document{
			room: {
				"docs/c/a": {
					Name: "docs/c/a",
					Fields: &common.MapValue{
						Fields: map[string]*common.Value{
							"x": intValue(1),
							"y": intValue(2),
						},
					},
				},
				"docs/c/b": {
					Name: "docs/c/b",
				},
				"docs/c/missing/nested/d": {
					Name: "docs/c/missing/nested/d",
				},
			},
		},
	}
	r, e := s.ListDocuments(ctx, &gs.ListDocumentsRequest{
		Parent:   "docs/c",
		PageSize: 1,
		ReadMask: &fieldmaskpb.FieldMask{
			Paths: []string{"x"},
		},
	})

	if e != nil || len(r.Documents) != 1 || r.Documents[0].Name != "docs/c/a" || r.NextPageToken == "" || len(r.Documents[0].GetFields().GetFields()) != 1 {
		t.Fatal("list mask or pagination failed", e)
	}

	next, e := s.ListDocuments(ctx, &gs.ListDocumentsRequest{
		Parent:    "docs/c",
		PageToken: r.NextPageToken,
	})

	if e != nil || len(next.Documents) != 1 || next.Documents[0].Name != "docs/c/b" {
		t.Fatal("second page failed", e)
	}

	_, e = s.ListDocuments(ctx, &gs.ListDocumentsRequest{
		Parent:    "docs/other",
		PageToken: r.NextPageToken,
	})

	if status.Code(e) != codes.InvalidArgument {
		t.Fatal("cross-query token accepted", e)
	}

	missing, e := s.ListDocuments(ctx, &gs.ListDocumentsRequest{
		Parent:      "docs/c",
		ShowMissing: true,
	})

	if e != nil || len(missing.Documents) != 3 {
		t.Fatal("missing ancestors failed", e)
	}
}
func TestFriendsActivationOwnership(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/t-adf89f68-lp1/users/host", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	s := &friendsServer{
		p: &serverContext{
			writer:      &logs,
			identityKey: key,
		},
	}
	_, e := s.ActivateUser(ctx, &fr.ActivateUserRequest{
		Name: "tenants/current/users/current",
	})

	if e != nil || len(s.active) != 1 {
		t.Fatal("activation not stored", e)
	}

	_, e = s.ActivateUser(ctx, &fr.ActivateUserRequest{
		Name: "tenants/current/users/other",
	})

	if status.Code(e) != codes.PermissionDenied {
		t.Fatal("foreign activation accepted", e)
	}
}
