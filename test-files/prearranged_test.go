package main

import (
	"bytes"
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestPrearrangedPlayerIdentityAndTokens(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{8}, 32)
	s := &authServer{
		key: key,
		serverContext: &serverContext{
			writer: &logs,
		},
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))
	ext := &authpb.ExternalIdToken{
		Token: &authpb.ExternalIdToken_DummyExtIdToken{
			DummyExtIdToken: "private-player",
		},
	}
	names := map[string]bool{}

	for i := int32(0); i < 4; i++ {
		req := &authpb.IssuePrearrangedUserTokenRequest{
			Tenant:          "tenants/t-adf89f68-lp1",
			UserIndex:       i,
			ExternalIdToken: ext,
		}
		a, err := s.IssuePrearrangedUserToken(ctx, req)

		if err != nil {
			t.Fatal(err)
		}

		b, err := s.IssuePrearrangedUserToken(ctx, req)

		if err != nil || a.User.Name != b.User.Name || names[a.User.Name] || a.Token.User != a.User.Name || a.User.ShortId <= 0 {
			t.Fatal("invalid player identity", err)
		}

		names[a.User.Name] = true
		authCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", a.Token.AccessToken))

		if err = verifyAccess(authCtx, key); err != nil {
			t.Fatal(err)
		}
	}

	for _, i := range []int32{
		-1,
		4,
	} {
		_, err := s.IssuePrearrangedUserToken(ctx, &authpb.IssuePrearrangedUserTokenRequest{
			Tenant:          "tenants/current",
			UserIndex:       i,
			ExternalIdToken: ext,
		})

		if status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
	}

	_, err := s.IssuePrearrangedUserToken(ctx, &authpb.IssuePrearrangedUserTokenRequest{
		Tenant: "tenants/current",
	})

	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
}
