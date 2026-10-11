package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestNativeCachedUserRequiresFreshRegistration(t *testing.T) {
	parent := "tenants/t-adf89f68-lp1"
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))
	key := bytes.Repeat([]byte{7}, 32)
	server := &authServer{
		serverContext: &serverContext{
			writer:      &bytes.Buffer{},
			identityKey: key,
		},
		key: key,
	}
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"cache-migration-test"}`))
	external := &authpb.ExternalIdToken{
		Token: &authpb.ExternalIdToken_NsaIdToken{
			NsaIdToken: "header." + payload + ".signature",
		},
	}

	for _, id := range []string{"abcdefghijklmnopqrstuv", "u-0123456789abcdef0123"} {
		response, err := server.IssueToken(ctx, &authpb.IssueTokenRequest{
			User:            parent + "/users/" + id,
			ExternalIdToken: external,
		})

		if response != nil || status.Code(err) != codes.NotFound {
			t.Fatalf("stale native cache must return NotFound without a token: %v", err)
		}
	}

	user, err := server.CreateUser(ctx, &authpb.CreateUserRequest{
		Parent:          parent,
		ExternalIdToken: external,
	})

	if err != nil {
		t.Fatal(err)
	}

	response, err := server.IssueToken(ctx, &authpb.IssueTokenRequest{
		User:            user.Name,
		ExternalIdToken: external,
	})

	if err != nil || response == nil {
		t.Fatalf("registration must restore authentication: %v", err)
	}

	for _, id := range []string{"0123456789abcdef0123456789abcdef", "abcdefghijklmnopqrstu!"} {
		response, err := server.IssueToken(ctx, &authpb.IssueTokenRequest{
			User:            parent + "/users/" + id,
			ExternalIdToken: external,
		})

		if response != nil || status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unrelated or malformed user must remain rejected: %v", err)
		}
	}
}
