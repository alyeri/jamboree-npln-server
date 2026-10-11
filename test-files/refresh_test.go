package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestRefreshTokenBindingAndExpiry(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{7}, 32)
	s := &authServer{
		key: key,
		serverContext: &serverContext{
			writer: &logs,
		},
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))
	user := "tenants/t-adf89f68-lp1/users/test"
	token, _ := s.signSessionToken(user, "refresh", time.Hour)
	r, err := s.RefreshToken(ctx, &authpb.RefreshTokenRequest{
		User:         "tenants/current/users/test",
		RefreshToken: token,
	})

	if err != nil || r.GetToken().GetTtl().GetSeconds() != 900 || r.GetToken().GetRefreshToken() == token {
		t.Fatal("refresh failed", err)
	}

	accessCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", r.Token.AccessToken))

	if err = verifyAccess(accessCtx, key); err != nil {
		t.Fatal(err)
	}

	_, err = s.RefreshToken(ctx, &authpb.RefreshTokenRequest{
		User:         user + "wrong",
		RefreshToken: token,
	})

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}

	for _, kind := range []string{"access", "refresh"} {
		ttl := time.Minute

		if kind == "refresh" {
			ttl = -time.Minute
		}

		bad, _ := s.signSessionToken(user, kind, ttl)
		_, err = s.RefreshToken(ctx, &authpb.RefreshTokenRequest{
			User:         user,
			RefreshToken: bad,
		})

		if status.Code(err) != codes.Unauthenticated {
			t.Fatal("invalid refresh accepted", err)
		}
	}

	_, err = s.RefreshToken(ctx, &authpb.RefreshTokenRequest{
		User:         user,
		RefreshToken: token + "x",
	})

	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}

	if bytes.Contains(logs.Bytes(), []byte(token)) {
		t.Fatal("refresh token logged")
	}
}
