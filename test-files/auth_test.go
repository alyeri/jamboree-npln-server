package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestCreateUserWireAndIdentity(t *testing.T) {
	var output bytes.Buffer

	p := &serverContext{
		writer:      &output,
		identityKey: bytes.Repeat([]byte{7}, 32),
	}
	lis := bufconn.Listen(1 << 20)
	s := newServer(p)

	go s.Serve(lis)
	defer s.Stop()

	c, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx = metadata.AppendToOutgoingContext(ctx, "npln-tenant-id", "t-adf89f68-lp1")
	client := authpb.NewAuthClient(c)
	request := func(subject string) *authpb.CreateUserRequest {
		token := "header." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"`+subject+`"}`)) + "SECRET_SIGNATURE"

		return &authpb.CreateUserRequest{
			Parent: "tenants/t-adf89f68-lp1",
			ExternalIdToken: &authpb.ExternalIdToken{
				Token: &authpb.ExternalIdToken_NsaIdToken{
					NsaIdToken: token,
				},
			},
		}
	}
	// Three JWT segments are required, without treating the signature as verified.
	req := request("PRIVATE_SUBJECT")
	req.ExternalIdToken.Token = &authpb.ExternalIdToken_NsaIdToken{
		NsaIdToken: "header." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"PRIVATE_SUBJECT"}`)) + ".SECRET_SIGNATURE",
	}
	a, err := client.CreateUser(ctx, req)

	if err != nil {
		t.Fatal(err)
	}

	b, err := client.CreateUser(ctx, req)

	if err != nil {
		t.Fatal(err)
	}

	if a.Name != b.Name || a.ShortId != b.ShortId || a.ShortId <= 0 || a.Account != req.Parent || !strings.HasPrefix(a.Name, req.Parent+"/users/") || len(strings.TrimPrefix(a.Name, req.Parent+"/users/")) != 32 {
		t.Fatalf("invalid or unstable identity")
	}

	other := &authpb.CreateUserRequest{
		Parent: req.Parent,
		ExternalIdToken: &authpb.ExternalIdToken{
			Token: &authpb.ExternalIdToken_DummyExtIdToken{
				DummyExtIdToken: "different",
			},
		},
	}
	u, err := client.CreateUser(ctx, other)

	if err != nil || u.Name == a.Name {
		t.Fatal("identities collide", err)
	}

	_, err = client.CreateUser(ctx, &authpb.CreateUserRequest{
		Parent: req.Parent,
	})

	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}

	wrong := metadata.NewOutgoingContext(ctx, metadata.Pairs("npln-tenant-id", "other"))
	_, err = client.CreateUser(wrong, req)

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}

	req.Parent = "tenants/other"
	_, err = client.CreateUser(ctx, req)

	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}

	_, err = client.RefreshToken(ctx, &authpb.RefreshTokenRequest{})

	if status.Code(err) != codes.Unauthenticated {
		t.Fatal("missing refresh token must be rejected", err)
	}

	s.Stop()

	p.mu.Lock()

	logs := output.String()

	p.mu.Unlock()

	for _, secret := range []string{"PRIVATE_SUBJECT", "SECRET_SIGNATURE"} {
		if strings.Contains(logs, secret) {
			t.Fatal("identity/token leaked")
		}
	}
}
