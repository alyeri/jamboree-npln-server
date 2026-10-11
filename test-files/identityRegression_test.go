package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestRotatingBaasSubjectKeepsDiagnosticAccount(t *testing.T) {
	var logs bytes.Buffer

	s := &authServer{
		key: bytes.Repeat([]byte{7}, 32),
		serverContext: &serverContext{
			writer: &logs,
		},
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))
	ext := func(sub, pid string) *authpb.ExternalIdToken {
		nnex := "nx2." + base64.RawURLEncoding.EncodeToString([]byte(pid+".fixture.9999999999")) + ".unverified"
		raw := fmt.Sprintf(`{"sub":%q,"nnex":%q}`, sub, nnex)

		return &authpb.ExternalIdToken{
			Token: &authpb.ExternalIdToken_NsaIdToken{
				NsaIdToken: "header." + base64.RawURLEncoding.EncodeToString([]byte(raw)) + ".unverified",
			},
		}
	}
	a, err := s.CreateUser(ctx, &authpb.CreateUserRequest{
		Parent:          "tenants/t-adf89f68-lp1",
		ExternalIdToken: ext("random-one", "42"),
	})

	if err != nil {
		t.Fatal(err)
	}

	b, err := s.CreateUser(ctx, &authpb.CreateUserRequest{
		Parent:          "tenants/t-adf89f68-lp1",
		ExternalIdToken: ext("random-two", "42"),
	})

	if err != nil || a.Name != b.Name {
		t.Fatal("rotating subject changed identity", err)
	}

	_, err = s.IssueToken(ctx, &authpb.IssueTokenRequest{
		User:            a.Name,
		ExternalIdToken: ext("random-three", "42"),
	})

	if err != nil {
		t.Fatal(err)
	}

	_, err = s.IssueToken(ctx, &authpb.IssueTokenRequest{
		User:            a.Name,
		ExternalIdToken: ext("random-four", "43"),
	})

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("unrelated account must not receive token", err)
	}

	cut := len("tenants/t-adf89f68-lp1/users/")
	truncated := a.Name[:cut] + "u-" + a.Name[cut:len(a.Name)-1]
	_, err = s.IssueToken(ctx, &authpb.IssueTokenRequest{
		User:            truncated,
		ExternalIdToken: ext("random-five", "42"),
	})

	if status.Code(err) != codes.NotFound {
		t.Fatal("truncated legacy cache did not recover", err)
	}
}
