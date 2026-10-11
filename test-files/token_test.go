package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestIssueTokenBindingAndSignature(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{9}, 32)
	s := &authServer{
		serverContext: &serverContext{
			writer: &logs,
		},
		key: key,
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))
	ext := &authpb.ExternalIdToken{
		Token: &authpb.ExternalIdToken_DummyExtIdToken{
			DummyExtIdToken: "PRIVATE_IDENTITY",
		},
	}
	u, err := s.CreateUser(ctx, &authpb.CreateUserRequest{
		Parent:          "tenants/t-adf89f68-lp1",
		ExternalIdToken: ext,
	})

	if err != nil {
		t.Fatal(err)
	}

	req := &authpb.IssueTokenRequest{
		User:            u.Name,
		ExternalIdToken: ext,
	}
	r, err := s.IssueToken(ctx, req)

	if err != nil {
		t.Fatal(err)
	}

	if r.Token.User != u.Name || r.Token.Ttl.AsDuration() != 15*time.Minute || r.Token.AccessToken == r.Token.RefreshToken {
		t.Fatal("invalid response")
	}

	for kind, tok := range map[string]string{
		"access":  r.Token.AccessToken,
		"refresh": r.Token.RefreshToken,
	} {
		p := strings.Split(tok, ".")

		if len(p) != 3 {
			t.Fatal("not JWT")
		}

		signature, e := base64.RawURLEncoding.DecodeString(p[2])

		if e != nil {
			t.Fatal(e)
		}

		mac := hmac.New(sha256.New, key)

		mac.Write([]byte(p[0] + "." + p[1]))

		if !hmac.Equal(signature, mac.Sum(nil)) {
			t.Fatal("bad signature")
		}

		payload, _ := base64.RawURLEncoding.DecodeString(p[1])

		var claims map[string]any

		if json.Unmarshal(payload, &claims) != nil || claims["sub"] != u.Name || claims["kind"] != kind || claims["aud"] != "jamboree-diagnostic-only" || claims["identity_verified"] != false || claims["exp"].(float64) <= float64(time.Now().Unix()) {
			t.Fatal("bad claims")
		}

		if strings.Contains(logs.String(), tok) {
			t.Fatal("token leaked")
		}
	}

	req.User = u.Name + "wrong"
	canonical := req.User
	req.User = u.Name + "\x00"
	normalized, normalizeErr := s.IssueToken(ctx, req)

	if normalizeErr != nil || normalized.GetToken().GetUser() != u.Name {
		t.Fatal("NUL-terminated user resource not normalized", normalizeErr)
	}

	req.User = canonical
	_, err = s.IssueToken(ctx, req)

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}

	req.User = "tenants/t-adf89f68-lp1/users/" + strconv.FormatInt(u.ShortId, 10)
	alias, aliasErr := s.IssueToken(ctx, req)

	if aliasErr != nil || alias.GetToken().GetUser() != req.User {
		t.Fatal("cached short ID rejected", aliasErr)
	}

	req.User = "tenants/t-adf89f68-lp1/users/" + strconv.FormatInt(u.ShortId+1, 10)
	_, err = s.IssueToken(ctx, req)

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("foreign short ID accepted")
	}

	req.User = u.Name
	req.ExternalIdToken = nil
	_, err = s.IssueToken(ctx, req)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}

	if strings.Contains(logs.String(), "PRIVATE_IDENTITY") {
		t.Fatal("identity leaked")
	}
}
