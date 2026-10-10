package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func paddedPrearrangedRequest(token []byte, tenant string) []byte {
	nested := protowire.AppendTag(nil, 1, protowire.BytesType)
	nested = protowire.AppendBytes(nested, token)
	request := protowire.AppendTag(nil, 1, protowire.BytesType)
	request = protowire.AppendString(request, tenant)
	request = protowire.AppendTag(request, 2, protowire.VarintType)
	request = protowire.AppendVarint(request, 2)
	request = protowire.AppendTag(request, 3, protowire.BytesType)

	return protowire.AppendBytes(request, nested)
}

func TestPrearrangedPaddingMatchesEntireAcceptedToken(t *testing.T) {
	var logs bytes.Buffer
	state := &serverContext{writer: &logs}
	codec := &prearrangedDiagnosticCodec{p: state}
	auth := &authServer{key: bytes.Repeat([]byte{7}, 32), state: state}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))
	token := "header." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"padding-test"}`)) + ".signature"
	request := &authpb.CreateUserRequest{
		Parent: "tenants/current",
		ExternalIdToken: &authpb.ExternalIdToken{
			Token: &authpb.ExternalIdToken_NsaIdToken{NsaIdToken: token},
		},
	}
	if _, err := auth.CreateUser(ctx, request); err != nil {
		t.Fatal(err)
	}

	// Include an ASCII byte before invalid UTF-8: trimming only to the ASCII
	// prefix would silently change the JWT signature rather than recover it.
	padded := append([]byte(token), 'A', 0xff, 0xfe)
	wire := paddedPrearrangedRequest(padded, "tenants/current")
	decoded := &authpb.IssuePrearrangedUserTokenRequest{}
	if err := codec.Unmarshal(wire, decoded); err != nil {
		t.Fatal("accepted token with trailing garbage rejected", err)
	}
	if decoded.GetExternalIdToken().GetNsaIdToken() != token || decoded.UserIndex != 2 {
		t.Fatal("credential or player index changed")
	}
	if _, err := auth.IssuePrearrangedUserToken(ctx, decoded); err != nil {
		t.Fatal("normal identity handling rejected recovered token", err)
	}
	if bytes.Contains(logs.Bytes(), []byte(token)) || !bytes.Contains(logs.Bytes(), []byte("prearranged_token_padding_recovered")) {
		t.Fatal("unsafe or missing recovery log")
	}

	good, err := proto.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if err := codec.Unmarshal(good, &authpb.IssuePrearrangedUserTokenRequest{}); err != nil {
		t.Fatal("normal protobuf behavior changed", err)
	}
}

func TestPrearrangedPaddingRejectsUnknownExpiredAndModifiedTokens(t *testing.T) {
	token := "header.payload.signature"
	state := &serverContext{writer: &bytes.Buffer{}}
	state.prearrangedTokens.remember(token)
	codec := &prearrangedDiagnosticCodec{p: state}
	for _, test := range []struct {
		name   string
		token  []byte
		tenant string
	}{
		{"unknown", append([]byte("other.payload.signature"), 0xff), "tenants/current"},
		{"modified", append([]byte("header.payload.different"), 0xff), "tenants/current"},
		{"foreign-tenant", append([]byte(token), 0xff), "tenants/foreign"},
		{"excessive-padding", append([]byte(token), bytes.Repeat([]byte{0xff}, 65)...), "tenants/current"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := codec.Unmarshal(paddedPrearrangedRequest(test.token, test.tenant), &authpb.IssuePrearrangedUserTokenRequest{}); err == nil {
				t.Fatal("unrecognized or excessive padding accepted")
			}
		})
	}

	state.prearrangedTokens.tokens[token] = time.Now().Add(-time.Second)
	if err := codec.Unmarshal(paddedPrearrangedRequest(append([]byte(token), 0xff), "tenants/current"), &authpb.IssuePrearrangedUserTokenRequest{}); err == nil {
		t.Fatal("expired cache entry used for recovery")
	}

	state.prearrangedTokens.remember(token)
	wire := paddedPrearrangedRequest(append([]byte(token), 0xff), "tenants/current")
	wire = protowire.AppendTag(wire, 3, protowire.BytesType)
	wire = protowire.AppendBytes(wire, []byte{0xff})
	if err := codec.Unmarshal(wire, &authpb.IssuePrearrangedUserTokenRequest{}); err == nil {
		t.Fatal("duplicate malformed external token accepted")
	}

	var cache prearrangedTokenCache
	for i := 0; i < 100; i++ {
		cache.remember(strings.Repeat("a", i+1))
	}
	if len(cache.tokens) > 64 {
		t.Fatal("cache exceeded memory bound")
	}
}
