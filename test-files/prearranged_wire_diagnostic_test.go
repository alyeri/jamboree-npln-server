package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestPrearrangedWireDiagnosticDoesNotExposeOrAcceptMalformedToken(t *testing.T) {
	dir := t.TempDir()
	arm, _ := json.Marshal(rpcCaptureArm{
		ExpiresAt:   time.Now().Add(time.Minute),
		MaxMessages: 256,
	})

	if err := os.WriteFile(filepath.Join(dir, "arm.json"), arm, 0600); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	codec := &prearrangedDiagnosticCodec{p: &serverContext{
		capture: &rpcCapture{dir: dir},
		writer:  &logs,
	}}
	secret := []byte("private-token-that-must-never-be-logged")
	b := protowire.AppendTag(nil, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, secret)

	if codec.Unmarshal(b, &authpb.IssuePrearrangedUserTokenRequest{}) == nil {
		t.Fatal("malformed nested token accepted")
	}

	if bytes.Contains(logs.Bytes(), secret) || !bytes.Contains(logs.Bytes(), []byte("prearranged_decode_shape")) {
		t.Fatal("unsafe or missing shape log")
	}

	req := &authpb.IssuePrearrangedUserTokenRequest{
		Tenant:          "tenants/current",
		ExternalIdToken: &authpb.ExternalIdToken{Token: &authpb.ExternalIdToken_DummyExtIdToken{DummyExtIdToken: "private"}},
	}
	good, _ := codec.Marshal(req)
	decoded := &authpb.IssuePrearrangedUserTokenRequest{}

	if err := codec.Unmarshal(good, decoded); err != nil || !proto.Equal(req, decoded) {
		t.Fatal("normal protobuf behavior changed", err)
	}

	before := logs.Len()
	os.Remove(filepath.Join(dir, "arm.json"))
	codec.Unmarshal(b, &authpb.IssuePrearrangedUserTokenRequest{})

	if logs.Len() != before {
		t.Fatal("unarmed diagnostic logged")
	}
}

func TestPrearrangedTransportClassificationRedactsMessage(t *testing.T) {
	if got := prearrangedTransportErrorKind(status.Error(codes.Internal, "grpc: error unmarshalling request: secret")); got != "protobuf_decode" {
		t.Fatal(got)
	}
}
