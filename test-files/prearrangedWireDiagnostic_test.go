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

	codec := &prearrangedDiagnosticCodec{
		p: &serverContext{
			capture: &rpcCapture{
				dir: dir,
			},
			writer: &logs,
		},
	}
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
		Tenant: "tenants/current",
		ExternalIdToken: &authpb.ExternalIdToken{
			Token: &authpb.ExternalIdToken_DummyExtIdToken{
				DummyExtIdToken: "private",
			},
		},
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

func TestPrearrangedInvalidUTF8ReportsOnlyLengths(t *testing.T) {
	dir := t.TempDir()
	arm, _ := json.Marshal(rpcCaptureArm{
		ExpiresAt: time.Now().Add(time.Minute),
	})

	if err := os.WriteFile(filepath.Join(dir, "arm.json"), arm, 0600); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer

	codec := &prearrangedDiagnosticCodec{
		p: &serverContext{
			capture: &rpcCapture{
				dir: dir,
			},
			writer: &logs,
		},
	}

	secret := []byte("private.header.signature")
	value := append(append([]byte(nil), secret...), 0xff)
	nested := protowire.AppendTag(nil, 1, protowire.BytesType)
	nested = protowire.AppendBytes(nested, value)
	request := protowire.AppendTag(nil, 3, protowire.BytesType)
	request = protowire.AppendBytes(request, nested)

	if codec.Unmarshal(request, &authpb.IssuePrearrangedUserTokenRequest{}) == nil {
		t.Fatal("invalid UTF-8 token accepted")
	}

	if bytes.Contains(logs.Bytes(), secret) {
		t.Fatal("credential leaked")
	}

	var event struct {
		DecodeKind string `json:"decode_kind"`
		Fields     []struct {
			Nested []struct {
				Valid  bool `json:"utf8_valid"`
				Prefix int  `json:"jwt_ascii_prefix_bytes"`
			} `json:"nested_fields"`
		} `json:"fields"`
	}

	if err := json.Unmarshal(logs.Bytes(), &event); err != nil {
		t.Fatal(err)
	}

	if event.DecodeKind != "invalid_utf8" || len(event.Fields) != 1 ||
		len(event.Fields[0].Nested) != 1 || event.Fields[0].Nested[0].Valid ||
		event.Fields[0].Nested[0].Prefix != len(secret) {
		t.Fatal("incorrect safe diagnostic")
	}
}
