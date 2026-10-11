package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestFindApplicationConfigUnknownRPCCapture(t *testing.T) {
	dir := t.TempDir()
	key := bytes.Repeat([]byte{9}, 32)

	var logs bytes.Buffer

	p := &serverContext{
		identityKey: key,
		writer:      &logs,
		capture: &rpcCapture{
			dir: dir,
		},
	}
	arm, _ := json.Marshal(rpcCaptureArm{
		ExpiresAt: time.Now().Add(time.Minute),
		CallerDigests: []string{
			identityDigest("guest"),
		},
		MaxMessages: 4,
	})

	if err := os.WriteFile(filepath.Join(dir, "arm.json"), arm, 0600); err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1 << 20)
	server := newServer(p)

	go server.Serve(lis)
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("guest", "access", time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	wire := protowire.AppendTag(nil, 1, protowire.BytesType)
	wire = protowire.AppendString(wire, "tenants/current")
	wire = protowire.AppendTag(wire, 2, protowire.VarintType)
	wire = protowire.AppendVarint(wire, 42)
	wire = protowire.AppendTag(wire, 3, protowire.BytesType)
	wire = protowire.AppendString(wire, "PRIVATE_UNKNOWN_TOKEN")
	request := &emptypb.Empty{}

	request.ProtoReflect().SetUnknown(wire)

	if err = conn.Invoke(ctx, findApplicationConfigMethod, request, &emptypb.Empty{}); status.Code(err) != codes.Unimplemented {
		t.Fatal("handler result changed", err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "capture-*.jsonl"))

	if len(files) != 1 {
		t.Fatal("missing capture")
	}
	// The request record is committed before the handler returns.
	data, _ := os.ReadFile(files[0])

	if strings.Contains(string(data), token) || strings.Contains(string(data), "PRIVATE_UNKNOWN_TOKEN") || strings.Contains(logs.String(), "PRIVATE_UNKNOWN_TOKEN") {
		t.Fatal("credential leaked")
	}

	var captured bool

	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
		var r map[string]any

		if json.Unmarshal(line, &r) != nil {
			t.Fatal("invalid capture")
		}

		if r["direction"] != "request" {
			continue
		}

		captured = true

		if r["serialization_mode"] != "protobuf_wire_shape" || r["wire_valid"] != true || int(r["unknown_bytes"].(float64)) != len(wire) {
			t.Fatal("unknown fields lost")
		}

		fields := r["wire_fields"].([]any)

		if len(fields) != 3 || fields[0].(map[string]any)["resource"] != "tenants/current" || fields[1].(map[string]any)["uint64"] != "42" || fields[2].(map[string]any)["length"] != float64(len("PRIVATE_UNKNOWN_TOKEN")) {
			t.Fatal("incorrect decoded evidence")
		}
	}

	if !captured {
		t.Fatal("handler did not read request")
	}
}

func TestAppconfigWireShapeBounds(t *testing.T) {
	for _, raw := range [][]byte{
		{
			0,
		},
		{
			0x0a,
			0xff,
		},
		{
			0x0b,
		},
		make([]byte, 65537),
	} {
		if _, ok := appconfigWireShape(raw, 0); ok {
			t.Fatal("malformed or oversized message accepted")
		}
	}

	for _, value := range []string{
		"Bearer SECRET",
		"tenants/current/users/private",
		"tenants/current/applicationConfigs/x.y.z",
		"nx2.secret.secret",
	} {
		if safeAppconfigResource(value) {
			t.Fatal("unsafe text allowed")
		}
	}
}
