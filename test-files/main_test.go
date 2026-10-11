package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestUnknownUnaryAndStream(t *testing.T) {
	var output bytes.Buffer

	p := &serverContext{
		writer: &output,
	}
	lis := bufconn.Listen(1 << 20)
	s := newServer(p)

	go s.Serve(lis)
	defer s.Stop()

	conn, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer SECRET_TOKEN", "uid", "PRIVATE_USER", "npln-tenant-id", "t-adf89f68", "cookie", "PRIVATE_COOKIE"))
	err = conn.Invoke(ctx, "/nn.npln.auth.v1.Auth/UnknownDiagnosticRpc", &emptypb.Empty{}, &emptypb.Empty{})

	if status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}

	stream, err := conn.NewStream(ctx, &grpc.StreamDesc{
		ServerStreams: true,
		ClientStreams: true,
	}, "/nn.npln.gamesync.v1.Gamesync/UnknownDiagnosticStream")

	if err != nil {
		t.Fatal(err)
	}

	err = stream.RecvMsg(&emptypb.Empty{})

	if status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	} // No send required: unknown streaming RPC must not block waiting for payload.

	s.Stop()

	p.mu.Lock()

	data := output.String()

	p.mu.Unlock()

	for _, secret := range []string{"SECRET_TOKEN", "PRIVATE_USER", "PRIVATE_COOKIE"} {
		if strings.Contains(data, secret) {
			t.Fatalf("secret leaked: %s", secret)
		}
	}

	if !strings.Contains(data, "UnknownDiagnosticRpc") || !strings.Contains(data, "UnknownDiagnosticStream") || !strings.Contains(data, "unimplemented") {
		t.Fatal(data)
	}

	for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
		var e map[string]any

		if json.Unmarshal([]byte(line), &e) != nil {
			t.Fatal(line)
		}
	}
}
func TestRegisteredUnimplementedIsLogged(t *testing.T) {
	var output bytes.Buffer

	p := &serverContext{
		writer: &output,
	}
	ctx := p.TagRPC(context.Background(), &stats.RPCTagInfo{
		FullMethodName: "/known.Service/Missing",
	})

	p.HandleRPC(ctx, &stats.End{
		Error: status.Error(codes.Unimplemented, "TOKEN_SHOULD_NOT_BE_LOGGED"),
	})

	if !strings.Contains(output.String(), "Unimplemented") || strings.Contains(output.String(), "TOKEN_SHOULD_NOT_BE_LOGGED") {
		t.Fatal(output.String())
	}
}
