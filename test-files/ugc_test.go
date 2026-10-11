package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ugcpb "github.com/alyeri/jamboree-npln-server/proto/ugcstore/v1"
)

func TestDocumentAuthenticationAndMissingResource(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	s := &ugcServer{
		p: &serverContext{
			identityKey: key,
			writer:      &logs,
		},
	}
	signer := &authServer{
		key: key,
	}
	access, _ := signer.signSessionToken("tenants/current/users/private-user", "access", time.Minute)
	refresh, _ := signer.signSessionToken("tenants/current/users/private-user", "refresh", time.Minute)
	expired, _ := signer.signSessionToken("tenants/current/users/private-user", "access", -time.Minute)

	for _, tc := range []struct {
		name, token, tenant, resource string
		code                          codes.Code
	}{
		{
			"missing",
			access,
			"t-adf89f68-lp1",
			"tenants/current/documents/private-document",
			codes.NotFound,
		},
		{
			"no-token",
			"",
			"t-adf89f68-lp1",
			"tenants/current/documents/x",
			codes.Unauthenticated,
		},
		{
			"refresh",
			refresh,
			"t-adf89f68-lp1",
			"tenants/current/documents/x",
			codes.Unauthenticated,
		},
		{
			"expired",
			expired,
			"t-adf89f68-lp1",
			"tenants/current/documents/x",
			codes.Unauthenticated,
		},
		{
			"tampered",
			access + "x",
			"t-adf89f68-lp1",
			"tenants/current/documents/x",
			codes.Unauthenticated,
		},
		{
			"tenant",
			access,
			"foreign",
			"tenants/current/documents/x",
			codes.PermissionDenied,
		},
		{
			"resource",
			access,
			"t-adf89f68-lp1",
			"tenants/foreign/documents/x",
			codes.InvalidArgument,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", tc.tenant, "authorization", "Bearer "+tc.token))
			_, err := s.GetDocument(ctx, &ugcpb.GetDocumentRequest{
				Name: tc.resource,
			})

			if status.Code(err) != tc.code {
				t.Fatalf("got %v want %v", status.Code(err), tc.code)
			}
		})
	}

	for _, secret := range []string{
		access,
		refresh,
		"private-user",
		"private-document",
	} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("private data logged")
		}
	}
}
