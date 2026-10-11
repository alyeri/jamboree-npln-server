package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

func TestSurvivalReverseExternalIdQuery(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	primary := "tenants/t-adf89f68-lp1/users/abcdef0123456789abcdef0123456789"
	p := &serverContext{identityKey: key, accountIDs: map[string]string{
		primary: "00000000000000ab",
		"tenants/t-adf89f68-lp1/users/u-abcdef0123456789abcdef0123456789": "00000000000000ab",
		"tenants/t-adf89f68-lp1/users/1":                                  "00000000000000ab",
	}}
	token, _ := (&authServer{
		key: key,
	}).signSessionToken(primary, "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	s := &userServiceServer{
		p: p,
	}
	// Match the captured Survival request shape: current tenant, zero page
	// size, empty cursor, and 31 NSA identifiers in user_condition.
	condition := &authpb.QueryUserExternalIdsRequest_UserCondition{}

	for i := 0; i < 31; i++ {
		condition.ExternalIds = append(condition.ExternalIds, &authpb.ExternalId{
			Type: authpb.ExternalIdType_NSA_ID,
			Id:   fmt.Sprintf("%016x", i+1000),
		})
	}

	condition.ExternalIds[0].Id = "00000000000000AB"
	condition.ExternalIds[1].Id = "00000000000000ab"
	req := &authpb.QueryUserExternalIdsRequest{
		Tenant: "tenants/current",
		Condition: &authpb.QueryUserExternalIdsRequest_UserCondition_{
			UserCondition: condition,
		},
	}
	got, err := s.QueryUserExternalIds(ctx, req)

	if err != nil || len(got.GetUserExternalIds()) != 1 || got.UserExternalIds[0].User != primary || got.UserExternalIds[0].ExternalId.Id != "00000000000000ab" {
		t.Fatalf("wrong reverse mapping: %v", err)
	}

	condition.ExternalIds[0].Id = "0000000000000000"
	condition.ExternalIds[1].Id = "0000000000000000"
	got, err = s.QueryUserExternalIds(ctx, req)

	if err != nil || len(got.GetUserExternalIds()) != 0 {
		t.Fatal("unknown accounts invented", err)
	}

	condition.ExternalIds = make([]*authpb.ExternalId, 101)

	if _, err = s.QueryUserExternalIds(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatal("unbounded query accepted", err)
	}

	anonymous := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))

	if _, err = s.QueryUserExternalIds(anonymous, req); status.Code(err) != codes.Unauthenticated {
		t.Fatal("anonymous query accepted", err)
	}
}
