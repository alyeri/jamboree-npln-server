package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestDailyChallengeModesAndPoolIsolation(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	rooms := &gameSessionServer{
		p:       p,
		tickets: map[string]*creationRecord{},
	}
	m := &matchmakerServer{
		rooms:   rooms,
		tickets: map[string]*searchRecord{},
	}
	token, err := (&authServer{key: key}).signSessionToken("tenants/current/users/player", "access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	var pools []string

	for _, mode := range []int64{1344260, 492292} {
		req := &mm.CreateMatchmakingTicketRequest{
			Parent: "tenants/current",
			MatchmakingTicket: &mm.MatchmakingTicket{
				MatchmakingConfig: "tenants/current/matchmakingConfigs/BqP4MatchmakingConfigs",
				UserDefinitions: []*mm.UserDefinition{
					{
						User:       "users/current",
						Attributes: &common.MapValue{Fields: map[string]*common.Value{"gameMode": intValue(mode)}},
					},
				},
			},
		}
		ticket, err := m.CreateMatchmakingTicket(ctx, req)

		if err != nil {
			t.Fatal(mode, err)
		}

		if ticket.State != mm.MatchmakingTicket_SEARCHING || ticket.UserDefinitions[0].Attributes.Fields["gameMode"].GetIntegerValue() != mode {
			t.Fatal("daily mode lost")
		}

		pools = append(pools, searchPool(ticket))
	}

	if pools[0] == pools[1] {
		t.Fatal("different daily challenges share a pool")
	}

	for _, mode := range []int64{-1, 0, 492293, 1<<32 | 0x8304} {
		req := &mm.CreateMatchmakingTicketRequest{
			Parent: "tenants/current",
			MatchmakingTicket: &mm.MatchmakingTicket{
				MatchmakingConfig: "tenants/current/matchmakingConfigs/BqP4MatchmakingConfigs",
				UserDefinitions: []*mm.UserDefinition{
					{Attributes: &common.MapValue{Fields: map[string]*common.Value{"gameMode": intValue(mode)}}},
				},
			},
		}

		if _, err := m.CreateMatchmakingTicket(ctx, req); status.Code(err) != codes.Unimplemented {
			t.Fatal("unsupported mode accepted", mode, err)
		}
	}
}
