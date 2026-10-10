package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestKaboomCapturedTicketAndConfigIsolation(t *testing.T) {
	for _, tc := range []struct {
		mode   int64
		config string
		accept bool
	}{
		{100356, "BqP8MatchmakingConfigs", true},
		{0x8804, "BqP8MatchmakingConfigs", true},
		{0x28804, "BqP8MatchmakingConfigs", true},
		{100356, "BqP4MatchmakingConfigs", false},
		{34308, "BqP8MatchmakingConfigs", false},
		{0x18805, "BqP8MatchmakingConfigs", false},
		{0x100018804, "BqP8MatchmakingConfigs", false},
	} {
		t.Run(fmt.Sprintf("%d-%s", tc.mode, tc.config), func(t *testing.T) {
			key := bytes.Repeat([]byte{7}, 32)
			p := &serverContext{
				identityKey: key,
				writer:      &bytes.Buffer{},
			}
			m := &matchmakerServer{rooms: &gameSessionServer{
				p:       p,
				tickets: map[string]*creationRecord{},
			}}
			token, err := (&authServer{key: key}).signSessionToken("tenants/current/users/kaboom", "access", time.Minute)

			if err != nil {
				t.Fatal(err)
			}

			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
			req := &mm.CreateMatchmakingTicketRequest{
				Parent: "tenants/current",
				MatchmakingTicket: &mm.MatchmakingTicket{
					MatchmakingConfig: "tenants/current/matchmakingConfigs/" + tc.config,
					UserDefinitions: []*mm.UserDefinition{
						{
							User:       "users/current",
							Attributes: &common.MapValue{Fields: map[string]*common.Value{"gameMode": intValue(tc.mode)}},
						},
					},
				},
			}
			result, err := m.CreateMatchmakingTicket(ctx, req)

			if (err == nil) != tc.accept {
				t.Fatalf("accept=%v err=%v", tc.accept, err)
			}

			if tc.accept && (result.State != mm.MatchmakingTicket_SEARCHING || result.GameSession != nil) {
				t.Fatal("premature assignment")
			}
		})
	}
}

func TestKaboomCapacityDeadlineAndGamesync(t *testing.T) {
	for _, n := range []int{1, 2, 7, 8, 9} {
		t.Run(fmt.Sprintf("players%d", n), func(t *testing.T) {
			p := &serverContext{
				identityKey: bytes.Repeat([]byte{7}, 32),
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
			records := []*searchRecord{}
			for i := 0; i < n; i++ {
				owner := fmt.Sprintf("tenants/t-adf89f68-lp1/users/kaboom%d", i)
				ticket := &mm.MatchmakingTicket{
					Name:              fmt.Sprintf("ticket%d", i),
					State:             mm.MatchmakingTicket_SEARCHING,
					MatchmakingConfig: "tenants/t-adf89f68-lp1/matchmakingConfigs/BqP8MatchmakingConfigs",
					UserDefinitions: []*mm.UserDefinition{
						{
							User:       owner,
							Attributes: &common.MapValue{Fields: map[string]*common.Value{"gameMode": intValue(100356)}},
						},
					},
				}
				r := &searchRecord{
					owner:    owner,
					ticket:   ticket,
					tracking: true,
					created:  time.Now().Add(time.Duration(i-n) * time.Millisecond),
				}
				m.tickets[ticket.Name] = r
				records = append(records, r)
			}

			if err := m.assignGroup(records[0]); err != nil {
				t.Fatal(err)
			}

			if n < 8 {
				if len(rooms.tickets) != 0 {
					t.Fatal("partial group before deadline")
				}

				records[0].created = time.Now().Add(-matchmakingSearchWindow)

				if err := m.assignGroup(records[0]); err != nil {
					t.Fatal(err)
				}
			}

			want := n

			if want > 8 {
				want = 8
			}

			if len(rooms.tickets) != 1 {
				t.Fatal("expected one room")
			}

			name := records[0].ticket.GameSession.Name

			for i, r := range records {
				if i >= want {
					if r.ticket.State != mm.MatchmakingTicket_SEARCHING {
						t.Fatal("overflow consumed")
					}

					continue
				}

				gs := r.ticket.GameSession

				if gs.MaxParticipantCount != 8 || int(gs.CurrentParticipantCount) != want || len(gs.UserSessions) != want || gs.Name != name {
					t.Fatal("incorrect capacity or fabricated users")
				}
			}

			sync := &gamesyncServer{
				p:     p,
				rooms: rooms,
			}
			matched := records[want-1].ticket.MatchedUserSessions[0]

			if err := sync.initializeSessionDocuments(matched.UserDefinition.User + "|" + matched.UserSession); err != nil {
				t.Fatal(err)
			}

			for i, u := range records[0].ticket.GameSession.UserSessions {
				id := u.Name[len(name+"/userSessions/"):]

				if sync.documents[name]["docs/__us/"+id].GetFields().GetFields()["ussid"].GetIntegerValue() != int64(i+1) {
					t.Fatal("incorrect station index")
				}
			}
		})
	}
}
