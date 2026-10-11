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

func TestMarioPartyCapturedTicketAndConfigIsolation(t *testing.T) {
	for _, tc := range []struct {
		mode   int64
		config string
		accept bool
	}{
		{
			32772,
			"BqP4MatchmakingConfigs",
			true,
		},
		{
			33028,
			"BqP4MatchmakingConfigs",
			true,
		},
		{
			425988,
			"BqP4MatchmakingConfigs",
			true,
		},
		{
			0x68104,
			"BqP4MatchmakingConfigs",
			true,
		},
		{
			0x18004,
			"BqP4MatchmakingConfigs",
			true,
		},
		{
			0xffff8004,
			"BqP4MatchmakingConfigs",
			true,
		},
		{
			425988,
			"BqP8MatchmakingConfigs",
			false,
		},
		{
			425988,
			"BqP4NoEasePaticipantConfigs",
			false,
		},
		{
			0x68005,
			"BqP4MatchmakingConfigs",
			false,
		},
		{
			33028,
			"BqP8MatchmakingConfigs",
			false,
		},
		{
			33028,
			"BqP4NoEasePaticipantConfigs",
			false,
		},
		{
			0x8105,
			"BqP4MatchmakingConfigs",
			false,
		},
		{
			0x100008104,
			"BqP4MatchmakingConfigs",
			false,
		},
		{
			32772,
			"BqP8MatchmakingConfigs",
			false,
		},
		{
			32772,
			"BqP4NoEasePaticipantConfigs",
			false,
		},
		{
			0x8005,
			"BqP4MatchmakingConfigs",
			false,
		},
		{
			-1,
			"BqP4MatchmakingConfigs",
			false,
		},
		{
			0x100008004,
			"BqP4MatchmakingConfigs",
			false,
		},
	} {
		t.Run(fmt.Sprintf("%d-%s", tc.mode, tc.config), func(t *testing.T) {
			key := bytes.Repeat([]byte{7}, 32)
			p := &serverContext{
				identityKey: key,
				writer:      &bytes.Buffer{},
			}
			m := &matchmakerServer{
				rooms: &gameSessionServer{
					p:       p,
					tickets: map[string]*creationRecord{},
				},
			}
			token, err := (&authServer{
				key: key,
			}).signSessionToken("tenants/current/users/party", "access", time.Minute)

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
							User: "users/current",
							Attributes: &common.MapValue{
								Fields: map[string]*common.Value{
									"gameMode": intValue(tc.mode),
								},
							},
						},
					},
				},
			}
			result, err := m.CreateMatchmakingTicket(ctx, req)

			if (err == nil) != tc.accept {
				t.Fatalf("accept=%v err=%v", tc.accept, err)
			}

			if tc.accept && result.UserDefinitions[0].Attributes.Fields["gameMode"].GetIntegerValue() != tc.mode {
				t.Fatal("board/rule encoding lost")
			}

			if tc.accept && (result.State != mm.MatchmakingTicket_SEARCHING || result.GameSession != nil) {
				t.Fatal("premature assignment")
			}
		})
	}
}

func TestMarioPartyCapacityDeadlineAndGamesync(t *testing.T) {
	for _, mode := range []int64{
		32772,
		33028,
		425988,
		0x68104,
	} {
		t.Run(fmt.Sprintf("mode%d", mode), func(t *testing.T) {
			testMarioPartyCapacityDeadlineAndGamesync(t, mode)
		})
	}
}

func testMarioPartyCapacityDeadlineAndGamesync(t *testing.T, mode int64) {
	for _, n := range []int{
		1,
		2,
		3,
		4,
		5,
	} {
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
				owner := fmt.Sprintf("tenants/t-adf89f68-lp1/users/party%d", i)
				ticket := &mm.MatchmakingTicket{
					Name:              fmt.Sprintf("ticket%d", i),
					State:             mm.MatchmakingTicket_SEARCHING,
					MatchmakingConfig: "tenants/t-adf89f68-lp1/matchmakingConfigs/BqP4MatchmakingConfigs",
					UserDefinitions: []*mm.UserDefinition{
						{
							User: owner,
							Attributes: &common.MapValue{
								Fields: map[string]*common.Value{
									"gameMode": intValue(mode),
								},
							},
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

			if n < 4 {
				if len(rooms.tickets) != 0 {
					t.Fatal("partial group before deadline")
				}

				records[0].created = time.Now().Add(-matchmakingSearchWindow)

				if err := m.assignGroup(records[0]); err != nil {
					t.Fatal(err)
				}
			}

			want := n

			if want > 4 {
				want = 4
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

				if gs.MaxParticipantCount != 4 || int(gs.CurrentParticipantCount) != want || len(gs.UserSessions) != want || gs.Name != name {
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

func TestMarioPartyPairAndGuestFirstGamesync(t *testing.T) {
	t.Run("PartyRules", func(t *testing.T) {
		testMatchmakerPairAndGuestFirstGamesync(t, 32772)
	})

	t.Run("ProRules", func(t *testing.T) {
		testMatchmakerPairAndGuestFirstGamesync(t, 33028)
	})

	t.Run("MegaWigglerPartyRules", func(t *testing.T) {
		testMatchmakerPairAndGuestFirstGamesync(t, 425988)
	})
}

func TestMarioPartyDoesNotJoinOtherModes(t *testing.T) {
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

	for i, mode := range []int64{
		32772,
		33028,
		425988,
		0x68104,
		34308,
		33284,
		1344260,
	} {
		name := fmt.Sprintf("player%d", i)
		m.tickets[name] = &searchRecord{
			owner:    name,
			created:  time.Now().Add(-matchmakingSearchWindow),
			tracking: true,
			ticket: &mm.MatchmakingTicket{
				Name:              name,
				State:             mm.MatchmakingTicket_SEARCHING,
				MatchmakingConfig: "tenants/t-adf89f68-lp1/matchmakingConfigs/BqP4MatchmakingConfigs",
				UserDefinitions: []*mm.UserDefinition{
					{
						User: name,
						Attributes: &common.MapValue{
							Fields: map[string]*common.Value{
								"gameMode": intValue(mode),
							},
						},
					},
				},
			},
		}
	}

	if err := m.assignGroup(m.tickets["player0"]); err != nil {
		t.Fatal(err)
	}

	if len(rooms.tickets) != 1 || m.tickets["player0"].ticket.GameSession.CurrentParticipantCount != 1 {
		t.Fatal("mixed modes assigned to Party room")
	}

	for i := 1; i < 7; i++ {
		if m.tickets[fmt.Sprintf("player%d", i)].ticket.State != mm.MatchmakingTicket_SEARCHING {
			t.Fatal("other mode consumed")
		}
	}
}
