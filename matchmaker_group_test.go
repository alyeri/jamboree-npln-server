package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestMatchmakingGroupCapacityAndDeadline(t *testing.T) {
	for _, mode := range []int64{34308, 1344260, 492292, 33284} {
		for count := 1; count <= 5; count++ {
			t.Run(fmt.Sprintf("mode%d/players%d", mode, count), func(t *testing.T) {
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
				for i := 0; i < count; i++ {
					owner := fmt.Sprintf("tenants/t-adf89f68-lp1/users/player%d", i)
					ticket := &mm.MatchmakingTicket{
						Name:              fmt.Sprintf("ticket%d", i),
						State:             mm.MatchmakingTicket_SEARCHING,
						MatchmakingConfig: "tenants/t-adf89f68-lp1/matchmakingConfigs/BqP4MatchmakingConfigs",
						UserDefinitions: []*mm.UserDefinition{
							{
								User: owner,
								Attributes: &common.MapValue{
									Fields: map[string]*common.Value{"gameMode": intValue(mode)},
								},
							},
						},
					}
					r := &searchRecord{
						owner:    owner,
						ticket:   ticket,
						tracking: true,
						created:  time.Now().Add(time.Duration(i-count) * time.Millisecond),
					}
					m.tickets[ticket.Name] = r
					records = append(records, r)
				}

				if err := m.assignGroup(records[0]); err != nil {
					t.Fatal(err)
				}

				if count < 4 {
					if len(rooms.tickets) != 0 {
						t.Fatal("partial group assigned before deadline")
					}

					records[0].created = time.Now().Add(-matchmakingSearchWindow)

					if err := m.assignGroup(records[0]); err != nil {
						t.Fatal(err)
					}
				}

				want := count

				if want > 4 {
					want = 4
				}

				if len(rooms.tickets) != 1 {
					t.Fatal("group did not create exactly one room")
				}

				roomName := records[0].ticket.GameSession.Name

				for i, r := range records {
					if i >= want {
						if r.ticket.State != mm.MatchmakingTicket_SEARCHING {
							t.Fatal("fifth player consumed")
						}

						continue
					}

					gs := r.ticket.GameSession

					if r.ticket.State != mm.MatchmakingTicket_SUCCEEDED || gs.Name != roomName || int(gs.CurrentParticipantCount) != want || gs.MaxParticipantCount != 4 || len(gs.UserSessions) != want || len(r.ticket.MatchedUserSessions) != 1 {
						t.Fatal("incorrect group membership", i)
					}

					matched := r.ticket.MatchedUserSessions[0]
					proof, err := verifySessionProof(matched.MatchmakingIdToken, p.identityKey, "matchmaking")

					if err != nil || proof != r.owner+"|"+matched.UserSession {
						t.Fatal("incorrect participant proof", err)
					}
				}

				gs := &gamesyncServer{
					p:     p,
					rooms: rooms,
				}
				last := records[want-1].ticket.MatchedUserSessions[0]

				if err := gs.initializeSessionDocuments(last.UserDefinition.User + "|" + last.UserSession); err != nil {
					t.Fatal(err)
				}

				for i, u := range records[0].ticket.GameSession.UserSessions {
					id := u.Name[len(roomName+"/userSessions/"):]

					if gs.documents[roomName]["docs/__us/"+id].GetFields().GetFields()["ussid"].GetIntegerValue() != int64(i+1) {
						t.Fatal("incorrect station index")
					}
				}
			})
		}
	}
}

func TestMatchmakingExcludesDisconnectedTracker(t *testing.T) {
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
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("player%d", i)
		m.tickets[name] = &searchRecord{
			owner:    "tenants/current/users/" + name,
			tracking: true,
			created:  time.Now(),
			ticket: &mm.MatchmakingTicket{
				State:             mm.MatchmakingTicket_SEARCHING,
				MatchmakingConfig: "BqP4MatchmakingConfigs",
				UserDefinitions: []*mm.UserDefinition{
					{
						User:       "tenants/current/users/" + name,
						Attributes: &common.MapValue{Fields: map[string]*common.Value{"gameMode": intValue(34308)}},
					},
				},
			},
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.tickets["player3"].trackContext = ctx

	if err := m.assignGroup(m.tickets["player0"]); err != nil {
		t.Fatal(err)
	}

	if len(rooms.tickets) != 0 {
		t.Fatal("disconnected player completed group")
	}

	m.tickets["player0"].created = time.Now().Add(-matchmakingSearchWindow)

	if err := m.assignGroup(m.tickets["player0"]); err != nil {
		t.Fatal(err)
	}

	if m.tickets["player0"].ticket.GameSession.CurrentParticipantCount != 3 || m.tickets["player3"].ticket.GameSession != nil {
		t.Fatal("disconnected player assigned")
	}
}
