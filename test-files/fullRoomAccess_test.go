package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestFullAssignedRoomParticipantAccess(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	name := "tenants/t-adf89f68-lp1/gameSessions/full"
	users := []string{
		"host",
		"guest1",
		"guest2",
		"guest3",
	}
	room := &mm.GameSession{
		Name:                    name,
		State:                   mm.GameSession_ACTIVE,
		IsPublic:                true,
		CanParticipate:          true,
		MaxParticipantCount:     4,
		CurrentParticipantCount: 4,
	}
	contexts := map[string]context.Context{}

	for _, user := range append(users, "outsider") {
		owner := "tenants/t-adf89f68-lp1/users/" + user
		token, err := (&authServer{
			key: key,
		}).signSessionToken(owner, "access", time.Minute)

		if err != nil {
			t.Fatal(err)
		}

		contexts[user] = metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))

		if user != "outsider" {
			room.UserSessions = append(room.UserSessions, &mm.UserSession{
				User: owner,
				Name: name + "/userSessions/" + user,
			})
		}
	}

	r := &creationRecord{
		owner:   room.UserSessions[0].User,
		expires: time.Now().Add(time.Hour),
		ticket: &mm.GameSessionCreationTicket{
			GameSession: room,
		},
	}
	s := &gameSessionServer{
		p: p,
		tickets: map[string]*creationRecord{
			"full": r,
		},
	}
	request := &mm.CreateGameSessionShortAliasRequest{
		Parent: "tenants/current",
		GameSessionShortAlias: &mm.GameSessionShortAlias{
			GameSession: name,
		},
	}
	alias, err := s.CreateGameSessionShortAlias(contexts["guest1"], request)

	if err != nil {
		t.Fatal(err)
	}

	for _, state := range []mm.GameSession_State{
		mm.GameSession_CREATING,
		mm.GameSession_ACTIVE,
	} {
		room.State = state

		for _, user := range users {
			got, err := s.GetGameSession(contexts[user], &mm.GetGameSessionRequest{
				Name: name,
			})

			if err != nil || got.CurrentParticipantCount != 4 {
				t.Fatal("participant cannot read full room", user, err)
			}

			gotAlias, err := s.CreateGameSessionShortAlias(contexts[user], request)

			if err != nil || gotAlias.Name != alias.Name {
				t.Fatal("participant cannot retrieve published alias", user, err)
			}

			batch, err := s.BatchGetGameSessions(contexts[user], &mm.BatchGetGameSessionsRequest{
				Names: []string{
					name,
				},
			})

			if err != nil || len(batch.GameSessions) != 1 {
				t.Fatal("participant cannot batch read full room", user, err)
			}
		}
	}

	if discoverableRoom(r) {
		t.Fatal("full room advertised for new entrants")
	}

	if _, err := s.GetGameSession(contexts["outsider"], &mm.GetGameSessionRequest{
		Name: name,
	}); status.Code(err) != codes.NotFound {
		t.Fatal("outsider can read full room", err)
	}

	if _, err := s.CreateGameSessionShortAlias(contexts["outsider"], request); status.Code(err) != codes.NotFound {
		t.Fatal("outsider can retrieve full room alias", err)
	}

	r.expires = time.Now().Add(-time.Second)

	if _, err := s.GetGameSession(contexts["guest1"], &mm.GetGameSessionRequest{
		Name: name,
	}); status.Code(err) != codes.NotFound {
		t.Fatal("expired room remained accessible", err)
	}
}
