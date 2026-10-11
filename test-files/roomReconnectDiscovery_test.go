package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestAssignedGuestCanFindAndRejoinFullRoom(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	guest := "tenants/t-adf89f68-lp1/users/guest"
	room := &mm.GameSession{
		Name:                    "tenants/t-adf89f68-lp1/gameSessions/reconnect",
		State:                   mm.GameSession_ACTIVE,
		MaxParticipantCount:     4,
		CurrentParticipantCount: 4,
		UserSessions: []*mm.UserSession{
			{
				User: "host",
				Name: "host-session",
			},
			{
				User: guest,
				Name: "guest-session",
			},
			{
				User: "other1",
				Name: "other1-session",
			},
			{
				User: "other2",
				Name: "other2-session",
			},
		},
	}
	server := &gameSessionServer{
		p: &serverContext{
			writer:      &bytes.Buffer{},
			identityKey: key,
		},
		tickets: map[string]*creationRecord{
			"room": {
				owner:   "host",
				expires: time.Now().Add(time.Hour),
				ticket: &mm.GameSessionCreationTicket{
					GameSession: room,
				},
			},
		},
	}
	contextFor := func(user string) context.Context {
		token, err := (&authServer{
			key: key,
		}).signSessionToken(user, "access", time.Minute)

		if err != nil {
			t.Fatal(err)
		}

		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	}
	request := &mm.QueryGameSessionsRequest{
		MinVacancyCount: 1,
	}
	result, err := server.QueryGameSessions(contextFor(guest), request)

	if err != nil || len(result.GetGameSessions()) != 1 || !result.GameSessions[0].CanParticipate {
		t.Fatalf("assigned guest cannot discover its reconnect slot: %v", err)
	}

	joined, err := server.JoinGameSession(contextFor(guest), &mm.JoinGameSessionRequest{
		Name: room.Name,
	})

	if err != nil || joined.GetGameSession().GetCurrentParticipantCount() != 4 || len(room.UserSessions) != 4 {
		t.Fatalf("reconnection must reuse the existing participant: %v", err)
	}

	result, err = server.QueryGameSessions(contextFor("outsider"), request)

	if err != nil || len(result.GetGameSessions()) != 0 || room.CanParticipate {
		t.Fatalf("full room must remain unavailable to new entrants: %v", err)
	}

	room.State = mm.GameSession_TERMINATED
	result, err = server.QueryGameSessions(contextFor(guest), request)

	if err != nil || len(result.GetGameSessions()) != 0 {
		t.Fatalf("terminated room must not be offered for reconnection: %v", err)
	}
}
