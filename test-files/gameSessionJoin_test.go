package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestJoinPasswordCapacityAndSessionBinding(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	p := &serverContext{
		writer:      &logs,
		identityKey: key,
	}
	room := &mm.GameSession{
		Name:                    canonicalRoom("room"),
		Password:                "1234",
		MaxParticipantCount:     2,
		CurrentParticipantCount: 1,
		CanParticipate:          true,
		State:                   mm.GameSession_ACTIVE,
		UserSessions: []*mm.UserSession{
			{
				Name: canonicalRoom("room") + "/userSessions/host",
				User: "host",
			},
		},
	}
	s := &gameSessionServer{
		p: p,
		tickets: map[string]*creationRecord{
			"r": {
				owner:   "host",
				expires: time.Now().Add(time.Hour),
				ticket: &mm.GameSessionCreationTicket{
					GameSession: room,
					UserDefinitions: []*mm.UserDefinition{
						{
							User: "host",
						},
					},
				},
			},
		},
	}
	ctxFor := func(user string) context.Context {
		token, _ := (&authServer{
			key: key,
		}).signSessionToken(user, "access", time.Minute)

		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	}

	for _, password := range []string{"", "*", "wrong"} {
		if _, e := s.JoinGameSession(ctxFor("guest"), &mm.JoinGameSessionRequest{
			Name:     room.Name,
			Password: password,
		}); e == nil || room.CurrentParticipantCount != 1 {
			t.Fatal("missing, masked or wrong password accepted")
		}
	}

	gs := &gamesyncServer{
		p:     p,
		rooms: s,
	}

	if e := gs.initializeSessionDocuments("host|" + room.Name + "/userSessions/host"); e != nil {
		t.Fatal(e)
	}

	joined, e := s.JoinGameSession(ctxFor("guest"), &mm.JoinGameSessionRequest{
		Name:                room.Name,
		Password:            "1234",
		IncludeIdTokenUsers: []string{"guest"},
	})

	if e != nil || room.CurrentParticipantCount != 2 || room.CanParticipate || joined.GameSession.Password != "" {
		t.Fatal("join failed", e)
	}

	proof, e := matchmakingProof(joined.MatchedUserSessions[0].MatchmakingIdToken, key)

	if e != nil {
		t.Fatal(e)
	}

	if e := gs.activateBoundRoom(proof); e != nil {
		t.Fatal("guest activation", e)
	}

	if e := gs.initializeSessionDocuments(proof); e != nil {
		t.Fatal("guest documents", e)
	}

	guestDoc := gs.documents[room.Name]["docs/__us/guest"]

	if guestDoc.GetFields().GetFields()["ussid"].GetIntegerValue() != 2 {
		t.Fatal("guest session identifier duplicates host")
	}

	if _, e := s.JoinGameSession(ctxFor("guest"), &mm.JoinGameSessionRequest{
		Name:                room.Name,
		Password:            "1234",
		IncludeIdTokenUsers: []string{"guest"},
	}); e != nil || room.CurrentParticipantCount != 2 {
		t.Fatal("retry duplicated participant", e)
	}

	if _, e := s.JoinGameSession(ctxFor("third"), &mm.JoinGameSessionRequest{
		Name:                room.Name,
		Password:            "1234",
		IncludeIdTokenUsers: []string{"guest"},
	}); e == nil {
		t.Fatal("full room accepted")
	}
}
