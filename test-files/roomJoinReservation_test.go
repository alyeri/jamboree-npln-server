package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestFailedRoomJoinReleasesVacancy(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	now := time.Now()
	room := &mm.GameSession{
		Name: canonicalRoom("reservation"), State: mm.GameSession_ACTIVE,
		MaxParticipantCount: 4, CurrentParticipantCount: 4,
		UserSessions: []*mm.UserSession{
			{
				User:  "host",
				State: mm.UserSession_ACTIVE,
			},
			{
				User:  "guest1",
				State: mm.UserSession_ACTIVE,
			},
			{
				User:  "guest2",
				State: mm.UserSession_ACTIVE,
			},
			{
				User:       "failed",
				State:      mm.UserSession_RESERVED,
				CreateTime: timestamppb.New(now.Add(-2 * time.Minute)),
			},
		},
	}
	record := &creationRecord{
		owner:   "host",
		expires: now.Add(time.Hour),
		ticket: &mm.GameSessionCreationTicket{
			GameSession: room,
		},
	}
	server := &gameSessionServer{
		p: &serverContext{
			writer:      &bytes.Buffer{},
			identityKey: key,
		},
		tickets: map[string]*creationRecord{
			"room": record,
		},
	}
	token, err := (&authServer{
		key: key,
	}).signSessionToken("newguest", "access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))

	directory := (&friendsServer{
		p:     server.p,
		rooms: server,
	}).publicRoomHosts("newguest")

	if len(directory.FriendAccounts) != 1 || room.CurrentParticipantCount != 3 {
		t.Fatal("failed reservation still hides the host")
	}

	result, err := server.QueryGameSessions(ctx, &mm.QueryGameSessionsRequest{
		MinVacancyCount: 1,
	})

	if err != nil || len(result.GetGameSessions()) != 1 {
		t.Fatalf("released vacancy missing from room search: %v", err)
	}

	joined, err := server.JoinGameSession(ctx, &mm.JoinGameSessionRequest{
		Name: room.Name,
	})

	if err != nil || joined.GetGameSession().GetCurrentParticipantCount() != 4 {
		t.Fatalf("new guest cannot use released vacancy: %v", err)
	}

	server.expirePendingRoomJoins(record, now)

	if room.CurrentParticipantCount != 4 {
		t.Fatal("fresh reservation expired")
	}

	sync := &gamesyncServer{
		p:     server.p,
		rooms: server,
	}
	member := joined.MatchedUserSessions[0].UserSession

	if err := sync.activateBoundRoom("newguest|" + member); err != nil {
		t.Fatal(err)
	}

	server.expirePendingRoomJoins(record, now.Add(3*time.Minute))

	if room.CurrentParticipantCount != 4 || room.CanParticipate {
		t.Fatal("established player expired or full room reopened")
	}
}
