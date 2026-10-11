package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestPublicRoomBootstrapGamesyncAdmission(t *testing.T) {
	for _, tc := range []struct {
		config, password string
		public           bool
	}{
		{
			"BqP4MatchmakingConfigs",
			"",
			true,
		},
		{
			"tenants/current/matchmakingConfigs/BqP8MatchmakingConfigs",
			"",
			true,
		},
		{
			"BqP4MatchmakingConfigs",
			"1234",
			true,
		},
		{
			"tenants/current/matchmakingConfigs/BqP8MatchmakingConfigs",
			"1234",
			true,
		},
		{
			"friend_room4",
			"",
			false,
		},
		{
			"friend_room4",
			"1234",
			false,
		},
		{
			"friend_room8",
			"1234",
			false,
		},
		{
			"unknown",
			"",
			false,
		},
	} {
		t.Run(tc.config+tc.password, func(t *testing.T) {
			key := bytes.Repeat([]byte{7}, 32)
			owner := "tenants/current/users/host"
			rooms := &gameSessionServer{
				p: &serverContext{
					writer:      &bytes.Buffer{},
					identityKey: key,
				},
				tickets: map[string]*creationRecord{},
			}
			token, _ := (&authServer{
				key: key,
			}).signSessionToken(owner, "access", time.Minute)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
			ticket, err := rooms.CreateGameSessionCreationTicket(ctx, &mm.CreateGameSessionCreationTicketRequest{
				Parent: "tenants/current",
				GameSessionCreationTicket: &mm.GameSessionCreationTicket{
					MatchmakingConfig: tc.config,
					GameSession: &mm.GameSession{
						Password: tc.password,
					},
				},
			})

			if err != nil {
				t.Fatal(err)
			}

			if ticket.GameSession.IsPublic != tc.public {
				t.Fatal("wrong assigned visibility")
			}
			// Exercise the actual snapshot initializer against the assigned room,
			// rather than checking only the unary matchmaking flag.
			room := ticket.GameSession
			room.Name = "tenants/t-adf89f68-lp1/gameSessions/test"
			room.CanParticipate = true
			member := room.Name + "/userSessions/host"
			room.UserSessions = []*mm.UserSession{
				{
					Name: member,
					User: owner,
				},
			}
			rooms.tickets[ticket.Name].ticket = ticket
			sync := &gamesyncServer{
				rooms: rooms,
			}

			if err := sync.initializeSessionDocuments(owner + "|" + member); err != nil {
				t.Fatal(err)
			}

			docs := sync.sessionSnapshot(member, &gs.Target{
				TargetType: &gs.Target_Documents{
					Documents: &gs.DocumentsTarget{
						Documents: []string{"docs/__gs/s"},
					},
				},
			})

			if len(docs) != 1 {
				t.Fatal("missing admission snapshot")
			}

			fields := docs[0].Fields.Fields

			if !fields["cp"].GetBooleanValue() || fields["ip"].GetBooleanValue() != tc.public || fields["pw"].GetStringValue() != tc.password {
				t.Fatal("snapshot differs from assigned room")
			}

			if !strings.HasSuffix(docs[0].Name, "__gs/s") {
				t.Fatal("wrong target")
			}
		})
	}
}
