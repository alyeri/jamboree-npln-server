package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestJoinLatencySurvivesSessionReadAndGamesync(t *testing.T) {
	for _, nonempty := range []bool{
		false,
		true,
	} {
		name := "empty-message"

		if nonempty {
			name = "measured-region"
		}

		t.Run(name, func(t *testing.T) {
			key := bytes.Repeat([]byte{6}, 32)

			var logs bytes.Buffer

			p := &serverContext{
				identityKey: key,
				writer:      &logs,
			}
			rname := canonicalRoom("example")
			room := &mm.GameSession{
				Name:                    rname,
				MaxParticipantCount:     4,
				CurrentParticipantCount: 1,
				CanParticipate:          true,
				State:                   mm.GameSession_ACTIVE,
				UserSessions: []*mm.UserSession{
					{
						Name: rname + "/userSessions/host",
						User: "host",
					},
				},
			}
			s := &gameSessionServer{
				p: p,
				tickets: map[string]*creationRecord{"r": {
					owner:   "host",
					expires: time.Now().Add(time.Minute),
					ticket: &mm.GameSessionCreationTicket{
						GameSession: room,
						UserDefinitions: []*mm.UserDefinition{
							{
								User: "host",
							},
						},
					},
				}},
			}
			latency := &mm.LatencyData{
				Latencies: map[string]*durationpb.Duration{},
			}
			expected := `{"latencies":{}}`

			if nonempty {
				latency.Latencies["test-region"] = &durationpb.Duration{
					Seconds: 1,
					Nanos:   20_000_000,
				}
				expected = `{"latencies":{"test-region":{"nanos":1020000000}}}`
			}

			token, _ := (&authServer{
				key: key,
			}).signSessionToken("guest", "access", time.Minute)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
			request := &mm.JoinGameSessionRequest{
				Name: rname,
				UserDefinitions: []*mm.UserDefinition{
					{
						LatencyData: latency,
					},
				},
			}
			joined, err := s.JoinGameSession(ctx, request)

			if err != nil {
				t.Fatal(err)
			}

			read, err := s.GetGameSession(ctx, &mm.GetGameSessionRequest{
				Name: rname,
			})

			if err != nil {
				t.Fatal(err)
			}

			if !proto.Equal(joined.GameSession.UserSessions[1].LatencyData, latency) || !proto.Equal(read.UserSessions[1].LatencyData, latency) {
				t.Fatal("join or subsequent read lost latency data")
			}

			gs := &gamesyncServer{
				p:     p,
				rooms: s,
			}

			if err := gs.initializeSessionDocuments("host|" + rname + "/userSessions/host"); err != nil {
				t.Fatal(err)
			}

			if err := gs.initializeSessionDocuments("guest|" + joined.MatchedUserSessions[0].UserSession); err != nil {
				t.Fatal(err)
			}

			if got := gs.documents[rname]["docs/__us/guest"].Fields.Fields["ltc"].GetStringValue(); got != expected {
				t.Fatalf("inconsistent Gamesync latency: %s", got)
			}

			latency.Latencies["test-region"] = &durationpb.Duration{
				Seconds: 99,
			}

			if got := gamesyncLatencyJSON(room.UserSessions[1].LatencyData); got != expected {
				t.Fatal("caller mutation changed stored participant")
			}
		})
	}
}
