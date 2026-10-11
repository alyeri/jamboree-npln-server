package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestHostEntryClosureSurvivesVacancyCleanup(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		reservation bool
		closed      bool
	}{
		{
			"disconnected",
			false,
			true,
		},
		{
			"reservation",
			true,
			true,
		},
		{
			"visibility-only",
			false,
			false,
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			key := bytes.Repeat([]byte{7}, 32)
			p := &serverContext{
				writer:      &bytes.Buffer{},
				identityKey: key,
			}
			room := canonicalRoom(scenario.name)
			host := "tenants/current/users/host"
			guest := "tenants/current/users/guest"
			member := room + "/userSessions/host"
			guestSession := room + "/userSessions/guest"
			guestProof := guest + "|" + guestSession
			now := time.Now()
			resource := &mm.GameSession{
				Name: room, State: mm.GameSession_ACTIVE, MaxParticipantCount: 2,
				CurrentParticipantCount: 2,
				UserSessions: []*mm.UserSession{
					{
						Name:  member,
						User:  host,
						State: mm.UserSession_ACTIVE,
					},
					{Name: guestSession, User: guest, State: mm.UserSession_RESERVED,
						CreateTime: timestamppb.New(now.Add(-2 * time.Minute))},
				},
			}
			record := &creationRecord{owner: host, expires: now.Add(time.Hour),
				ticket: &mm.GameSessionCreationTicket{GameSession: resource,
					UserDefinitions: []*mm.UserDefinition{
						{
							User: host,
						},
					}}}
			rooms := &gameSessionServer{
				p: p,
				tickets: map[string]*creationRecord{
					"room": record,
				},
			}
			s := &gamesyncServer{
				p:     p,
				rooms: rooms,
				epochs: map[string]uint64{
					guestProof: 1,
				},
			}

			if err := s.initializeSessionDocuments(host + "|" + member); err != nil {
				t.Fatal(err)
			}

			token, _ := (&authServer{
				key: key,
			}).signSessionToken(host+"|"+member, "gamesync-access", time.Minute)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
			setEntry := func(allowed bool) {
				t.Helper()

				_, err := s.WriteDocuments(ctx, &gs.WriteDocumentsRequest{WriteOperations: []*gs.WriteOperation{{
					OperationType: &gs.WriteOperation_UpdateDocument{UpdateDocument: &gs.UpdateDocumentRequest{
						Document: &gs.Document{Name: "docs/__gs/m", Fields: &common.MapValue{Fields: map[string]*common.Value{
							"cp": {
								ValueType: &common.Value_BooleanValue{
									BooleanValue: allowed,
								},
							},
						}}},
						UpdateMask: &fieldmaskpb.FieldMask{
							Paths: []string{"cp"},
						},
					}},
				}}})

				if err != nil {
					t.Fatal(err)
				}
			}

			if scenario.closed {
				setEntry(false)
			} else {
				_, err := s.WriteDocuments(ctx, &gs.WriteDocumentsRequest{WriteOperations: []*gs.WriteOperation{{
					OperationType: &gs.WriteOperation_UpdateDocument{UpdateDocument: &gs.UpdateDocumentRequest{
						Document: &gs.Document{Name: "docs/__gs/m", Fields: &common.MapValue{Fields: map[string]*common.Value{
							"ip": {
								ValueType: &common.Value_BooleanValue{
									BooleanValue: true,
								},
							},
						}}},
						UpdateMask: &fieldmaskpb.FieldMask{
							Paths: []string{"ip"},
						},
					}},
				}}})

				if err != nil {
					t.Fatal(err)
				}
			}

			if scenario.reservation {
				rooms.expirePendingRoomJoins(record, now)
			} else {
				s.expireDisconnectedSession(guestProof, room, 1)
			}

			if resource.CurrentParticipantCount != 1 || resource.CanParticipate == scenario.closed || discoverableRoom(record) == scenario.closed {
				t.Fatal("vacancy cleanup did not preserve the host entry policy")
			}

			for _, document := range []string{"docs/__gs/m", "docs/__gs/s"} {
				if s.documents[room][document].GetFields().GetFields()["cp"].GetBooleanValue() {
					t.Fatal("closed control state changed", document)
				}
			}

			setEntry(true)

			if !discoverableRoom(record) || record.entryClosed {
				t.Fatal("host could not reopen the vacant room")
			}
		})
	}
}

func TestAssignedParticipantCannotJoinTerminatedRoom(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	guest := "tenants/current/users/guest"
	room := &mm.GameSession{Name: canonicalRoom("terminated"), State: mm.GameSession_TERMINATED,
		UserSessions: []*mm.UserSession{
			{
				User: guest,
				Name: "guest-session",
			},
		}}
	s := &gameSessionServer{p: &serverContext{
		writer:      &bytes.Buffer{},
		identityKey: key,
	},
		tickets: map[string]*creationRecord{"room": {owner: "host", expires: time.Now().Add(time.Hour),
			ticket: &mm.GameSessionCreationTicket{
				GameSession: room,
			}}}}
	token, _ := (&authServer{
		key: key,
	}).signSessionToken(guest, "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token,
		"npln-tenant-id", "t-adf89f68-lp1"))
	result, err := s.JoinGameSession(ctx, &mm.JoinGameSessionRequest{
		Name: room.Name,
	})

	if status.Code(err) != codes.FailedPrecondition || result != nil {
		t.Fatal("terminated room issued a matchmaking token", err)
	}
}
