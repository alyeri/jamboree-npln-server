package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

type controlTestStream struct {
	grpc.ServerStream
	messages []*gs.KeepUserSessionResponse
}

func (s *controlTestStream) Send(r *gs.KeepUserSessionResponse) error {
	s.messages = append(s.messages, proto.Clone(r).(*gs.KeepUserSessionResponse))

	return nil
}
func (s *controlTestStream) Recv() (*gs.KeepUserSessionRequest, error) {
	panic("unused")
}

func TestHostControlTransitionPublishesSessionState(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		writer:      &bytes.Buffer{},
		identityKey: key,
	}
	room := "tenants/t-adf89f68-lp1/gameSessions/control"
	owner := "tenants/current/users/host"
	member := room + "/userSessions/host"
	resource := &mm.GameSession{
		Name:           room,
		CanParticipate: true,
		IsPublic:       true,
		UserSessions: []*mm.UserSession{
			{
				Name: member,
				User: owner,
			},
		},
	}
	rooms := &gameSessionServer{
		p: p,
		tickets: map[string]*creationRecord{
			"ticket": {
				owner: owner,
				ticket: &mm.GameSessionCreationTicket{
					GameSession: resource,
					UserDefinitions: []*mm.UserDefinition{
						{
							User: owner,
						},
					},
				},
			},
		},
	}
	s := &gamesyncServer{
		p:     p,
		rooms: rooms,
	}

	if err := s.initializeSessionDocuments(owner + "|" + member); err != nil {
		t.Fatal(err)
	}

	hostStream, guestStream := &controlTestStream{}, &controlTestStream{}
	target := &gs.Target{
		TargetType: &gs.Target_Documents{
			Documents: &gs.DocumentsTarget{
				Documents: []string{"docs/__gs/s"},
			},
		},
	}
	host := &gsSubscriber{
		proof:  owner + "|" + member,
		stream: hostStream,
		targets: map[string]*gs.Target{
			"state": target,
		},
	}
	guest := &gsSubscriber{
		proof:  "guest|" + room + "/userSessions/guest",
		stream: guestStream,
		targets: map[string]*gs.Target{
			"state": target,
		},
	}
	s.subs = map[string]map[*gsSubscriber]bool{
		room: {
			host:  true,
			guest: true,
		},
	}
	ctxFor := func(proof string) context.Context {
		token, _ := (&authServer{
			key: key,
		}).signSessionToken(proof, "gamesync-access", time.Minute)

		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
	}
	update := &gs.WriteOperation{OperationType: &gs.WriteOperation_UpdateDocument{UpdateDocument: &gs.UpdateDocumentRequest{
		Document: &gs.Document{
			Name: "docs/__gs/m",
			Fields: &common.MapValue{
				Fields: map[string]*common.Value{
					"ip": {
						ValueType: &common.Value_BooleanValue{
							BooleanValue: false,
						},
					},
				},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"`ip`"},
		},
		CurrentDocument: &gs.Precondition{
			ConditionType: &gs.Precondition_Exists{
				Exists: true,
			},
		},
	}}}
	ctx := ctxFor(owner + "|" + member)
	invalid := &gs.WriteOperation{
		OperationType: &gs.WriteOperation_DeleteDocument{
			DeleteDocument: &gs.DeleteDocumentRequest{
				Name: "docs/../invalid",
			},
		},
	}

	if _, err := s.WriteDocuments(ctx, &gs.WriteDocumentsRequest{
		WriteOperations: []*gs.WriteOperation{
			update,
			invalid,
		},
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}

	if !resource.IsPublic || !s.documents[room]["docs/__gs/m"].Fields.Fields["ip"].GetBooleanValue() || len(hostStream.messages) != 0 {
		t.Fatal("failed batch partially changed control")
	}

	if _, err := s.WriteDocuments(ctxFor(guest.proof), &gs.WriteDocumentsRequest{
		WriteOperations: []*gs.WriteOperation{
			update,
		},
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("guest altered host control", err)
	}

	result, err := s.WriteDocuments(ctx, &gs.WriteDocumentsRequest{
		WriteOperations: []*gs.WriteOperation{
			update,
		},
	})

	if err != nil || len(result.GetWriteResults()) != 1 {
		t.Fatal("captured exists=true write rejected", err)
	}

	if resource.IsPublic || !resource.CanParticipate {
		t.Fatal("matchmaking state not updated or participation lost")
	}

	doc, err := s.GetDocument(ctx, &gs.GetDocumentRequest{
		Name: "docs/__gs/m",
	})

	if err != nil || doc.Fields.Fields["ip"].GetBooleanValue() || !doc.Fields.Fields["cp"].GetBooleanValue() {
		t.Fatal("mutable read differs from committed state", err)
	}

	for _, stream := range []*controlTestStream{
		hostStream,
		guestStream,
	} {
		if len(stream.messages) != 1 {
			t.Fatal("state notification missing")
		}

		change := stream.messages[0].GetDocumentChange()

		if change.DocumentChangeType != gs.DocumentChange_UPDATED || change.Document.Name != "docs/__gs/s" || change.Document.Fields.Fields["ip"].GetBooleanValue() {
			t.Fatal("wrong state notification")
		}
	}
}
