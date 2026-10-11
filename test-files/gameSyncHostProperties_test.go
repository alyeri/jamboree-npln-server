package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func postmatchFixture(t *testing.T) (*gamesyncServer, context.Context, *gs.WriteDocumentsRequest, *creationRecord, string) {
	t.Helper()

	key := bytes.Repeat([]byte{7}, 32)
	room := "tenants/t-adf89f68-lp1/gameSessions/postmatch"
	host, guest := "tenants/current/users/host", "tenants/current/users/guest"
	hostProof := host + "|" + room + "/userSessions/host"
	guestProof := guest + "|" + room + "/userSessions/guest"
	props := &common.MapValue{Fields: map[string]*common.Value{
		"_Bex_PropIsHost": textValue("host"), "_Bex_PropIsVoiceChat": intValue(0),
		"gameMode": intValue(-1), "searchWord": textValue("synthetic-search-word"), "uniqueId": intValue(-9223372036854775808)}}
	resource := &mm.GameSession{Name: room, State: mm.GameSession_ACTIVE, Properties: props, UserSessions: []*mm.UserSession{
		{
			User: host,
			Name: room + "/userSessions/host",
		}, {
			User: guest,
			Name: room + "/userSessions/guest",
		}}}
	record := &creationRecord{owner: host, expires: time.Now().Add(time.Minute), entryClosed: true, ticket: &mm.GameSessionCreationTicket{
		MatchmakingConfig: "tenants/current/matchmakingConfigs/BqP4MatchmakingConfigs", GameSession: resource,
		UserDefinitions: []*mm.UserDefinition{
			{
				User: host,
			},
			{
				User: guest,
			},
		}}}
	p := &serverContext{
		writer:      &bytes.Buffer{},
		identityKey: key,
	}
	s := &gamesyncServer{
		p: p,
		rooms: &gameSessionServer{
			p: p,
			tickets: map[string]*creationRecord{
				"ticket": record,
			},
		},
	}

	if err := s.initializeSessionDocuments(guestProof); err != nil {
		t.Fatal(err)
	}

	s.documents[room]["docs/__gs/m"].Fields.Fields["cp"] = &common.Value{
		ValueType: &common.Value_BooleanValue{
			BooleanValue: false,
		},
	}
	s.documents[room]["docs/__gs/m"].Fields.Fields["ip"] = &common.Value{
		ValueType: &common.Value_BooleanValue{
			BooleanValue: false,
		},
	}
	s.epochs = map[string]uint64{
		hostProof:  1,
		guestProof: 1,
	}
	s.subs = map[string]map[*gsSubscriber]bool{
		room: {
			&gsSubscriber{
				proof:   guestProof,
				stream:  &controlTestStream{},
				targets: map[string]*gs.Target{},
			}: true,
		},
	}
	token, err := (&authServer{
		key: key,
	}).signSessionToken(guestProof, "gamesync-access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
	changed := proto.Clone(props).(*common.MapValue)
	changed.Fields["_Bex_PropIsHost"] = textValue("pending") // Captured lengths 4 -> 7; values are synthetic.
	req := &gs.WriteDocumentsRequest{WriteOperations: []*gs.WriteOperation{{OperationType: &gs.WriteOperation_UpdateDocument{UpdateDocument: &gs.UpdateDocumentRequest{
		Document: &gs.Document{
			Name: "docs/__gs/m",
			Fields: &common.MapValue{
				Fields: map[string]*common.Value{
					"prp": {
						ValueType: &common.Value_MapValue{
							MapValue: changed,
						},
					},
				},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"`prp`"},
		}, CurrentDocument: &gs.Precondition{
			ConditionType: &gs.Precondition_Exists{
				Exists: true,
			},
		}}}}}}

	return s, ctx, req, record, hostProof
}

func TestPostmatchCapturedPropertiesWrite(t *testing.T) {
	s, ctx, req, record, _ := postmatchFixture(t)
	owner := record.owner

	if _, err := s.WriteDocuments(ctx, req); err != nil {
		t.Fatalf("captured-shaped postmatch update rejected: %v", err)
	}

	if record.owner != owner || record.ticket.GameSession.CanParticipate || record.ticket.GameSession.IsPublic || !record.entryClosed {
		t.Fatal("property update transferred ownership or reopened room")
	}

	if record.ticket.GameSession.Properties.Fields["_Bex_PropIsHost"].GetStringValue() != "pending" {
		t.Fatal("property update not committed")
	}
}

func TestPostmatchPropertiesAfterOwnerReopensRoom(t *testing.T) {
	for _, flags := range [][2]bool{
		{
			false,
			false,
		},
		{
			false,
			true,
		},
		{
			true,
			false,
		},
		{
			true,
			true,
		},
	} {
		s, ctx, req, record, _ := postmatchFixture(t)
		control := s.documents[record.ticket.GameSession.Name]["docs/__gs/m"].Fields.Fields
		control["cp"] = &common.Value{
			ValueType: &common.Value_BooleanValue{
				BooleanValue: flags[0],
			},
		}
		control["ip"] = &common.Value{
			ValueType: &common.Value_BooleanValue{
				BooleanValue: flags[1],
			},
		}
		record.entryClosed = !flags[0]
		record.ticket.GameSession.CanParticipate = flags[0]
		record.ticket.GameSession.IsPublic = flags[1]
		owner := record.owner

		if _, err := s.WriteDocuments(ctx, req); err != nil {
			t.Fatalf("unchanged admission flags %v rejected: %v", flags, err)
		}

		if record.owner != owner || record.entryClosed != !flags[0] || record.ticket.GameSession.CanParticipate != flags[0] || record.ticket.GameSession.IsPublic != flags[1] || control["cp"].GetBooleanValue() != flags[0] || control["ip"].GetBooleanValue() != flags[1] {
			t.Fatal("host marker update changed owner or admission")
		}
	}
}

func TestPostmatchPropertiesRetainsRestrictions(t *testing.T) {
	cases := map[string]func(*gamesyncServer, *gs.WriteDocumentsRequest, *creationRecord, string){
		"owner-connected": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			s.subs[c.ticket.GameSession.Name][&gsSubscriber{
				proof: p,
			}] = true
		},
		"owner-never-connected": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			delete(s.epochs, p)
		},
		"caller-disconnected": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			s.subs[c.ticket.GameSession.Name] = nil
		},
		"foreign-member": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			c.ticket.GameSession.UserSessions = c.ticket.GameSession.UserSessions[:1]
		},
		"expired-room": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			c.expires = time.Now().Add(-time.Second)
		},
		"terminated-room": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			c.ticket.GameSession.State = mm.GameSession_TERMINATED
		},
		"invalid-admission-type": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			s.documents[c.ticket.GameSession.Name]["docs/__gs/m"].Fields.Fields["cp"] = textValue("true")
		},
		"other-config": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			c.ticket.MatchmakingConfig = "other"
		},
		"changed-voice": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			r.WriteOperations[0].GetUpdateDocument().Document.Fields.Fields["prp"].GetMapValue().Fields["_Bex_PropIsVoiceChat"] = intValue(1)
		},
		"changed-search": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			r.WriteOperations[0].GetUpdateDocument().Document.Fields.Fields["prp"].GetMapValue().Fields["searchWord"] = textValue("different")
		},
		"omitted-property": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			delete(r.WriteOperations[0].GetUpdateDocument().Document.Fields.Fields["prp"].GetMapValue().Fields, "gameMode")
		},
		"changed-admission": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			u := r.WriteOperations[0].GetUpdateDocument()
			u.UpdateMask.Paths = append(u.UpdateMask.Paths, "`cp`")
			u.Document.Fields.Fields["cp"] = &common.Value{
				ValueType: &common.Value_BooleanValue{
					BooleanValue: true,
				},
			}
		},
		"changed-password": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			u := r.WriteOperations[0].GetUpdateDocument()
			u.UpdateMask.Paths = []string{"`pw`"}
			u.Document.Fields.Fields["pw"] = textValue("new")
		},
		"missing-precondition": func(s *gamesyncServer, r *gs.WriteDocumentsRequest, c *creationRecord, p string) {
			r.WriteOperations[0].GetUpdateDocument().CurrentDocument = nil
		},
	}

	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s, ctx, req, record, host := postmatchFixture(t)

			mutate(s, req, record, host)
			// Mutations to the existing room are fixture setup, not partial writes.
			before := proto.Clone(s.documents[record.ticket.GameSession.Name]["docs/__gs/m"])

			if _, err := s.WriteDocuments(ctx, req); status.Code(err) != codes.PermissionDenied {
				t.Fatalf("unsafe update accepted: %v", err)
			}

			if !proto.Equal(before, s.documents[record.ticket.GameSession.Name]["docs/__gs/m"]) {
				t.Fatal("rejected write changed document")
			}
		})
	}
}

func TestPostmatchPropertiesFailedBatchIsAtomic(t *testing.T) {
	s, ctx, req, record, _ := postmatchFixture(t)
	before := proto.Clone(s.documents[record.ticket.GameSession.Name]["docs/__gs/m"])
	unauthorized := proto.Clone(req.WriteOperations[0]).(*gs.WriteOperation)
	u := unauthorized.GetUpdateDocument()
	u.UpdateMask.Paths = []string{"`cp`"}
	u.Document.Fields = &common.MapValue{
		Fields: map[string]*common.Value{
			"cp": {
				ValueType: &common.Value_BooleanValue{
					BooleanValue: true,
				},
			},
		},
	}
	req.WriteOperations = append(req.WriteOperations, unauthorized)

	if _, err := s.WriteDocuments(ctx, req); status.Code(err) != codes.PermissionDenied {
		t.Fatal("unauthorized batch accepted", err)
	}

	if !proto.Equal(before, s.documents[record.ticket.GameSession.Name]["docs/__gs/m"]) || record.ticket.GameSession.Properties.Fields["_Bex_PropIsHost"].GetStringValue() != "host" {
		t.Fatal("failed batch partially changed document or matchmaking properties")
	}
}
