package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestQueryDoesNotOfferAssignedOldRoom(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	user := "tenants/t-adf89f68-lp1/users/guest"
	word := strings.Repeat("a", 32)
	old := &mm.GameSession{
		Name:                    "old",
		State:                   mm.GameSession_ACTIVE,
		MaxParticipantCount:     4,
		CurrentParticipantCount: 2,
		CanParticipate:          true,
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"gameMode":   intValue(-1),
				"searchWord": textValue(word),
			},
		},
		UserSessions: []*mm.UserSession{
			{
				User: user,
			},
		},
	}
	current := proto.Clone(old).(*mm.GameSession)
	current.Name = "new"
	current.State = mm.GameSession_CREATING
	current.Properties.Fields["gameMode"] = intValue(4)
	s := &gameSessionServer{p: &serverContext{
		writer:      &bytes.Buffer{},
		identityKey: key,
	}, tickets: map[string]*creationRecord{
		"old": {
			owner:   "host",
			expires: time.Now().Add(time.Minute),
			ticket: &mm.GameSessionCreationTicket{
				GameSession: old,
			},
		},
		"new": {
			owner:   "host",
			expires: time.Now().Add(time.Minute),
			ticket: &mm.GameSessionCreationTicket{
				GameSession: current,
			},
		},
	}}
	token, err := (&authServer{
		key: key,
	}).signSessionToken(user, "access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))

	go func() {
		time.Sleep(300 * time.Millisecond)

		s.mu.Lock()

		current.State = mm.GameSession_ACTIVE

		s.mu.Unlock()
	}()

	result, err := s.QueryGameSessions(ctx, &mm.QueryGameSessionsRequest{
		GameSessionSearchConfig: "BqGameSessionSearchConfigs",
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"gameMode":   intValue(4),
				"searchWord": textValue(word),
			},
		},
	})

	if err != nil || len(result.GetGameSessions()) != 1 || result.GameSessions[0].Name != "new" {
		t.Fatalf("wrong return room selected: %v %v", result, err)
	}
}

func TestReturnQueryRejectsOldGameMode(t *testing.T) {
	room := &mm.GameSession{
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"gameMode":   intValue(-1),
				"searchWord": textValue("synthetic-code"),
			},
		},
	}
	req := &mm.QueryGameSessionsRequest{
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"gameMode":   intValue(4),
				"searchWord": textValue("synthetic-code"),
			},
		},
	}

	if matchesRoomQuery(room, req) {
		t.Fatal("old mode -1 matched requested mode 4")
	}

	room.Properties.Fields["gameMode"] = intValue(4)

	if !matchesRoomQuery(room, req) {
		t.Fatal("matching return room rejected")
	}

	room.Properties.Fields["searchWord"] = textValue("different-code")

	if matchesRoomQuery(room, req) {
		t.Fatal("wrong return code matched")
	}

	delete(room.Properties.Fields, "gameMode")

	if matchesRoomQuery(room, req) {
		t.Fatal("missing required mode matched")
	}
}

func TestPublicQueryEmptyWordAndUsers(t *testing.T) {
	room := &mm.GameSession{
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"gameMode": intValue(1279),
			},
		},
		UserSessions: []*mm.UserSession{
			{
				User: "tenants/t-adf89f68-lp1/users/host",
			},
		},
	}
	req := &mm.QueryGameSessionsRequest{
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"gameMode":   intValue(1279),
				"searchWord": textValue(""),
			},
		},
		Users: []string{"tenants/current/users/host"},
	}
	before := proto.Clone(room)

	if !matchesRoomQuery(room, req) {
		t.Fatal("captured public-room query rejected")
	}

	req.Users = []string{"tenants/current/users/other"}

	if matchesRoomQuery(room, req) {
		t.Fatal("foreign user room matched")
	}

	req.Users = nil
	req.Properties.Fields["gameMode"] = textValue("1279")

	if matchesRoomQuery(room, req) {
		t.Fatal("wrong property type matched")
	}

	if !proto.Equal(room, before) {
		t.Fatal("query mutated source room")
	}
}
