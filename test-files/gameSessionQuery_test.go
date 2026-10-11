package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestPublicDiscoveryWithoutFriendship(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	rooms := &gameSessionServer{
		p: &serverContext{
			writer:      &logs,
			identityKey: key,
		},
		tickets: map[string]*creationRecord{},
	}

	for _, item := range []struct {
		name     string
		max      int32
		password string
		state    mm.GameSession_State
	}{
		{
			"four",
			4,
			"1234",
			mm.GameSession_ACTIVE,
		},
		{
			"eight",
			8,
			"",
			mm.GameSession_ACTIVE,
		},
		{
			"closed",
			4,
			"",
			mm.GameSession_TERMINATED,
		},
	} {
		rooms.tickets[item.name] = &creationRecord{
			owner:   "tenants/t-adf89f68-lp1/users/host",
			expires: time.Now().Add(time.Hour),
			ticket: &mm.GameSessionCreationTicket{
				GameSession: &mm.GameSession{
					Name:                    canonicalRoom(item.name),
					MaxParticipantCount:     item.max,
					CurrentParticipantCount: 1,
					CanParticipate:          true,
					Password:                item.password,
					State:                   item.state,
				},
			},
		}
	}

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/t-adf89f68-lp1/users/visitor", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	directory := (&friendsServer{
		rooms: rooms,
	}).publicRoomHosts("tenants/t-adf89f68-lp1/users/visitor")

	if len(directory.FriendAccounts) != 1 || len(directory.FriendAccounts[0].Users) != 1 {
		t.Fatal("public host missing or duplicated")
	}

	own := (&friendsServer{
		rooms: rooms,
	}).publicRoomHosts("tenants/t-adf89f68-lp1/users/host")

	if len(own.FriendAccounts) != 0 {
		t.Fatal("own host announced")
	}

	found, e := rooms.QueryGameSessions(ctx, &mm.QueryGameSessionsRequest{})

	if e != nil || len(found.GameSessions) != 2 {
		t.Fatal("active rooms missing", e)
	}

	for _, room := range found.GameSessions {
		if room.Password != "" {
			t.Fatal("password leaked")
		}
	}

	fullList, e := rooms.QueryGameSessions(ctx, &mm.QueryGameSessionsRequest{
		View: mm.GameSessionView_FULL,
	})

	if e != nil || len(fullList.GameSessions) != 2 {
		t.Fatal("full discovery unavailable", e)
	}

	for _, room := range fullList.GameSessions {
		if room.Name == canonicalRoom("four") && room.Password != "*" {
			t.Fatal("full list lost native password marker")
		}

		if room.Name == canonicalRoom("eight") && room.Password != "" {
			t.Fatal("unprotected room marked as protected")
		}

		if len(room.ProtoReflect().GetUnknown()) != 0 {
			t.Fatal("speculative wire flag retained")
		}
	}

	filtered, e := rooms.QueryGameSessions(ctx, &mm.QueryGameSessionsRequest{
		GameSessionSearchConfig: "BqP8SearchConfigs",
	})

	if e != nil || len(filtered.GameSessions) != 1 || filtered.GameSessions[0].MaxParticipantCount != 8 {
		t.Fatal("capacity filter", e)
	}

	read, e := rooms.GetGameSession(ctx, &mm.GetGameSessionRequest{
		Name: canonicalRoom("four"),
	})

	if e != nil || read.Password != "" {
		t.Fatal("foreign read unavailable or secret leaked", e)
	}

	full, e := rooms.GetGameSession(ctx, &mm.GetGameSessionRequest{
		Name: canonicalRoom("four"),
		View: mm.GameSessionView_FULL,
	})

	if e != nil || full.Password != "*" {
		t.Fatal("full room view lost native password marker", e)
	}

	if len(full.ProtoReflect().GetUnknown()) != 0 {
		t.Fatal("speculative wire flag retained in room read")
	}

	if rooms.tickets["four"].ticket.GameSession.Password != "1234" {
		t.Fatal("room password mutated by discovery")
	}

	hostToken, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/t-adf89f68-lp1/users/host", "access", time.Minute)
	hostCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", hostToken, "npln-tenant-id", "t-adf89f68-lp1"))
	hostRoom, e := rooms.GetGameSession(hostCtx, &mm.GetGameSessionRequest{
		Name: canonicalRoom("four"),
		View: mm.GameSessionView_FULL,
	})

	if e != nil || hostRoom.Password != "1234" {
		t.Fatal("owner password changed", e)
	}

	fullBatch, e := rooms.BatchGetGameSessions(ctx, &mm.BatchGetGameSessionsRequest{
		Names: []string{
			canonicalRoom("four"),
		},
		View: mm.GameSessionView_FULL,
	})

	if e != nil || len(fullBatch.GameSessions) != 1 || fullBatch.GameSessions[0].Password != "*" {
		t.Fatal("batch read lost native password marker", e)
	}

	batch, e := rooms.BatchGetGameSessions(ctx, &mm.BatchGetGameSessionsRequest{
		Names: []string{
			canonicalRoom("four"),
			canonicalRoom("closed"),
		},
	})

	if e != nil || len(batch.GameSessions) != 1 {
		t.Fatal("batch listing includes closed room", e)
	}
}

func TestFriendAccountIDIsSeparateFromNplnUser(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	p := &serverContext{
		writer:      &logs,
		identityKey: key,
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))
	user, e := (&authServer{
		key:           key,
		serverContext: p,
	}).CreateUser(ctx, &authpb.CreateUserRequest{
		Parent: "tenants/current",
		ExternalIdToken: &authpb.ExternalIdToken{
			Token: &authpb.ExternalIdToken_DummyExtIdToken{
				DummyExtIdToken: "directory-test",
			},
		},
	})

	if e != nil {
		t.Fatal(e)
	}

	canonical := canonicalUserSession(user.Name)
	room := &creationRecord{
		owner:   user.Name,
		expires: time.Now().Add(time.Hour),
		ticket: &mm.GameSessionCreationTicket{
			GameSession: &mm.GameSession{
				State:                   mm.GameSession_ACTIVE,
				CanParticipate:          true,
				MaxParticipantCount:     4,
				CurrentParticipantCount: 1,
			},
		},
	}
	directory := (&friendsServer{
		p: p,
		rooms: &gameSessionServer{
			tickets: map[string]*creationRecord{
				"room": room,
			},
		},
	}).publicRoomHosts("visitor")
	account := directory.FriendAccounts[0]

	if len(account.NsaId) != 16 || account.NsaId != p.accountIDs[canonical] {
		t.Fatal("account identifier not 64-bit hexadecimal")
	}

	if account.Users[0].FriendUser != canonical || !strings.HasSuffix(account.Users[0].Name, "/friendUsers/"+strings.TrimPrefix(user.Name, "tenants/current/users/")) {
		t.Fatal("friend resource identifier conflated with account")
	}
}

func TestVisitorCanResolvePublishedRoomCode(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("visitor", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	room := canonicalRoom(strings.Repeat("a", 32))
	alias := &mm.GameSessionShortAlias{
		Name:        "tenants/t-adf89f68-lp1/GameSessionShortAliases/DDRM73",
		GameSession: room,
	}
	s := &gameSessionServer{
		p: &serverContext{
			writer:      &logs,
			identityKey: key,
		},
		tickets: map[string]*creationRecord{
			"r": {
				owner:   "host",
				expires: time.Now().Add(time.Hour),
				alias:   alias,
				ticket: &mm.GameSessionCreationTicket{
					GameSession: &mm.GameSession{
						Name:                    room,
						State:                   mm.GameSession_ACTIVE,
						CanParticipate:          true,
						CurrentParticipantCount: 1,
						MaxParticipantCount:     4,
					},
				},
			},
		},
	}
	result, e := s.CreateGameSessionShortAlias(ctx, &mm.CreateGameSessionShortAliasRequest{
		Parent: "tenants/current",
		GameSessionShortAlias: &mm.GameSessionShortAlias{
			GameSession: room,
		},
	})

	if e != nil || result.Name != alias.Name {
		t.Fatal("visitor could not read existing code", e)
	}

	for _, name := range []string{
		"ddrm73",
		"tenants/current/gameSessionShortAliases/DDRM73\x00",
		alias.Name,
		"tenants/current/GameSessionShortAliases/" + strings.Repeat("A", 32),
	} {
		resolved, e := s.GetGameSessionShortAlias(ctx, &mm.GetGameSessionShortAliasRequest{
			Name: name,
		})

		if e != nil || resolved.GameSession != room {
			t.Fatal("alias normalization", e)
		}
	}

	s.tickets["r"].alias = nil

	if _, e := s.CreateGameSessionShortAlias(ctx, &mm.CreateGameSessionShortAliasRequest{
		Parent: "tenants/current",
		GameSessionShortAlias: &mm.GameSessionShortAlias{
			GameSession: room,
		},
	}); e == nil {
		t.Fatal("visitor allocated host code")
	}
}
