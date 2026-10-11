package main

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestEightPlayerSessionSequencesSurviveGuestDeparture(t *testing.T) {
	room := canonicalRoom("sequence-test")
	resource := &mm.GameSession{
		Name:                room,
		MaxParticipantCount: 8,
	}

	for index := 1; index <= 8; index++ {
		user := fmt.Sprintf("tenants/t-adf89f68-lp1/users/player%d", index)
		resource.UserSessions = append(resource.UserSessions, &mm.UserSession{
			Name: room + "/userSessions/" + fmt.Sprintf("player%d", index),
			User: user,
		})
	}

	server := &gamesyncServer{
		p: &serverContext{
			writer: &bytes.Buffer{},
		},
		rooms: &gameSessionServer{
			tickets: map[string]*creationRecord{
				"room": {
					expires: time.Now().Add(time.Hour),
					ticket: &mm.GameSessionCreationTicket{
						GameSession: resource,
						UserDefinitions: []*mm.UserDefinition{
							{},
						},
					},
				},
			},
		},
	}

	initialize := func(member *mm.UserSession) {
		t.Helper()

		if err := server.initializeSessionDocuments(member.User + "|" + member.Name); err != nil {
			t.Fatal(err)
		}
	}

	initialize(resource.UserSessions[0])

	// Removing the second guest compacts the matchmaking list, but the
	// remaining players must keep their original sequence numbers.
	resource.UserSessions = append(resource.UserSessions[:1], resource.UserSessions[2:]...)

	delete(server.documents[room], "docs/__us/player2")

	initialize(resource.UserSessions[len(resource.UserSessions)-1])

	if value := server.documents[room]["docs/__us/player8"].Fields.Fields["ussid"].GetIntegerValue(); value != 8 {
		t.Fatalf("player8 was renumbered after another player left: %d", value)
	}

	newMember := &mm.UserSession{
		Name: room + "/userSessions/newcomer",
		User: "tenants/t-adf89f68-lp1/users/newcomer",
	}
	resource.UserSessions = append(resource.UserSessions, newMember)

	initialize(newMember)

	if value := server.documents[room]["docs/__us/newcomer"].Fields.Fields["ussid"].GetIntegerValue(); value != 9 {
		t.Fatalf("new participant reused an existing sequence: %d", value)
	}
}

func TestDepartedSessionSequenceIsNotReused(t *testing.T) {
	server := &gamesyncServer{
		documents: map[string]map[string]*gs.Document{
			"room": {
				"guest": {
					Fields: &common.MapValue{
						Fields: map[string]*common.Value{
							"ussid": intValue(8),
						},
					},
				},
			},
		},
	}

	if sequence := server.sessionSequence("room", "new-guest"); sequence != 9 {
		t.Fatalf("expected sequence9, got %d", sequence)
	}

	delete(server.documents["room"], "guest")

	if sequence := server.sessionSequence("room", "another-guest"); sequence != 10 {
		t.Fatalf("departed session sequence was reused: %d", sequence)
	}
}
