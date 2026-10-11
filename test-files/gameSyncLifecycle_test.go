package main

import (
	"bytes"
	"testing"
	"time"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestParticipantExpiryAndReconnect(t *testing.T) {
	room := canonicalRoom("lifecycle")
	host := "host|" + room + "/userSessions/host"
	guest := "guest|" + room + "/userSessions/guest"
	r := &creationRecord{
		owner:   "host",
		expires: time.Now().Add(time.Hour),
		ticket: &mm.GameSessionCreationTicket{
			GameSession: &mm.GameSession{
				Name:                    room,
				State:                   mm.GameSession_ACTIVE,
				MaxParticipantCount:     2,
				CurrentParticipantCount: 2,
				UserSessions: []*mm.UserSession{
					{
						Name: room + "/userSessions/host",
						User: "host",
					},
					{
						Name: room + "/userSessions/guest",
						User: "guest",
					},
				},
			},
		},
	}
	hostSub := &gsSubscriber{
		proof: host,
	}
	s := &gamesyncServer{
		p: &serverContext{
			writer: &bytes.Buffer{},
		},
		rooms: &gameSessionServer{
			tickets: map[string]*creationRecord{
				"r": r,
			},
		},
		epochs: map[string]uint64{
			guest: 2,
			host:  1,
		},
		subs: map[string]map[*gsSubscriber]bool{
			room: {
				hostSub: true,
			},
		},
		documents: map[string]map[string]*gs.Document{
			room: {
				"docs/__us/guest": {
					Name: "docs/__us/guest",
				},
				"docs/__pus/guest": {
					Name: "docs/__pus/guest",
				},
			},
		},
		deferments: map[string]map[string]*gs.Deferment{
			guest: {
				"cleanup": {
					WriteOperations: []*gs.WriteOperation{
						{
							OperationType: &gs.WriteOperation_DeleteDocument{
								DeleteDocument: &gs.DeleteDocumentRequest{
									Name: "docs/__pus/guest",
								},
							},
						},
					},
				},
			},
		},
	}

	s.expireDisconnectedSession(guest, room, 1)

	if r.ticket.GameSession.CurrentParticipantCount != 2 {
		t.Fatal("old timer removed reconnected participant")
	}

	guestSub := &gsSubscriber{
		proof: guest,
	}
	s.subs[room][guestSub] = true

	s.expireDisconnectedSession(guest, room, 2)

	if r.ticket.GameSession.CurrentParticipantCount != 2 {
		t.Fatal("live guest removed")
	}

	delete(s.subs[room], guestSub)

	s.expireDisconnectedSession(guest, room, 2)

	if r.ticket.GameSession.CurrentParticipantCount != 1 || !discoverableRoom(r) {
		t.Fatal("guest vacancy not restored")
	}

	if s.documents[room]["docs/__us/guest"] != nil || s.documents[room]["docs/__pus/guest"] != nil {
		t.Fatal("guest documents left behind while host live")
	}

	delete(s.subs[room], hostSub)

	s.expireDisconnectedSession(host, room, 1)

	if discoverableRoom(r) || r.ticket.GameSession.State != mm.GameSession_TERMINATED {
		t.Fatal("closed host remains discoverable")
	}

	if s.activateBoundRoom(host) == nil {
		t.Fatal("expired host resurrected")
	}
}
