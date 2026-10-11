package main

import (
	"time"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

const roomJoinReservationTimeout = 90 * time.Second

// Capacity changes must preserve the host's explicit entry policy.
// Call with the room registry lock held.
func refreshRoomParticipation(record *creationRecord) {
	room := record.ticket.GetGameSession()
	room.CanParticipate = !record.entryClosed &&
		room.State == mm.GameSession_ACTIVE &&
		room.CurrentParticipantCount < room.MaxParticipantCount
}

// The caller must hold the room registry lock. Gamesync activation uses that
// same lock, so an established participant cannot expire during discovery.
func (s *gameSessionServer) expirePendingRoomJoins(record *creationRecord, now time.Time) {
	room := record.ticket.GetGameSession()

	if room.GetState() != mm.GameSession_ACTIVE || !record.expires.After(now) {
		return
	}

	removed := 0
	participants := make([]*mm.UserSession, 0, len(room.UserSessions))

	for _, user := range room.UserSessions {
		if user.User != record.owner && user.State == mm.UserSession_RESERVED &&
			user.CreateTime != nil && user.CreateTime.IsValid() &&
			!user.CreateTime.AsTime().Add(roomJoinReservationTimeout).After(now) {
			removed++

			continue
		}

		participants = append(participants, user)
	}

	if removed == 0 {
		return
	}

	room.UserSessions = participants
	room.CurrentParticipantCount = int32(len(participants))

	refreshRoomParticipation(record)

	s.p.event(map[string]any{
		"event":        "room_join_reservation_expired",
		"removed":      removed,
		"participants": room.CurrentParticipantCount,
		"capacity":     room.MaxParticipantCount,
	})
}
