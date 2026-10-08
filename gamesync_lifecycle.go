package main

import (
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

// Allow transport replacement without deleting the participant's documents.
const sessionReconnectGrace = 15 * time.Second

func (s *gamesyncServer) scheduleSessionCleanup(proof, room string) {
	s.mu.Lock()
	epoch := s.epochs[proof]
	s.mu.Unlock()
	s.p.event(map[string]any{
		"event":                   "gamesync_keep_closed",
		"reconnect_grace_seconds": 15,
	})
	time.AfterFunc(sessionReconnectGrace, func() {
		s.expireDisconnectedSession(proof, room, epoch)
	})
}

func (s *gamesyncServer) expireDisconnectedSession(proof, room string, epoch uint64) {
	s.publication.Lock()
	s.mu.Lock()

	if s.epochs[proof] != epoch {
		s.mu.Unlock()
		s.publication.Unlock()

		return
	}

	for sub := range s.subs[room] {
		if sub.proof == proof {
			s.mu.Unlock()
			s.publication.Unlock()

			return
		}
	}

	s.mu.Unlock()
	// publication serializes expiry against registration of a replacement stream.
	parts := strings.Split(proof, "|")
	hostClosed := false

	if s.rooms != nil {
		s.rooms.mu.Lock()

		for _, record := range s.rooms.tickets {
			r := record.ticket.GetGameSession()

			if r.GetName() != room {
				continue
			}

			for i, u := range r.UserSessions {
				if u.Name != parts[1] || u.User != parts[0] {
					continue
				}

				if record.owner == parts[0] {
					r.State = mm.GameSession_TERMINATED
					r.CanParticipate = false
					record.expires = time.Now()
					hostClosed = true
				} else {
					r.UserSessions = append(r.UserSessions[:i], r.UserSessions[i+1:]...)
					r.CurrentParticipantCount = int32(len(r.UserSessions))
					r.CanParticipate = r.CurrentParticipantCount < r.MaxParticipantCount
				}

				break
			}
		}

		s.rooms.mu.Unlock()
	}

	s.mu.Lock()
	name := "docs/__us/" + parts[1][strings.LastIndex(parts[1], "/")+1:]
	delete(s.documents[room], name)
	s.publishChangesLocked(room, s.documents[room], map[string]bool{name: true}, timestamppb.Now())
	s.cleanupDefermentsLocked(proof, room)
	s.mu.Unlock()
	s.publication.Unlock()
	s.p.event(map[string]any{
		"event":       "gamesync_participant_expired",
		"host_closed": hostClosed,
	})
}

// A creation ticket reserves a participant. Its authenticated Gamesync stream
// establishes the live session; GetGameSession must then reflect that state.
func (s *gamesyncServer) activateBoundRoom(proof string) error {
	if s.rooms == nil {
		return nil
	}

	parts := strings.Split(proof, "|")
	s.rooms.mu.Lock()
	defer s.rooms.mu.Unlock()

	for _, record := range s.rooms.tickets {
		if !record.expires.After(time.Now()) {
			continue
		}

		for _, user := range record.ticket.GetGameSession().GetUserSessions() {
			if user.Name != parts[1] || user.User != parts[0] {
				continue
			}

			room := record.ticket.GameSession

			if room.State != mm.GameSession_CREATING && room.State != mm.GameSession_ACTIVE {
				return status.Error(codes.FailedPrecondition, "room is no longer live")
			}

			room.State = mm.GameSession_ACTIVE
			user.State = mm.UserSession_ACTIVE
			record.expires = time.Now().Add(time.Hour)
			s.p.event(map[string]any{
				"event": "room_session_activated",
				"state": "active",
			})

			return nil
		}
	}

	return status.Error(codes.NotFound, "bound room is no longer available")
}
