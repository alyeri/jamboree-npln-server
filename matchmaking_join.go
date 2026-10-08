package main

import (
	"context"
	"crypto/hmac"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func (s *gameSessionServer) JoinGameSession(ctx context.Context, req *mm.JoinGameSessionRequest) (*mm.JoinGameSessionResponse, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)
	if e != nil {
		return nil, e
	}

	s.p.event(map[string]any{
		"event":             "room_join_request_shape",
		"definitions":       len(req.UserDefinitions),
		"delegation_tokens": len(req.UserDelegationTokens),
		"include_id_users":  len(req.IncludeIdTokenUsers),
	})

	if len(req.UserDelegationTokens) > 0 || len(req.UserDefinitions) > 1 {
		return nil, status.Error(codes.Unimplemented, "delegated joins unsupported")
	}

	definition := &mm.UserDefinition{User: owner}

	if len(req.UserDefinitions) == 1 {
		definition = proto.Clone(req.UserDefinitions[0]).(*mm.UserDefinition)
	}

	if definition.User != "" &&
		definition.User != "users/current" &&
		!strings.HasSuffix(definition.User, "/users/current") &&
		canonicalUserSession(definition.User) != canonicalUserSession(owner) {
		return nil, status.Error(codes.PermissionDenied, "join user must match caller")
	}

	definition.User = owner
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, record := range s.tickets {
		room := record.ticket.GetGameSession()

		if room.GetName() != canonicalRoom(req.Name) || !record.expires.After(time.Now()) {
			continue
		}

		if !hmac.Equal([]byte(room.Password), []byte(req.Password)) {
			return nil, status.Error(codes.PermissionDenied, "incorrect room password")
		}

		var user *mm.UserSession

		for _, existing := range room.UserSessions {
			if existing.User == owner {
				user = existing
				break
			}
		}

		if user == nil {
			if !discoverableRoom(record) {
				return nil, status.Error(codes.FailedPrecondition, "room unavailable or full")
			}

			id := owner[strings.LastIndex(owner, "/")+1:]
			user = &mm.UserSession{
				Name:        room.Name + "/userSessions/" + id,
				User:        owner,
				State:       mm.UserSession_RESERVED,
				CreateTime:  timestamppb.Now(),
				Attributes:  definition.Attributes,
				Team:        definition.Team,
				LatencyData: cloneLatencyData(definition),
			}
		}

		proof, e := (&authServer{key: s.p.identityKey}).signSessionToken(owner+"|"+user.Name, "matchmaking", 15*time.Minute)
		if e != nil {
			return nil, e
		}

		exists := false

		for _, existing := range room.UserSessions {
			if existing.Name == user.Name {
				exists = true
			}
		}

		if !exists {
			room.UserSessions = append(room.UserSessions, user)
			room.CurrentParticipantCount++
			room.CanParticipate = room.CurrentParticipantCount < room.MaxParticipantCount
		}

		result := proto.Clone(room).(*mm.GameSession)
		result.Password = ""
		s.p.event(map[string]any{
			"event":        "room_user_joined",
			"participants": room.CurrentParticipantCount,
			"capacity":     room.MaxParticipantCount,
		})

		return &mm.JoinGameSessionResponse{
			GameSession: result,
			MatchedUserSessions: []*mm.MatchedUserSession{
				{
					UserDefinition:     definition,
					UserSession:        user.Name,
					MatchmakingIdToken: proof,
				},
			},
		}, nil
	}

	return nil, status.Error(codes.NotFound, "room not found")
}
