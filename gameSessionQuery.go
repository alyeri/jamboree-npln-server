package main

import (
	"context"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func discoverableRoom(r *creationRecord) bool {
	room := r.ticket.GetGameSession()

	return r.expires.After(time.Now()) &&
		room.GetState() == mm.GameSession_ACTIVE &&
		room.GetCanParticipate() &&
		room.GetCurrentParticipantCount() < room.GetMaxParticipantCount()
}

func (s *gameSessionServer) BatchGetGameSessions(ctx context.Context, req *mm.BatchGetGameSessionsRequest) (*mm.BatchGetGameSessionsResponse, error) {
	if len(req.Names) > 256 {
		return nil, status.Error(codes.InvalidArgument, "too many rooms")
	}

	response := &mm.BatchGetGameSessionsResponse{}

	if _, e := callerIdentity(ctx, s.p.identityKey); e != nil {
		return nil, e
	}

	for _, name := range req.Names {
		room, e := s.GetGameSession(ctx, &mm.GetGameSessionRequest{
			Name: name,
			View: req.View,
		})

		if status.Code(e) == codes.NotFound {
			continue
		}

		if e != nil {
			return nil, e
		}

		response.GameSessions = append(response.GameSessions, room)
	}

	return response, nil
}

func (s *gameSessionServer) QueryGameSessions(ctx context.Context, req *mm.QueryGameSessionsRequest) (*mm.QueryGameSessionsResponse, error) {
	deadline := time.Now().Add(5 * time.Second)
	properties := req.GetProperties().GetFields()
	waitForReturn := matchmakingConfigID(req.GameSessionSearchConfig) == "BqGameSessionSearchConfigs" && properties["gameMode"].GetIntegerValue() == 4 && len(properties["searchWord"].GetStringValue()) == 32

	for {
		response, err := s.queryGameSessionsOnce(ctx, req)

		if err != nil || !waitForReturn || len(response.GetGameSessions()) > 0 || !time.Now().Before(deadline) {
			return response, err
		}

		timer := time.NewTimer(200 * time.Millisecond)

		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()

		case <-timer.C:
		}
	}
}

func (s *gameSessionServer) queryGameSessionsOnce(ctx context.Context, req *mm.QueryGameSessionsRequest) (*mm.QueryGameSessionsResponse, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)

	if e != nil {
		return nil, e
	}

	if req.PageToken != "" {
		return nil, status.Error(codes.InvalidArgument, "unsupported page token")
	}

	response := &mm.QueryGameSessionsResponse{}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.tickets {
		s.expirePendingRoomJoins(r, time.Now())

		room := r.ticket.GetGameSession()

		if !matchesRoomQuery(room, req) {
			continue
		}

		assigned := false

		if r.expires.After(time.Now()) && room.GetState() == mm.GameSession_ACTIVE {
			for _, participant := range room.UserSessions {
				if participant.User == owner {
					assigned = true

					break
				}
			}
		}

		if r.owner == owner || (!assigned && !discoverableRoom(r)) {
			continue
		}

		if (!assigned && room.MaxParticipantCount-room.CurrentParticipantCount < req.MinVacancyCount) || room.CurrentParticipantCount < req.MinParticipantCount {
			continue
		}

		config := req.GetGameSessionSearchConfig()

		if strings.Contains(config, "BqP8") && room.MaxParticipantCount != 8 {
			continue
		}

		if strings.Contains(config, "BqP4") && room.MaxParticipantCount != 4 {
			continue
		}

		copy := proto.Clone(room).(*mm.GameSession)
		copy.Password = ""

		// Reconnection reuses this caller's existing slot, even in a full room.
		if assigned {
			copy.CanParticipate = true
		}

		// Publish the native password marker without exposing the secret.
		if req.GetView() == mm.GameSessionView_FULL && room.Password != "" {
			copy.Password = "*"
		}

		response.GameSessions = append(response.GameSessions, copy)
	}

	sort.Slice(response.GameSessions, func(i, j int) bool {
		return response.GameSessions[i].Name < response.GameSessions[j].Name
	})

	limit := int(req.PageSize)

	if limit <= 0 || limit > 256 {
		limit = 256
	}

	if len(response.GameSessions) > limit {
		response.GameSessions = response.GameSessions[:limit]
	}

	s.p.event(map[string]any{
		"event": "public_rooms_query",
		"count": len(response.GameSessions),
	})

	users := make([]string, 0, len(req.Users))

	for _, user := range req.Users {
		users = append(users, identityDigest(canonicalUserSession(user)))
	}

	rooms := make([]map[string]any, 0, len(response.GameSessions))

	for _, room := range response.GameSessions {
		rooms = append(rooms, map[string]any{
			"room_digest": identityDigest(room.Name), "state": int(room.State),
			"can_participate": room.CanParticipate, "participants": room.CurrentParticipantCount,
			"properties": summarizeFields(room.Properties, 0),
		})
	}

	event := gamesyncDebugContext(ctx, owner, "")

	delete(event, "room_digest")

	event["event"] = "room_query_selection"
	event["search_config"] = matchmakingConfigID(req.GameSessionSearchConfig)
	event["requested_properties"] = summarizeFields(req.Properties, 0)
	event["requested_users"] = users
	event["min_vacancy"] = req.MinVacancyCount
	event["min_participants"] = req.MinParticipantCount
	event["rooms"] = rooms

	s.p.event(event)

	return response, nil
}

func matchesRoomQuery(room *mm.GameSession, req *mm.QueryGameSessionsRequest) bool {
	for key, wanted := range req.GetProperties().GetFields() {
		// Native public-room searches send an empty searchWord when no code
		// filter is selected; these rooms may omit the property entirely.
		if key == "searchWord" && wanted.GetStringValue() == "" {
			if _, ok := wanted.GetValueType().(*common.Value_StringValue); ok {
				continue
			}
		}

		actual, exists := room.GetProperties().GetFields()[key]

		if !exists || !proto.Equal(actual, wanted) {
			return false
		}
	}

	if len(req.Users) == 0 {
		return true
	}

	for _, user := range req.Users {
		for _, participant := range room.GetUserSessions() {
			if canonicalUserSession(user) == canonicalUserSession(participant.User) {
				return true
			}
		}
	}

	return false
}
