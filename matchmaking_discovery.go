package main

import (
	"context"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

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
		room, e := s.GetGameSession(ctx, &mm.GetGameSessionRequest{Name: name, View: req.View})

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
		if r.owner == owner || !discoverableRoom(r) {
			continue
		}

		room := r.ticket.GetGameSession()

		if room.MaxParticipantCount-room.CurrentParticipantCount < req.MinVacancyCount || room.CurrentParticipantCount < req.MinParticipantCount {
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

	return response, nil
}
