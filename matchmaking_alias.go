package main

import (
	"context"
	"crypto/rand"
	"math/big"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func canonicalRoom(name string) string {
	name = canonicalUserSession(name)

	if len(name) == 32 && !strings.Contains(name, "/") {
		return "tenants/t-adf89f68-lp1/gameSessions/" + name
	}

	return name
}

func roomAccessible(r *creationRecord, caller string) bool {
	if r.owner == caller || discoverableRoom(r) {
		return true
	}

	for _, participant := range r.ticket.GetGameSession().GetUserSessions() {
		if canonicalUserSession(participant.GetUser()) == canonicalUserSession(caller) {
			return true
		}
	}

	return false
}

func (s *gameSessionServer) CreateGameSessionShortAlias(ctx context.Context, req *mm.CreateGameSessionShortAliasRequest) (*mm.GameSessionShortAlias, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)
	if e != nil {
		return nil, e
	}

	parent := canonicalUserSession(req.GetParent())

	if parent != "tenants/current" && parent != "tenants/t-adf89f68" && parent != "tenants/t-adf89f68-lp1" {
		return nil, status.Error(codes.InvalidArgument, "tenant required")
	}

	room := canonicalRoom(req.GetGameSessionShortAlias().GetGameSession())
	s.mu.Lock()
	defer s.mu.Unlock()

	var rec *creationRecord

	for _, r := range s.tickets {
		if r.expires.After(time.Now()) && r.ticket.GetGameSession().GetName() == room && roomAccessible(r, owner) {
			rec = r
			break
		}
	}

	if rec == nil {
		return nil, status.Error(codes.NotFound, "owned room not found")
	}

	if rec.alias != nil {
		return proto.Clone(rec.alias).(*mm.GameSessionShortAlias), nil
	}

	if rec.owner != owner {
		return nil, status.Error(codes.NotFound, "room code not published yet")
	}

	for attempt := 0; attempt < 64; attempt++ {
		alphabet := "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
		code := make([]byte, 6)

		for i := range code {
			n, e := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
			if e != nil {
				return nil, status.Error(codes.Internal, "alias entropy unavailable")
			}

			code[i] = alphabet[n.Int64()]
		}

		name := "tenants/t-adf89f68-lp1/GameSessionShortAliases/" + string(code)
		collision := false

		for _, r := range s.tickets {
			aliasID := strings.TrimPrefix(name, "tenants/t-adf89f68-lp1/GameSessionShortAliases/")
			sessionID := strings.TrimPrefix(r.ticket.GetGameSession().GetName(), "tenants/t-adf89f68-lp1/gameSessions/")
			bySession := len(aliasID) == 32 && strings.EqualFold(aliasID, sessionID)

			if (r.alias.GetName() == name || bySession) && r.alias != nil && r.expires.After(time.Now()) {
				collision = true
				break
			}
		}

		if collision {
			continue
		}

		rec.alias = &mm.GameSessionShortAlias{
			Name:        name,
			GameSession: room,
			ExpireTime:  timestamppb.New(rec.expires),
		}
		s.p.event(map[string]any{
			"event": "room_short_alias_created",
		})

		return proto.Clone(rec.alias).(*mm.GameSessionShortAlias), nil
	}

	return nil, status.Error(codes.ResourceExhausted, "alias allocation unavailable")
}

func (s *gameSessionServer) GetGameSessionShortAlias(ctx context.Context, req *mm.GetGameSessionShortAliasRequest) (*mm.GameSessionShortAlias, error) {
	if _, e := callerIdentity(ctx, s.p.identityKey); e != nil {
		return nil, e
	}

	name := canonicalShortAlias(req.GetName())
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.tickets {
		aliasID := strings.TrimPrefix(name, "tenants/t-adf89f68-lp1/GameSessionShortAliases/")
		sessionID := strings.TrimPrefix(r.ticket.GetGameSession().GetName(), "tenants/t-adf89f68-lp1/gameSessions/")
		bySession := len(aliasID) == 32 && strings.EqualFold(aliasID, sessionID)

		if (r.alias.GetName() == name || bySession) && r.alias != nil && r.expires.After(time.Now()) {
			return proto.Clone(r.alias).(*mm.GameSessionShortAlias), nil
		}
	}

	s.p.event(map[string]any{
		"event":           "room_alias_lookup_miss",
		"public_resource": name,
		"name_length":     len(name),
	})

	return nil, status.Error(codes.NotFound, "room alias not found")
}

func (s *gameSessionServer) GetGameSession(ctx context.Context, req *mm.GetGameSessionRequest) (*mm.GameSession, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)
	if e != nil {
		return nil, e
	}

	name := canonicalRoom(req.GetName())
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, r := range s.tickets {
		if r.expires.After(time.Now()) && r.ticket.GetGameSession().GetName() == name && roomAccessible(r, owner) {
			s.p.event(map[string]any{
				"event":            "room_session_read",
				"password_present": r.ticket.GameSession.Password != "",
				"is_public":        r.ticket.GameSession.IsPublic,
				"view":             int(req.GetView()),
			})
			room := proto.Clone(r.ticket.GameSession).(*mm.GameSession)

			if r.owner != owner {
				room.Password = ""
			}

			return room, nil
		}
	}

	return nil, status.Error(codes.NotFound, "owned room not found")
}

func canonicalShortAlias(name string) string {
	name = canonicalUserSession(name)
	parts := strings.Split(name, "/")

	if len(parts) == 1 && len(name) == 6 {
		return "tenants/t-adf89f68-lp1/GameSessionShortAliases/" + strings.ToUpper(name)
	}

	for i, part := range parts {
		if strings.EqualFold(part, "GameSessionShortAliases") {
			parts[i] = "GameSessionShortAliases"

			if i+1 < len(parts) {
				parts[i+1] = strings.ToUpper(parts[i+1])
			}
		}
	}

	name = strings.Join(parts, "/")

	if strings.HasPrefix(name, "GameSessionShortAliases/") {
		name = "tenants/t-adf89f68-lp1/" + name
	}

	return name
}
