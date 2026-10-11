package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	mmpb "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

type creationRecord struct {
	alias       *mmpb.GameSessionShortAlias
	owner       string
	ticket      *mmpb.GameSessionCreationTicket
	expires     time.Time
	entryClosed bool
}

type gameSessionServer struct {
	mmpb.UnimplementedGameSessionServiceServer
	p           *serverContext
	mu          sync.Mutex
	tickets     map[string]*creationRecord
	delegations map[string]*delegationRecord
}

func callerIdentity(ctx context.Context, key []byte) (string, error) {
	if err := verifyAccess(ctx, key); err != nil {
		return "", err
	}

	md, _ := metadata.FromIncomingContext(ctx)
	raw := strings.TrimSpace(md.Get("authorization")[0])

	if len(raw) >= 7 && strings.EqualFold(raw[:7], "Bearer ") {
		raw = strings.TrimSpace(raw[7:])
	}

	data, _ := base64.RawURLEncoding.DecodeString(strings.Split(raw, ".")[1])

	var claims struct{ Sub string }

	if json.Unmarshal(data, &claims) != nil {
		return "", status.Error(codes.Unauthenticated, "invalid caller")
	}

	return claims.Sub, nil
}

func (g *gameSessionServer) CreateGameSessionCreationTicket(
	ctx context.Context,
	req *mmpb.CreateGameSessionCreationTicketRequest,
) (*mmpb.GameSessionCreationTicket, error) {
	owner, err := callerIdentity(ctx, g.p.identityKey)

	if err != nil {
		return nil, err
	}

	parent := req.GetParent()

	if parent != "tenants/current" && parent != "tenants/t-adf89f68-lp1" && parent != "tenants/t-adf89f68" {
		return nil, status.Error(codes.InvalidArgument, "Jamboree tenant resource required")
	}

	if req.GetGameSessionCreationTicket() == nil {
		return nil, status.Error(codes.InvalidArgument, "creation ticket required")
	}

	ticket := proto.Clone(req.GameSessionCreationTicket).(*mmpb.GameSessionCreationTicket)

	if ticket.GameSession == nil {
		ticket.GameSession = &mmpb.GameSession{}
	}

	requestedCapacity := ticket.GameSession.MaxParticipantCount
	baseConfig := ticket.GameSession.GetProperties().GetFields()["_BaseConfigName"].GetStringValue()
	// BqP4/BqP8 are the public-room configurations used by Jamboree.
	// The creation request leaves the assigned session flags at protobuf
	// defaults. Carrying that false into Gamesync's ip field prevents
	// IsSessionEntry from succeeding even after the native join succeeds.
	config := matchmakingConfigID(ticket.MatchmakingConfig)

	// Password protection does not change a public room's admission state.
	if config == "BqP4MatchmakingConfigs" || config == "BqP8MatchmakingConfigs" {
		ticket.GameSession.IsPublic = true
	}

	if ticket.GameSession.MaxParticipantCount == 0 {
		ticket.GameSession.MaxParticipantCount = 4

		if matchmakingConfigID(ticket.MatchmakingConfig) == "BqP8MatchmakingConfigs" ||
			matchmakingConfigID(baseConfig) == "BqP8MatchmakingConfigs" ||
			strings.HasSuffix(ticket.MatchmakingConfig, "/friend_room8") ||
			ticket.MatchmakingConfig == "friend_room8" ||
			baseConfig == "friend_room8" {
			ticket.GameSession.MaxParticipantCount = 8
		}
	}

	g.p.event(map[string]any{
		"event":                 "room_capacity_requested",
		"requested_max_players": requestedCapacity,
		"resolved_max_players":  ticket.GameSession.MaxParticipantCount,
		"matchmaking_config":    matchmakingConfigID(ticket.MatchmakingConfig),
		"base_config":           matchmakingConfigID(baseConfig),
	})

	if ticket.GameSession.MaxParticipantCount < 1 || ticket.GameSession.MaxParticipantCount > 8 || len(ticket.GameSession.Password) > 64 {
		return nil, status.Error(codes.InvalidArgument, "unsupported room limits")
	}

	if len(ticket.UserDefinitions) == 0 {
		ticket.UserDefinitions = []*mmpb.UserDefinition{
			{
				User: owner,
			},
		}
	}

	if len(ticket.UserDefinitions) != 1 {
		return nil, status.Error(codes.Unimplemented, "delegated local players not implemented")
	}

	for _, d := range ticket.UserDefinitions {
		if d == nil {
			return nil, status.Error(codes.InvalidArgument, "user definition required")
		}

		if d.User != "" && d.User != "users/current" && !strings.HasSuffix(d.User, "/users/current") && d.User != owner {
			return nil, status.Error(codes.PermissionDenied, "ticket user must match caller")
		}

		d.User = owner
	}

	nonce := make([]byte, 16)

	if _, err = rand.Read(nonce); err != nil {
		return nil, status.Error(codes.Internal, "ticket entropy unavailable")
	}

	ticket.Name = "tenants/t-adf89f68-lp1/gameSessionCreationTickets/" + hex.EncodeToString(nonce)
	ticket.State = mmpb.GameSessionCreationTicket_PENDING
	ticket.MatchedUserSessions = nil

	g.mu.Lock()
	defer g.mu.Unlock()

	for k, r := range g.tickets {
		if time.Now().After(r.expires) {
			delete(g.tickets, k)
		}
	}

	if len(g.tickets) >= 256 {
		return nil, status.Error(codes.ResourceExhausted, "room ticket limit reached")
	}

	g.tickets[ticket.Name] = &creationRecord{
		owner:   owner,
		ticket:  proto.Clone(ticket).(*mmpb.GameSessionCreationTicket),
		expires: time.Now().Add(15 * time.Minute),
	}

	g.p.event(map[string]any{
		"event":                 "room_creation_ticket",
		"max_players":           ticket.GameSession.MaxParticipantCount,
		"password_present":      ticket.GameSession.Password != "",
		"user_count":            len(ticket.UserDefinitions),
		"request_unknown_bytes": len(req.ProtoReflect().GetUnknown()),
	})

	return ticket, nil
}

func matchmakingConfigID(name string) string {
	name = name[strings.LastIndex(name, "/")+1:]

	switch name {
	case "", "friend_room4", "friend_room8", "BqP4MatchmakingConfigs", "BqP4NoEasePaticipantConfigs", "BqP8MatchmakingConfigs", "BqP20MatchmakingConfigs", "BqGameSessionSearchConfigs", "BqReEntrySessionSearchConfigs":
		return name

	default:
		return "other"
	}
}

func (g *gameSessionServer) TrackGameSessionCreationTicket(
	req *mmpb.TrackGameSessionCreationTicketRequest,
	stream grpc.ServerStreamingServer[mmpb.GameSessionCreationTicket],
) error {
	owner, err := callerIdentity(stream.Context(), g.p.identityKey)

	if err != nil {
		return err
	}

	g.mu.Lock()

	lookup := canonicalCreationTicket(req.GetName())
	r := g.tickets[lookup]

	if r == nil || time.Now().After(r.expires) {
		g.p.event(map[string]any{
			"event":               "creation_ticket_lookup_miss",
			"name_segments":       len(strings.Split(req.GetName(), "/")),
			"name_length":         len(req.GetName()),
			"recognized_resource": lookup != "",
		})

		g.mu.Unlock()

		return status.Error(codes.NotFound, "creation ticket not found")
	}

	if r.owner != owner {
		g.mu.Unlock()

		return status.Error(codes.PermissionDenied, "ticket belongs to another caller")
	}

	ticket := proto.Clone(r.ticket).(*mmpb.GameSessionCreationTicket)

	if ticket.State == mmpb.GameSessionCreationTicket_PENDING {
		id := strings.TrimPrefix(ticket.Name, "tenants/t-adf89f68-lp1/gameSessionCreationTickets/")
		session := ticket.GameSession
		session.Name = "tenants/t-adf89f68-lp1/gameSessions/" + id
		session.CreateTime = timestamppb.Now()
		session.Host = "t-adf89f68-lp1.lp1.t.npln.srv.nintendo.net"
		session.Port = 443
		session.State = mmpb.GameSession_CREATING
		session.CurrentParticipantCount = 1
		session.CanParticipate = session.MaxParticipantCount > 1
		// Participant resources must carry real identifiers, including the creator.
		// The old literal "host" propagated as a four-character PIA susid while
		// joined participants used 32-character identifiers.
		participantNonce := make([]byte, 16)

		if _, e := rand.Read(participantNonce); e != nil {
			g.mu.Unlock()

			return status.Error(codes.Internal, "participant entropy unavailable")
		}

		userSession := session.Name + "/userSessions/" + hex.EncodeToString(participantNonce)
		definition := ticket.UserDefinitions[0]
		session.UserSessions = []*mmpb.UserSession{
			{
				Name:        userSession,
				User:        owner,
				State:       mmpb.UserSession_RESERVED,
				CreateTime:  session.CreateTime,
				Attributes:  definition.Attributes,
				Team:        definition.Team,
				LatencyData: cloneLatencyData(definition),
			},
		}
		proof, e := (&authServer{
			key: g.p.identityKey,
		}).signSessionToken(owner+"|"+userSession, "matchmaking", 15*time.Minute)

		if e != nil {
			g.mu.Unlock()

			return e
		}

		ticket.MatchedUserSessions = []*mmpb.MatchedUserSession{
			{
				UserDefinition:     definition,
				UserSession:        userSession,
				MatchmakingIdToken: proof,
			},
		}
		ticket.State = mmpb.GameSessionCreationTicket_SUCCEEDED
		r.ticket = proto.Clone(ticket).(*mmpb.GameSessionCreationTicket)
	}

	g.mu.Unlock()

	g.p.event(map[string]any{
		"event":         "room_creation_assigned",
		"session_state": "creating",
		"max_players":   ticket.GameSession.MaxParticipantCount,
	})

	return stream.Send(ticket)
}

func canonicalCreationTicket(name string) string {
	if strings.HasPrefix(name, "gameSessionCreationTickets/") {
		name = "tenants/current/" + name
	}

	for _, tenant := range []string{
		"tenants/current",
		"tenants/t-adf89f68",
		"tenants/t-adf89f68-lp1",
	} {
		prefix := tenant + "/gameSessionCreationTickets/"

		if strings.HasPrefix(name, prefix) {
			id := strings.TrimPrefix(name, prefix)

			if len(id) != 32 {
				return ""
			}

			if _, err := hex.DecodeString(id); err != nil {
				return ""
			}

			return "tenants/t-adf89f68-lp1/gameSessionCreationTickets/" + id
		}
	}

	return ""
}
