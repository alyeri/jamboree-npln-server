package main

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

const delegationLifetime = 15 * time.Minute

type delegationRecord struct {
	detail  *mm.UserDelegationDetail
	expires time.Time
}

// Participation grants are valid only for the caller named as mandatary.
// A grant never transfers ticket ownership or authorizes other delegates.
func (g *gameSessionServer) delegatedParticipants(caller string, tokens []string) (map[string]bool, error) {
	if len(tokens) > 3 {
		return nil, status.Error(codes.InvalidArgument, "too many delegation tokens")
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	users := map[string]bool{caller: true}
	seen := map[string]bool{}

	for _, token := range tokens {
		record := g.delegations[token]

		if seen[token] || record == nil || !record.expires.After(time.Now()) || record.detail.MandataryUser != caller {
			return nil, status.Error(codes.PermissionDenied, "invalid participation delegation")
		}

		seen[token] = true
		participation := false

		for _, action := range record.detail.DelegationActions {
			participation = participation || action == mm.DelegationAction_ANY_PARTICIPATION
		}

		if !participation {
			return nil, status.Error(codes.PermissionDenied, "participation delegation required")
		}

		users[record.detail.DelegatorUser] = true
	}

	return users, nil
}

func delegationUser(name, caller string) string {
	if name == "users/current" || name == "tenants/current/users/current" ||
		name == "tenants/t-adf89f68/users/current" || name == "tenants/t-adf89f68-lp1/users/current" {
		return caller
	}

	name = canonicalUserSession(name)
	prefix := "tenants/t-adf89f68-lp1/users/"
	id := strings.TrimPrefix(name, prefix)

	if !strings.HasPrefix(name, prefix) || id == "" || strings.ContainsAny(id, "/\x00") || len(id) > 128 {
		return ""
	}

	return name
}

func (g *gameSessionServer) authorizeSearchUsers(caller string, definitions []*mm.UserDefinition, tokens []string) ([]*mm.UserDefinition, error) {
	allowed, err := g.delegatedParticipants(caller, tokens)

	if err != nil {
		return nil, err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	result := make([]*mm.UserDefinition, 0, len(definitions))
	seen := map[string]bool{}

	for _, definition := range definitions {
		if definition == nil {
			return nil, status.Error(codes.InvalidArgument, "user definition required")
		}

		user := definition.User

		if user == "" {
			user = "users/current"
		}

		user = delegationUser(user, caller)

		if !allowed[user] || seen[user] {
			return nil, status.Error(codes.PermissionDenied, "search user requires delegation")
		}

		seen[user] = true
		d := proto.Clone(definition).(*mm.UserDefinition)
		d.User = user

		if user != caller {
			for _, token := range tokens {
				r := g.delegations[token]

				if r.detail.DelegatorUser == user {
					if !r.expires.After(time.Now()) {
						return nil, status.Error(codes.PermissionDenied, "delegation expired")
					}

					if r.detail.Attributes != nil {
						d.Attributes = proto.Clone(r.detail.Attributes).(*common.MapValue)
					}
				}
			}
		}

		result = append(result, d)
	}

	if !seen[caller] {
		return nil, status.Error(codes.InvalidArgument, "caller must participate in search")
	}

	return result, nil
}

// Each remote teammate obtains its own session proof. Participation alone
// does not grant the leader access to another client's credentials.
func (g *gameSessionServer) IssueMatchmakingIdToken(ctx context.Context, req *mm.IssueMatchmakingIdTokenRequest) (*mm.IssueMatchmakingIdTokenResponse, error) {
	caller, err := callerIdentity(ctx, g.p.identityKey)

	if err != nil {
		return nil, err
	}

	caller = canonicalUserSession(caller)

	if req == nil || len(req.Users) != 1 ||
		delegationUser(req.Users[0], caller) != caller ||
		len(req.IncludeIdTokenUsers) != 0 || len(req.UserDelegationTokens) != 0 {
		return nil, status.Error(codes.PermissionDenied, "only caller session proof supported")
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	for _, record := range g.tickets {
		room := record.ticket.GetGameSession()

		if room.GetName() != canonicalRoom(req.GameSession) || !record.expires.After(time.Now()) {
			continue
		}

		for _, user := range room.UserSessions {
			if canonicalUserSession(user.User) != caller {
				continue
			}

			proof, err := (&authServer{key: g.p.identityKey}).signSessionToken(caller+"|"+user.Name, "matchmaking", 15*time.Minute)

			if err != nil {
				return nil, err
			}

			result := proto.Clone(room).(*mm.GameSession)
			result.Password = ""
			user = proto.Clone(user).(*mm.UserSession)

			return &mm.IssueMatchmakingIdTokenResponse{
				GameSession: result,
				MatchedUserSessions: []*mm.MatchedUserSession{
					{
						UserSession:        user.Name,
						MatchmakingIdToken: proof,
						UserDefinition: &mm.UserDefinition{
							User:        caller,
							Team:        user.Team,
							Attributes:  user.Attributes,
							LatencyData: user.LatencyData,
						},
					},
				},
			}, nil
		}

		return nil, status.Error(codes.PermissionDenied, "caller is not assigned to room")
	}

	return nil, status.Error(codes.NotFound, "room not found")
}

func (g *gameSessionServer) IssueUserDelegationToken(ctx context.Context, req *mm.IssueUserDelegationTokenRequest) (*mm.IssueUserDelegationTokenResponse, error) {
	caller, err := callerIdentity(ctx, g.p.identityKey)

	if err != nil {
		return nil, err
	}

	caller = canonicalUserSession(caller)

	if req == nil || proto.Size(req) > 64*1024 {
		return nil, status.Error(codes.InvalidArgument, "delegation request required")
	}

	if req.Parent != "tenants/current" && req.Parent != "tenants/t-adf89f68" && req.Parent != "tenants/t-adf89f68-lp1" {
		return nil, status.Error(codes.InvalidArgument, "Jamboree tenant required")
	}

	delegator := delegationUser(req.DelegatorUser, caller)
	mandatary := delegationUser(req.MandataryUser, caller)

	if delegator == "" || mandatary == "" {
		return nil, status.Error(codes.InvalidArgument, "delegation users required")
	}

	if delegator != caller {
		return nil, status.Error(codes.PermissionDenied, "delegator must match caller")
	}

	if len(req.DelegationActions) == 0 || len(req.DelegationActions) > 3 {
		return nil, status.Error(codes.InvalidArgument, "delegation actions required")
	}

	seen := map[mm.DelegationAction]bool{}

	for _, action := range req.DelegationActions {
		if action < mm.DelegationAction_ANY_PARTICIPATION || action > mm.DelegationAction_INCLUDE_ID_TOKEN_FOR_SAME_CLIENT || seen[action] {
			return nil, status.Error(codes.InvalidArgument, "invalid delegation action")
		}

		seen[action] = true
	}

	id, err := randomResourceID()

	if err != nil {
		return nil, err
	}

	detail := &mm.UserDelegationDetail{
		DelegatorUser:       delegator,
		MandataryUser:       mandatary,
		DelegationActions:   append([]mm.DelegationAction(nil), req.DelegationActions...),
		UserDelegationToken: id,
		Ttl:                 durationpb.New(delegationLifetime),
	}

	if req.Attributes != nil {
		detail.Attributes = proto.Clone(req.Attributes).(*common.MapValue)
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.delegations == nil {
		g.delegations = map[string]*delegationRecord{}
	}

	for token, record := range g.delegations {
		if !record.expires.After(time.Now()) {
			delete(g.delegations, token)
		}
	}

	if len(g.delegations) >= 1024 {
		return nil, status.Error(codes.ResourceExhausted, "delegation limit reached")
	}

	g.delegations[id] = &delegationRecord{
		detail:  detail,
		expires: time.Now().Add(delegationLifetime),
	}
	g.p.event(map[string]any{
		"event":     "user_delegation_issued",
		"same_user": delegator == mandatary,
		"actions":   req.DelegationActions,
	})

	return &mm.IssueUserDelegationTokenResponse{UserDelegationDetail: proto.Clone(detail).(*mm.UserDelegationDetail)}, nil
}
