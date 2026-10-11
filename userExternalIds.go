package main

import (
	"context"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

type userServiceServer struct {
	authpb.UnimplementedUserServiceServer
	p *serverContext
}

func (s *userServiceServer) QueryUserExternalIds(ctx context.Context, r *authpb.QueryUserExternalIdsRequest) (*authpb.QueryUserExternalIdsResponse, error) {
	if _, e := callerIdentity(ctx, s.p.identityKey); e != nil {
		return nil, e
	}

	if !validRankingTenant(r.Tenant) || r.PageSize < 0 || r.PageSize > 100 || r.PageToken != "" {
		return nil, status.Error(codes.InvalidArgument, "invalid user query")
	}

	c := r.GetExternalIdCondition()

	if reverse := r.GetUserCondition(); reverse != nil {
		if len(reverse.ExternalIds) > 100 {
			return nil, status.Error(codes.InvalidArgument, "user query limit exceeded")
		}
		// A user can have several resource aliases. Prefer its primary 128-bit
		// identifier and return one association per requested NSA identifier.
		s.p.mu.Lock()
		defer s.p.mu.Unlock()

		users := make([]string, 0, len(s.p.accountIDs))

		for user := range s.p.accountIDs {
			users = append(users, user)
		}

		sort.Slice(users, func(i, j int) bool {
			primary := func(user string) bool {
				id := user[strings.LastIndex(user, "/")+1:]

				return len(id) == 32 && !strings.HasPrefix(id, "u-")
			}

			if primary(users[i]) != primary(users[j]) {
				return primary(users[i])
			}

			return users[i] < users[j]
		})

		byNSA := make(map[string]string)

		for _, user := range users {
			id := strings.ToLower(s.p.accountIDs[user])

			if byNSA[id] == "" {
				byNSA[id] = user
			}
		}

		out := &authpb.QueryUserExternalIdsResponse{}
		seen := make(map[string]bool)
		limit := int(r.PageSize)

		if limit == 0 {
			limit = 100
		}

		for _, external := range reverse.ExternalIds {
			if external == nil || external.Type != authpb.ExternalIdType_NSA_ID {
				continue
			}

			id := strings.ToLower(external.Id)
			user := byNSA[id]

			if user == "" || seen[id] {
				continue
			}

			seen[id] = true
			out.UserExternalIds = append(out.UserExternalIds, &authpb.UserExternalId{
				Name: user + "/externalIds/nsa_id",
				User: user,
				ExternalId: &authpb.ExternalId{
					Type: authpb.ExternalIdType_NSA_ID,
					Id:   id,
				},
			})

			if len(out.UserExternalIds) == limit {
				break
			}
		}

		return out, nil
	}

	if c == nil || len(c.Users) > 100 {
		return nil, status.Error(codes.InvalidArgument, "user condition required")
	}

	include := len(c.ExternalIdTypes) == 0

	for _, kind := range c.ExternalIdTypes {
		if kind == authpb.ExternalIdType_NSA_ID {
			include = true
		}
	}

	out := &authpb.QueryUserExternalIdsResponse{}

	if !include {
		return out, nil
	}

	s.p.mu.Lock()
	defer s.p.mu.Unlock()

	limit := int(r.PageSize)

	if limit == 0 {
		limit = 100
	}

	for _, raw := range c.Users {
		user := canonicalUserSession(raw)
		id := s.p.accountIDs[user]

		if id == "" {
			continue
		}

		out.UserExternalIds = append(out.UserExternalIds, &authpb.UserExternalId{
			Name: user + "/externalIds/nsa_id",
			User: user,
			ExternalId: &authpb.ExternalId{
				Type: authpb.ExternalIdType_NSA_ID,
				Id:   strings.ToLower(id),
			},
		})

		if len(out.UserExternalIds) == limit {
			break
		}
	}

	return out, nil
}
