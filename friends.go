package main

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	fr "github.com/alyeri/jamboree-npln-server/proto/friends/v1"
)

type friendsServer struct {
	fr.UnimplementedFriendsServer
	p      *serverContext
	mu     sync.Mutex
	active map[string]time.Time
	rooms  *gameSessionServer
}

func (s *friendsServer) ActivateUser(ctx context.Context, req *fr.ActivateUserRequest) (*fr.ActivateUserResponse, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)

	if e != nil {
		return nil, e
	}

	name := canonicalUserSession(req.GetName())

	if name != canonicalUserSession(owner) && name != "users/current" && name != "tenants/t-adf89f68-lp1/users/current" {
		return nil, status.Error(codes.PermissionDenied, "activation user must match caller")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.active == nil {
		s.active = map[string]time.Time{}
	}

	for k, expiry := range s.active {
		if !expiry.After(time.Now()) {
			delete(s.active, k)
		}
	}

	if len(s.active) >= 256 && s.active[owner].IsZero() {
		return nil, status.Error(codes.ResourceExhausted, "active user limit")
	}

	s.active[owner] = time.Now().Add(15 * time.Minute)

	s.p.event(map[string]any{
		"event": "friends_user_activated",
	})

	return &fr.ActivateUserResponse{}, nil
}

func (s *friendsServer) SubscribeFriendUsers(req *fr.SubscribeFriendUsersRequest, stream grpc.ServerStreamingServer[fr.SubscribeFriendUsersResponse]) error {
	if _, e := s.ActivateUser(stream.Context(), &fr.ActivateUserRequest{
		Name: req.GetUser(),
	}); e != nil {
		return e
	}

	owner, _ := callerIdentity(stream.Context(), s.p.identityKey)
	send := func() error {
		response := s.publicRoomHosts(owner)

		s.p.event(map[string]any{
			"event": "public_room_hosts",
			"count": len(response.FriendAccounts),
		})

		return stream.Send(response)
	}

	if e := send(); e != nil {
		return e
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	s.p.event(map[string]any{
		"event": "friends_subscription_open",
	})

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()

		case <-ticker.C:
			if _, e := callerIdentity(stream.Context(), s.p.identityKey); e != nil {
				return e
			}
			if e := send(); e != nil {
				return e
			}
		}
	}
}

// Jamboree uses this directory to discover hosts. It is a public room directory,
// independent of the callers' Nintendo friendship graph.
func (s *friendsServer) publicRoomHosts(owner string) *fr.SubscribeFriendUsersResponse {
	response := &fr.SubscribeFriendUsersResponse{
		KeepAliveInterval: durationpb.New(10 * time.Second),
	}

	if s.rooms == nil {
		return response
	}

	s.rooms.mu.Lock()
	defer s.rooms.mu.Unlock()

	seen := map[string]bool{}

	for _, r := range s.rooms.tickets {
		s.rooms.expirePendingRoomJoins(r, time.Now())

		if r.owner == owner || seen[r.owner] || !discoverableRoom(r) {
			continue
		}

		seen[r.owner] = true
		user := canonicalUserSession(r.owner)
		friendID := user[strings.LastIndex(user, "/")+1:]
		id := friendID

		if s.p != nil {
			s.p.mu.Lock()

			accountID := s.p.accountIDs[user]

			s.p.mu.Unlock()

			if accountID != "" {
				id = accountID
			} else if len(id) >= 16 {
				id = id[len(id)-16:]
			}
		} else if len(id) >= 16 {
			id = id[len(id)-16:]
		}

		response.FriendAccounts = append(response.FriendAccounts, &fr.SubscribeFriendUsersResponse_FriendAccount{
			NsaId: id,
			Users: []*fr.FriendUser{
				{
					Name:       canonicalUserSession(owner) + "/friendUsers/" + friendID,
					FriendUser: user,
					NsaId:      id,
					Relationship: &fr.FriendUser_Relationship{
						PresenceDeliverable: true,
						PresenceReceivable:  true,
					},
				},
			},
		})
	}

	sort.Slice(response.FriendAccounts, func(i, j int) bool {
		return response.FriendAccounts[i].NsaId < response.FriendAccounts[j].NsaId
	})

	return response
}

func (s *friendsServer) ListFriendUsers(ctx context.Context, req *fr.ListFriendUsersRequest) (*fr.ListFriendUsersResponse, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)

	if e != nil {
		return nil, e
	}

	response := &fr.ListFriendUsersResponse{}

	for _, account := range s.publicRoomHosts(owner).FriendAccounts {
		response.FriendUsers = append(response.FriendUsers, account.Users...)
	}

	return response, nil
}
