package main

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func (s *gameSessionServer) AllocateIceServerSet(ctx context.Context, req *mm.AllocateIceServerSetRequest) (*mm.IceServerSet, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)

	if e != nil {
		return nil, e
	}

	t := strings.TrimPrefix(req.GetTenant(), "tenants/")

	if t != "current" && t != "t-adf89f68" && t != "t-adf89f68-lp1" {
		return nil, status.Error(codes.InvalidArgument, "Jamboree tenant required")
	}

	user := req.GetUser()

	if user != "" && user != "users/current" && user != "tenants/current/users/current" && user != owner {
		return nil, status.Error(codes.PermissionDenied, "ICE user must match caller")
	}

	username, password, e := newTURNCredentials(s.p.identityKey)

	if e != nil {
		return nil, status.Error(codes.Internal, "TURN credential entropy unavailable")
	}

	s.p.event(map[string]any{
		"event":               "ice_server_set_allocated",
		"caller_digest":       identityDigest(owner),
		"stun_port":           17478,
		"turn_port":           17479,
		"turn_servers":        1,
		"protocol":            "UDP",
		"credentials_present": username != "" && password != "",
		"ttl_seconds":         900,
		"cache_seconds":       60,
	})

	return &mm.IceServerSet{
		Name: "tenants/t-adf89f68-lp1/iceServerSets/default",
		StunServer: &mm.StunServer{
			Host:     "51.178.29.194",
			Port:     17478,
			Protocol: mm.StunServer_UDP,
		},
		TurnServers: []*mm.TurnServer{
			{
				Host:     "51.178.29.194",
				Port:     17479,
				Protocol: mm.TurnServer_UDP,
				Username: username,
				Password: password,
			},
		},
		Ttl:                 durationpb.New(15 * time.Minute),
		ClientCacheDuration: durationpb.New(time.Minute),
		UpdateTime:          timestamppb.Now(),
	}, nil
}
