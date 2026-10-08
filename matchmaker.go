package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

const matchmakingSearchWindow = 45 * time.Second

type searchRecord struct {
	owner    string
	ticket   *mm.MatchmakingTicket
	created  time.Time
	tracking bool
}

type matchmakerServer struct {
	mm.UnimplementedMatchmakerServer
	rooms   *gameSessionServer
	mu      sync.Mutex
	tickets map[string]*searchRecord
}

func randomResourceID() (string, error) {
	b := make([]byte, 16)

	if _, err := rand.Read(b); err != nil {
		return "", status.Error(codes.Internal, "resource entropy unavailable")
	}

	return hex.EncodeToString(b), nil
}

func searchPool(t *mm.MatchmakingTicket) string {
	// Local Survival policy: exact mode/settings; skill is retained, not used
	// to claim an undocumented Nintendo skill-range contract.
	if t == nil || len(t.UserDefinitions) != 1 || t.UserDefinitions[0] == nil {
		return ""
	}

	mode := t.UserDefinitions[0].GetAttributes().GetFields()["gameMode"]

	if mode == nil {
		return ""
	}

	return t.MatchmakingConfig + "/" + mode.String()
}

func canonicalSearchTicket(name string) string {
	if strings.HasPrefix(name, "matchmakingTickets/") {
		name = "tenants/current/" + name
	}

	for _, prefix := range []string{
		"tenants/current/",
		"tenants/t-adf89f68/",
		"tenants/t-adf89f68-lp1/",
	} {
		if strings.HasPrefix(name, prefix+"matchmakingTickets/") {
			id := strings.TrimPrefix(name, prefix+"matchmakingTickets/")

			if len(id) == 32 && !strings.Contains(id, "/") {
				return "tenants/t-adf89f68-lp1/matchmakingTickets/" + id
			}
		}
	}

	return ""
}

func (m *matchmakerServer) CreateMatchmakingTicket(ctx context.Context, r *mm.CreateMatchmakingTicketRequest) (*mm.MatchmakingTicket, error) {
	owner, err := callerIdentity(ctx, m.rooms.p.identityKey)
	if err != nil {
		return nil, err
	}

	owner = canonicalUserSession(owner)

	if r.GetParent() != "tenants/current" && r.GetParent() != "tenants/t-adf89f68" && r.GetParent() != "tenants/t-adf89f68-lp1" {
		return nil, status.Error(codes.InvalidArgument, "Jamboree tenant required")
	}

	if len(r.GetUserDelegationTokens()) != 0 {
		return nil, status.Error(codes.Unimplemented, "delegated search unsupported")
	}

	in := r.GetMatchmakingTicket()

	if in == nil || proto.Size(in) > 64*1024 {
		return nil, status.Error(codes.InvalidArgument, "search ticket required")
	}

	if matchmakingConfigID(in.MatchmakingConfig) != "BqP4MatchmakingConfigs" || in.Backfill != nil || in.GameSession != nil {
		return nil, status.Error(codes.Unimplemented, "search configuration unsupported")
	}

	if len(in.UserDefinitions) != 1 || in.UserDefinitions[0] == nil {
		return nil, status.Error(codes.InvalidArgument, "one local user required")
	}

	t := proto.Clone(in).(*mm.MatchmakingTicket)
	d := t.UserDefinitions[0]

	if d.User != "" && d.User != "users/current" && !strings.HasSuffix(d.User, "/users/current") && canonicalUserSession(d.User) != owner {
		return nil, status.Error(codes.PermissionDenied, "search user must match caller")
	}

	mode := d.GetAttributes().GetFields()["gameMode"]

	if mode == nil || (mode.GetIntegerValue() != 34308 && mode.GetIntegerValue() != 1344260 && mode.GetIntegerValue() != 33284) {
		return nil, status.Error(codes.Unimplemented, "matchmaking mode unsupported")
	}

	d.User = owner
	t.MatchmakingConfig = "tenants/t-adf89f68-lp1/matchmakingConfigs/BqP4MatchmakingConfigs"
	id, err := randomResourceID()
	if err != nil {
		return nil, err
	}

	t.Name = "tenants/t-adf89f68-lp1/matchmakingTickets/" + id
	t.State = mm.MatchmakingTicket_SEARCHING
	t.MatchedUserSessions = nil
	t.GameSession = nil
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tickets == nil {
		m.tickets = map[string]*searchRecord{}
	}

	for n, v := range m.tickets {
		if time.Since(v.created) > 15*time.Minute {
			delete(m.tickets, n)
		} else if v.owner == owner && v.ticket.State == mm.MatchmakingTicket_SEARCHING {
			v.ticket.State = mm.MatchmakingTicket_CANCELLED
		}
	}

	if len(m.tickets) >= 256 {
		return nil, status.Error(codes.ResourceExhausted, "search ticket limit reached")
	}

	m.tickets[t.Name] = &searchRecord{owner: owner, ticket: t, created: time.Now()}
	m.rooms.p.event(map[string]any{
		"event":           "matchmaking_search_created",
		"players":         1,
		"timeout_seconds": 45,
		"game_mode":       mode.GetIntegerValue(),
	})

	return proto.Clone(t).(*mm.MatchmakingTicket), nil
}

// Both tickets must have active trackers: a cancelled/disconnected search
// cannot be reserved by another client. Called with m.mu held.
func (m *matchmakerServer) assignPair(r *searchRecord) error {
	all := []*searchRecord{}

	for _, v := range m.tickets {
		if v.tracking &&
			v.ticket.State == mm.MatchmakingTicket_SEARCHING &&
			time.Since(v.created) < matchmakingSearchWindow &&
			searchPool(v.ticket) == searchPool(r.ticket) {
			all = append(all, v)
		}
	}

	if len(all) < 2 {
		return nil
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].created.Before(all[j].created)
	})
	all = all[:2]

	if all[0].owner == all[1].owner {
		return nil
	}

	id, err := randomResourceID()
	if err != nil {
		return err
	}

	now := timestamppb.Now()
	gs := &mm.GameSession{
		Name:                    "tenants/t-adf89f68-lp1/gameSessions/" + id,
		MaxParticipantCount:     4,
		CurrentParticipantCount: 2,
		CanParticipate:          true,
		IsPublic:                true,
		State:                   mm.GameSession_CREATING,
		Host:                    "t-adf89f68-lp1.lp1.t.npln.srv.nintendo.net",
		Port:                    443,
		CreateTime:              now,
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"_BaseConfigName": textValue("BqP4MatchmakingConfigs"),
				"_AliasSuffix":    textValue(""),
			},
		},
	}

	defs := []*mm.UserDefinition{}
	matched := []*mm.MatchedUserSession{}

	for _, v := range all {
		d := proto.Clone(v.ticket.UserDefinitions[0]).(*mm.UserDefinition)
		defs = append(defs, d)
		uid, e := randomResourceID()
		if e != nil {
			return e
		}

		us := gs.Name + "/userSessions/" + uid
		proof, e := (&authServer{key: m.rooms.p.identityKey}).signSessionToken(v.owner+"|"+us, "matchmaking", 15*time.Minute)
		if e != nil {
			return e
		}

		gs.UserSessions = append(gs.UserSessions, &mm.UserSession{
			Name:        us,
			User:        v.owner,
			State:       mm.UserSession_RESERVED,
			CreateTime:  now,
			Attributes:  d.Attributes,
			Team:        d.Team,
			LatencyData: cloneLatencyData(d),
		})
		matched = append(matched, &mm.MatchedUserSession{
			UserDefinition:     d,
			UserSession:        us,
			MatchmakingIdToken: proof,
		})
	}

	creation := &mm.GameSessionCreationTicket{
		Name:              "tenants/t-adf89f68-lp1/gameSessionCreationTickets/" + id,
		State:             mm.GameSessionCreationTicket_SUCCEEDED,
		GameSession:       gs,
		UserDefinitions:   defs,
		MatchmakingConfig: all[0].ticket.MatchmakingConfig,
	}
	m.rooms.mu.Lock()

	if len(m.rooms.tickets) >= 256 {
		m.rooms.mu.Unlock()

		return status.Error(codes.ResourceExhausted, "room limit reached")
	}

	m.rooms.tickets[creation.Name] = &creationRecord{
		owner:   all[0].owner,
		ticket:  creation,
		expires: time.Now().Add(15 * time.Minute),
	}
	m.rooms.mu.Unlock()

	for i, v := range all {
		v.ticket.GameSession = proto.Clone(gs).(*mm.GameSession)
		v.ticket.MatchedUserSessions = []*mm.MatchedUserSession{matched[i]}
		v.ticket.State = mm.MatchmakingTicket_SUCCEEDED
	}

	m.rooms.p.event(map[string]any{
		"event":    "matchmaking_humans_assigned",
		"players":  2,
		"capacity": 4,
	})

	return nil
}

func (m *matchmakerServer) TrackMatchmakingTicket(req *mm.TrackMatchmakingTicketRequest, stream grpc.ServerStreamingServer[mm.MatchmakingTicket]) error {
	owner, err := callerIdentity(stream.Context(), m.rooms.p.identityKey)
	if err != nil {
		return err
	}

	owner = canonicalUserSession(owner)

	if len(req.UserDelegationTokens) > 0 {
		return status.Error(codes.Unimplemented, "delegated tracking unsupported")
	}

	name := canonicalSearchTicket(req.Name)
	m.mu.Lock()
	r := m.tickets[name]

	if r == nil {
		m.mu.Unlock()

		return status.Error(codes.NotFound, "search ticket not found")
	}

	if r.owner != owner {
		m.mu.Unlock()

		return status.Error(codes.PermissionDenied, "search belongs to another caller")
	}

	if r.tracking {
		m.mu.Unlock()

		return status.Error(codes.AlreadyExists, "search already tracked")
	}

	r.tracking = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		r.tracking = false

		if r.ticket.State == mm.MatchmakingTicket_SEARCHING {
			r.ticket.State = mm.MatchmakingTicket_CANCELLED
		}

		m.mu.Unlock()
	}()

	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()

	previous := mm.MatchmakingTicket_STATE_UNSPECIFIED

	for {
		m.mu.Lock()

		if r.ticket.State == mm.MatchmakingTicket_SEARCHING {
			if time.Since(r.created) >= matchmakingSearchWindow {
				r.ticket.State = mm.MatchmakingTicket_TIMED_OUT
				m.rooms.p.event(map[string]any{
					"event": "matchmaking_search_timed_out",
				})
			} else if err := m.assignPair(r); err != nil {
				m.mu.Unlock()

				return err
			}
		}

		out := proto.Clone(r.ticket).(*mm.MatchmakingTicket)
		m.mu.Unlock()

		if out.State != previous {
			if err := stream.Send(out); err != nil {
				return err
			}

			previous = out.State
		}

		if out.State != mm.MatchmakingTicket_SEARCHING {
			return nil
		}

		select {
		case <-stream.Context().Done():
			return stream.Context().Err()

		case <-tick.C:
		}
	}
}

func (m *matchmakerServer) CancelMatchmakingTicket(ctx context.Context, req *mm.CancelMatchmakingTicketRequest) (*emptypb.Empty, error) {
	owner, err := callerIdentity(ctx, m.rooms.p.identityKey)
	if err != nil {
		return nil, err
	}

	owner = canonicalUserSession(owner)
	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.tickets[canonicalSearchTicket(req.Name)]

	if r == nil {
		return nil, status.Error(codes.NotFound, "search ticket not found")
	}

	if r.owner != owner {
		return nil, status.Error(codes.PermissionDenied, "search belongs to another caller")
	}

	if r.ticket.State == mm.MatchmakingTicket_SUCCEEDED {
		return nil, status.Error(codes.FailedPrecondition, "search already assigned")
	}

	r.ticket.State = mm.MatchmakingTicket_CANCELLED

	return &emptypb.Empty{}, nil
}
