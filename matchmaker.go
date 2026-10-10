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
	owner        string
	ticket       *mm.MatchmakingTicket
	created      time.Time
	tracking     bool
	trackContext context.Context
	trackers     map[string]context.Context
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
	// Match exact mode/settings; skill is retained, not used
	// to claim an undocumented Nintendo skill-range contract.
	if t == nil || len(t.UserDefinitions) == 0 || t.UserDefinitions[0] == nil {
		return ""
	}

	mode := t.UserDefinitions[0].GetAttributes().GetFields()["gameMode"]

	if mode == nil {
		return ""
	}

	return t.MatchmakingConfig + "/" + mode.String()
}

func activeSearch(r *searchRecord) bool {
	if !r.tracking {
		return false
	}

	if len(r.trackers) == 0 {
		return r.trackContext == nil || r.trackContext.Err() == nil
	}

	for _, ctx := range r.trackers {
		if ctx.Err() == nil {
			return true
		}
	}

	return false
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

	in := r.GetMatchmakingTicket()

	if in == nil || proto.Size(in) > 64*1024 {
		return nil, status.Error(codes.InvalidArgument, "search ticket required")
	}

	config := matchmakingConfigID(in.MatchmakingConfig)

	supportedConfig := config == "BqP4MatchmakingConfigs" ||
		config == "BqP4NoEasePaticipantConfigs" ||
		config == "BqP8MatchmakingConfigs" ||
		config == "BqP20MatchmakingConfigs"

	if !supportedConfig || in.Backfill != nil || in.GameSession != nil {
		return nil, status.Error(codes.Unimplemented, "search configuration unsupported")
	}

	if len(in.UserDefinitions) < 1 || len(in.UserDefinitions) > 2 {
		return nil, status.Error(codes.InvalidArgument, "one user or one pair required")
	}

	t := proto.Clone(in).(*mm.MatchmakingTicket)
	t.UserDefinitions, err = m.rooms.authorizeSearchUsers(owner, in.UserDefinitions, r.UserDelegationTokens)

	if err != nil {
		return nil, err
	}

	d := t.UserDefinitions[0]

	mode := d.GetAttributes().GetFields()["gameMode"]

	// Daily Challenges uses 0x8304 in the low word and a varying challenge
	// identifier in the high word. Keep the full value for pool matching.
	modeID := mode.GetIntegerValue()
	dailyChallenge := modeID >= 0 && modeID <= 0xffffffff && modeID&0xffff == 0x8304

	// Captured Kaboom request: BqP8MatchmakingConfigs, gameMode 0x18804.
	// Preserve the settings in the high word when matching search pools.
	kaboom := modeID >= 0 && modeID <= 0xffffffff && modeID&0xffff == 0x8804

	// Citron's captured Koopathlon request uses BqP20, gameMode 34564 (0x8704).
	// Preserve the complete settings value in searchPool, as in the other modes.
	koopathlon := modeID >= 0 && modeID <= 0xffffffff && modeID&0xffff == 0x8704

	// Captured board searches: Goomba Lagoon Party 0x8004, Pro 0x8104;
	// Mega Wiggler's Tree Party Party 0x68004. The high word carries board
	// settings; preserve the complete uint32 mode to isolate search pools.
	partyBoard := modeID >= 0 && modeID <= 0xffffffff && (modeID&0xffff == 0x8004 || modeID&0xffff == 0x8104)

	if mode == nil ||
		(modeID != 34308 && modeID != 33284 && modeID != 33796 &&
			!dailyChallenge && !kaboom && !koopathlon && !partyBoard) {
		return nil, status.Error(codes.Unimplemented, "matchmaking mode unsupported")
	}

	tagMatch := modeID == 33796 && config == "BqP4NoEasePaticipantConfigs"

	if koopathlon != (config == "BqP20MatchmakingConfigs") ||
		kaboom != (config == "BqP8MatchmakingConfigs") ||
		(modeID == 33796) != (config == "BqP4NoEasePaticipantConfigs") ||
		(len(t.UserDefinitions) > 1 && !tagMatch) {
		return nil, status.Error(codes.InvalidArgument, "mode and group configuration disagree")
	}

	for _, definition := range t.UserDefinitions {
		if !proto.Equal(mode, definition.GetAttributes().GetFields()["gameMode"]) {
			return nil, status.Error(codes.InvalidArgument, "group modes must agree")
		}
	}

	t.MatchmakingConfig = "tenants/t-adf89f68-lp1/matchmakingConfigs/" + config
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

	m.tickets[t.Name] = &searchRecord{
		owner:   owner,
		ticket:  t,
		created: time.Now(),
	}
	m.rooms.p.event(map[string]any{
		"event":           "matchmaking_search_created",
		"players":         len(t.UserDefinitions),
		"timeout_seconds": 45,
		"game_mode":       mode.GetIntegerValue(),
	})

	return proto.Clone(t).(*mm.MatchmakingTicket), nil
}

// Assign the configured capacity immediately, or the available participants when
// the oldest search reaches its deadline. Called with m.mu held.
func (m *matchmakerServer) assignGroup(r *searchRecord) error {
	pool := searchPool(r.ticket)

	if pool == "" {
		return nil
	}

	all := []*searchRecord{}

	for _, v := range m.tickets {
		if activeSearch(v) &&
			v.ticket.State == mm.MatchmakingTicket_SEARCHING &&
			searchPool(v.ticket) == pool {
			all = append(all, v)
		}
	}

	if len(all) == 0 {
		return nil
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].created.Before(all[j].created)
	})
	capacity := 4

	if matchmakingConfigID(r.ticket.MatchmakingConfig) == "BqP8MatchmakingConfigs" {
		capacity = 8
	}

	if matchmakingConfigID(r.ticket.MatchmakingConfig) == "BqP20MatchmakingConfigs" {
		capacity = 20
	}

	participants := make([]*searchRecord, 0, capacity)
	users := map[string]bool{}
	count := 0

	for _, candidate := range all {
		duplicate := false

		for _, d := range candidate.ticket.UserDefinitions {
			duplicate = duplicate || users[d.User]
		}

		size := len(candidate.ticket.UserDefinitions)

		if duplicate || count+size > capacity {
			continue
		}

		for _, d := range candidate.ticket.UserDefinitions {
			users[d.User] = true
		}

		participants = append(participants, candidate)
		count += size
	}

	if len(participants) == 0 {
		return nil
	}

	all = participants

	// A lone Tag Match pair is the requesting team, not an opponent.
	// Report the native timeout instead of replacing its companion session
	// with a successful match against itself; the client owns CPU fallback.
	if matchmakingConfigID(r.ticket.MatchmakingConfig) == "BqP4NoEasePaticipantConfigs" && count < 4 {
		return nil
	}

	if count < capacity && time.Since(all[0].created) < matchmakingSearchWindow {
		return nil
	}

	id, err := randomResourceID()

	if err != nil {
		return err
	}

	now := timestamppb.Now()
	gs := &mm.GameSession{
		Name:                    "tenants/t-adf89f68-lp1/gameSessions/" + id,
		MaxParticipantCount:     int32(capacity),
		CurrentParticipantCount: int32(count),
		CanParticipate:          true,
		IsPublic:                true,
		State:                   mm.GameSession_CREATING,
		Host:                    "t-adf89f68-lp1.lp1.t.npln.srv.nintendo.net",
		Port:                    443,
		CreateTime:              now,
		Properties: &common.MapValue{
			Fields: map[string]*common.Value{
				"_BaseConfigName": textValue(matchmakingConfigID(all[0].ticket.MatchmakingConfig)),
				"_AliasSuffix":    textValue(""),
			},
		},
	}

	defs := []*mm.UserDefinition{}
	matched := map[*searchRecord][]*mm.MatchedUserSession{}

	for _, v := range all {
		for _, definition := range v.ticket.UserDefinitions {
			d := proto.Clone(definition).(*mm.UserDefinition)
			defs = append(defs, d)
			uid, e := randomResourceID()

			if e != nil {
				return e
			}

			us := gs.Name + "/userSessions/" + uid
			proof, e := (&authServer{key: m.rooms.p.identityKey}).signSessionToken(d.User+"|"+us, "matchmaking", 15*time.Minute)

			if e != nil {
				return e
			}

			gs.UserSessions = append(gs.UserSessions, &mm.UserSession{
				Name:        us,
				User:        d.User,
				State:       mm.UserSession_RESERVED,
				CreateTime:  now,
				Attributes:  d.Attributes,
				Team:        d.Team,
				LatencyData: cloneLatencyData(d),
			})

			if d.User != v.owner {
				proof = ""
			}

			matched[v] = append(matched[v], &mm.MatchedUserSession{
				UserDefinition:     d,
				UserSession:        us,
				MatchmakingIdToken: proof,
			})
		}
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

	for _, v := range all {
		v.ticket.GameSession = proto.Clone(gs).(*mm.GameSession)
		v.ticket.MatchedUserSessions = matched[v]
		v.ticket.State = mm.MatchmakingTicket_SUCCEEDED
	}

	m.rooms.p.event(map[string]any{
		"event":    "matchmaking_humans_assigned",
		"players":  count,
		"capacity": capacity,
	})

	return nil
}

func (m *matchmakerServer) TrackMatchmakingTicket(req *mm.TrackMatchmakingTicketRequest, stream grpc.ServerStreamingServer[mm.MatchmakingTicket]) error {
	owner, err := callerIdentity(stream.Context(), m.rooms.p.identityKey)

	if err != nil {
		return err
	}

	owner = canonicalUserSession(owner)

	if _, err := m.rooms.delegatedParticipants(owner, req.UserDelegationTokens); err != nil {
		m.rooms.p.event(map[string]any{
			"event":  "matchmaking_tracking_rejected",
			"reason": "invalid_delegation",
		})

		return err
	}

	name := canonicalSearchTicket(req.Name)
	m.mu.Lock()
	r := m.tickets[name]

	if r == nil {
		m.mu.Unlock()

		return status.Error(codes.NotFound, "search ticket not found")
	}

	if r.owner != owner {
		participant := false

		for _, definition := range r.ticket.UserDefinitions {
			participant = participant || definition.User == owner
		}

		if !participant {
			m.rooms.p.event(map[string]any{
				"event":  "matchmaking_tracking_rejected",
				"reason": "not_participant",
			})
			m.mu.Unlock()

			return status.Error(codes.PermissionDenied, "caller is not a search participant")
		}
	}

	if r.trackers == nil {
		r.trackers = map[string]context.Context{}
	}

	if r.trackers[owner] != nil {
		m.mu.Unlock()

		return status.Error(codes.AlreadyExists, "search already tracked")
	}

	r.tracking = true
	r.trackers[owner] = stream.Context()
	r.trackContext = nil
	m.rooms.p.event(map[string]any{
		"event":           "matchmaking_search_tracking",
		"creator":         owner == r.owner,
		"active_trackers": len(r.trackers),
	})
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(r.trackers, owner)
		r.tracking = false

		for _, ctx := range r.trackers {
			r.tracking = r.tracking || ctx.Err() == nil
		}

		if !r.tracking && r.ticket.State == mm.MatchmakingTicket_SEARCHING {
			r.ticket.State = mm.MatchmakingTicket_CANCELLED
		}

		m.mu.Unlock()
	}()

	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()

	previous := mm.MatchmakingTicket_STATE_UNSPECIFIED

	for {
		if err := stream.Context().Err(); err != nil {
			return err
		}

		m.mu.Lock()

		if r.ticket.State == mm.MatchmakingTicket_SEARCHING {
			if err := m.assignGroup(r); err != nil {
				m.mu.Unlock()

				return err
			}

			if r.ticket.State == mm.MatchmakingTicket_SEARCHING && time.Since(r.created) >= matchmakingSearchWindow {
				r.ticket.State = mm.MatchmakingTicket_TIMED_OUT
				m.rooms.p.event(map[string]any{
					"event": "matchmaking_search_timed_out",
				})
			}
		}

		out := proto.Clone(r.ticket).(*mm.MatchmakingTicket)
		m.mu.Unlock()

		if out.State == mm.MatchmakingTicket_SUCCEEDED {
			for _, matched := range out.MatchedUserSessions {
				matched.MatchmakingIdToken = ""

				if matched.GetUserDefinition().GetUser() == owner {
					proof, err := (&authServer{key: m.rooms.p.identityKey}).signSessionToken(owner+"|"+matched.UserSession, "matchmaking", 15*time.Minute)

					if err != nil {
						return err
					}

					matched.MatchmakingIdToken = proof
				}
			}
		}

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
