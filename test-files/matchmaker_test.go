package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

type searchTestStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent chan *mm.MatchmakingTicket
}

func (s *searchTestStream) Context() context.Context {
	return s.ctx
}
func (s *searchTestStream) Send(t *mm.MatchmakingTicket) error {
	s.sent <- proto.Clone(t).(*mm.MatchmakingTicket)

	return nil
}

func TestSurvivalMatchmakerPairAndGuestFirstGamesync(t *testing.T) {
	t.Run("Survival", func(t *testing.T) {
		testMatchmakerPairAndGuestFirstGamesync(t, 34308)
	})

	t.Run("DailyChallenges", func(t *testing.T) {
		testMatchmakerPairAndGuestFirstGamesync(t, 1344260)
	})

	t.Run("Showdown", func(t *testing.T) {
		testMatchmakerPairAndGuestFirstGamesync(t, 33284)
	})
}

func testMatchmakerPairAndGuestFirstGamesync(t *testing.T, mode int64) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	g := &gameSessionServer{
		p:       p,
		tickets: map[string]*creationRecord{},
	}
	m := &matchmakerServer{
		rooms:   g,
		tickets: map[string]*searchRecord{},
	}
	ctxFor := func(user string) context.Context {
		token, _ := (&authServer{
			key: key,
		}).signSessionToken("tenants/current/users/"+user, "access", time.Minute)

		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	}
	a, b := ctxFor("a"), ctxFor("b")
	req := &mm.CreateMatchmakingTicketRequest{
		Parent: "tenants/current",
		MatchmakingTicket: &mm.MatchmakingTicket{
			MatchmakingConfig: "tenants/current/matchmakingConfigs/BqP4MatchmakingConfigs",
			UserDefinitions: []*mm.UserDefinition{
				{
					User: "users/current",
					Attributes: &common.MapValue{
						Fields: map[string]*common.Value{
							"gameMode": intValue(34308),
							"udemae":   intValue(0),
						},
					},
				},
			},
		},
	}
	req.MatchmakingTicket.UserDefinitions[0].Attributes.Fields["gameMode"] = intValue(mode)

	if mode != 34308 {
		delete(req.MatchmakingTicket.UserDefinitions[0].Attributes.Fields, "udemae")
	}

	before := proto.Clone(req)
	one, err := m.CreateMatchmakingTicket(a, req)

	if err != nil {
		t.Fatal(err)
	}

	if !proto.Equal(before, req) || one.State != mm.MatchmakingTicket_SEARCHING || one.GameSession != nil {
		t.Fatal("create fabricated success or mutated request")
	}

	if _, err := m.CancelMatchmakingTicket(b, &mm.CancelMatchmakingTicketRequest{
		Name: one.Name,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("cross-user cancel", err)
	}

	two, err := m.CreateMatchmakingTicket(b, req)

	if err != nil {
		t.Fatal(err)
	}

	ca, cancelA := context.WithCancel(a)
	defer cancelA()

	cb, cancelB := context.WithCancel(b)
	defer cancelB()

	sa := &searchTestStream{
		ctx:  ca,
		sent: make(chan *mm.MatchmakingTicket, 4),
	}
	sb := &searchTestStream{
		ctx:  cb,
		sent: make(chan *mm.MatchmakingTicket, 4),
	}
	done := make(chan error, 2)

	go func() {
		done <- m.TrackMatchmakingTicket(&mm.TrackMatchmakingTicketRequest{
			Name: one.Name,
		}, sa)
	}()

	select {
	case out := <-sa.sent:
		if out.State != mm.MatchmakingTicket_SEARCHING {
			t.Fatal("single user assigned")
		}

	case <-time.After(time.Second):
		t.Fatal("no searching state")
	}

	go func() {
		done <- m.TrackMatchmakingTicket(&mm.TrackMatchmakingTicketRequest{
			Name: two.Name,
		}, sb)
	}()

	select {
	case out := <-sb.sent:
		if out.State != mm.MatchmakingTicket_SEARCHING {
			t.Fatal("partial group assigned before deadline")
		}

	case <-time.After(time.Second):
		t.Fatal("second participant did not begin tracking")
	}

	m.mu.Lock()

	m.tickets[one.Name].created = time.Now().Add(-matchmakingSearchWindow)

	m.mu.Unlock()

	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}

		case <-time.After(2 * time.Second):
			t.Fatal("pair did not complete")
		}
	}

	m.mu.Lock()

	outA := proto.Clone(m.tickets[one.Name].ticket).(*mm.MatchmakingTicket)
	outB := proto.Clone(m.tickets[two.Name].ticket).(*mm.MatchmakingTicket)

	m.mu.Unlock()

	if outA.State != mm.MatchmakingTicket_SUCCEEDED || outA.GameSession.Name != outB.GameSession.Name || outA.GameSession.CurrentParticipantCount != 2 || len(g.tickets) != 1 {
		t.Fatal("pair not assigned to one room")
	}

	if outA.MatchedUserSessions[0].UserSession == outB.MatchedUserSessions[0].UserSession {
		t.Fatal("participant identity shared")
	}

	for _, out := range []*mm.MatchmakingTicket{
		outA,
		outB,
	} {
		v := out.MatchedUserSessions[0]
		proof, err := verifySessionProof(v.MatchmakingIdToken, key, "matchmaking")

		if err != nil || proof != v.UserDefinition.User+"|"+v.UserSession {
			t.Fatal("incorrect proof", err)
		}
	}

	gs := &gamesyncServer{
		p:     p,
		rooms: g,
	}
	v := outB.MatchedUserSessions[0]

	if err := gs.initializeSessionDocuments(v.UserDefinition.User + "|" + v.UserSession); err != nil {
		t.Fatal(err)
	}

	docs := gs.documents[outA.GameSession.Name]

	for i, u := range outA.GameSession.UserSessions {
		doc := docs["docs/__us/"+u.Name[len(outA.GameSession.Name+"/userSessions/"):]]

		if doc == nil || doc.Fields.Fields["ussid"].GetIntegerValue() != int64(i+1) {
			t.Fatal("wrong station index")
		}
	}
}

func TestMatchmakerSeparatesDailyAndSurvival(t *testing.T) {
	p := &serverContext{
		writer: &bytes.Buffer{},
	}
	g := &gameSessionServer{
		p:       p,
		tickets: map[string]*creationRecord{},
	}
	m := &matchmakerServer{
		rooms:   g,
		tickets: map[string]*searchRecord{},
	}

	for i, mode := range []int64{34308, 1344260, 33284} {
		name := string(rune('a' + i))
		m.tickets[name] = &searchRecord{
			owner:    name,
			created:  time.Now(),
			tracking: true,
			ticket: &mm.MatchmakingTicket{
				State:             mm.MatchmakingTicket_SEARCHING,
				MatchmakingConfig: "tenants/t-adf89f68-lp1/matchmakingConfigs/BqP4MatchmakingConfigs",
				UserDefinitions: []*mm.UserDefinition{
					{
						Attributes: &common.MapValue{
							Fields: map[string]*common.Value{
								"gameMode": intValue(mode),
							},
						},
					},
				},
			},
		}
	}

	if err := m.assignGroup(m.tickets["a"]); err != nil {
		t.Fatal(err)
	}

	if len(g.tickets) != 0 || m.tickets["a"].ticket.State != mm.MatchmakingTicket_SEARCHING || m.tickets["b"].ticket.State != mm.MatchmakingTicket_SEARCHING || m.tickets["c"].ticket.State != mm.MatchmakingTicket_SEARCHING {
		t.Fatal("mixed game modes were assigned")
	}
}

func TestSurvivalMatchmakerTimeoutAndDisconnectedSearch(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	g := &gameSessionServer{
		p:       p,
		tickets: map[string]*creationRecord{},
	}
	m := &matchmakerServer{
		rooms:   g,
		tickets: map[string]*searchRecord{},
	}
	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/a", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	owner, _ := callerIdentity(ctx, key)
	owner = canonicalUserSession(owner)
	name := "tenants/t-adf89f68-lp1/matchmakingTickets/01234567890123456789012345678901"
	m.tickets[name] = &searchRecord{
		owner:   owner,
		created: time.Now().Add(-matchmakingSearchWindow),
		ticket: &mm.MatchmakingTicket{
			Name:  name,
			State: mm.MatchmakingTicket_SEARCHING,
		},
	}
	s := &searchTestStream{
		ctx:  ctx,
		sent: make(chan *mm.MatchmakingTicket, 2),
	}

	if err := m.TrackMatchmakingTicket(&mm.TrackMatchmakingTicketRequest{
		Name: name,
	}, s); err != nil {
		t.Fatal(err)
	}

	if out := <-s.sent; out.State != mm.MatchmakingTicket_TIMED_OUT || out.GameSession != nil || len(g.tickets) != 0 {
		t.Fatal("timeout fabricated room")
	}

	m.tickets[name].created = time.Now()
	m.tickets[name].ticket.State = mm.MatchmakingTicket_SEARCHING
	c, cancel := context.WithCancel(ctx)

	cancel()

	s.ctx = c

	if err := m.TrackMatchmakingTicket(&mm.TrackMatchmakingTicketRequest{
		Name: name,
	}, s); err == nil {
		t.Fatal("cancel ignored")
	}

	if m.tickets[name].ticket.State != mm.MatchmakingTicket_CANCELLED {
		t.Fatal("disconnected search remained queued")
	}
}
