package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/test/bufconn"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func testDashboard(t *testing.T) (*serverContext, *dashboardMonitor) {
	t.Helper()

	t.Setenv("DASH_LISTEN", "127.0.0.1:8116")

	t.Setenv("DASH_TOKEN", strings.Repeat("test-token", 4))

	p := &serverContext{
		writer:      &bytes.Buffer{},
		identityKey: bytes.Repeat([]byte{7}, 32),
		accountIDs:  map[string]string{},
	}
	d, err := dashboardFromEnvironment(p)

	if err != nil {
		t.Fatal(err)
	}

	p.dashboard = d

	return p, d
}

func TestDashboardConfiguration(t *testing.T) {
	t.Setenv("DASH_LISTEN", "")

	if d, err := dashboardFromEnvironment(nil); d != nil || err != nil {
		t.Fatal("disabled dashboard requires no configuration")
	}

	t.Setenv("DASH_TOKEN", strings.Repeat("test", 8))

	for _, listen := range []string{
		"0.0.0.0:8116",
		"192.0.2.10:8116",
		"[::]:8116",
		":8116",
		"127.0.0.1:0",
		"127.0.0.1:65536",
	} {
		t.Setenv("DASH_LISTEN", listen)

		if _, err := dashboardFromEnvironment(nil); err == nil {
			t.Fatal("unsafe listener accepted", listen)
		}
	}

	for _, listen := range []string{"127.0.0.1:8116", "10.1.2.3:8116", "[fd00::1]:8116"} {
		t.Setenv("DASH_LISTEN", listen)

		if _, err := dashboardFromEnvironment(nil); err != nil {
			t.Fatal("internal listener rejected", err)
		}
	}

	t.Setenv("DASH_TOKEN", "")

	if _, err := dashboardFromEnvironment(nil); err == nil {
		t.Fatal("missing token accepted")
	}
}

func TestDashboardAuthenticationAndJSON(t *testing.T) {
	_, d := testDashboard(t)
	h := d.httpServer().Handler

	for _, key := range []string{"", "wrong"} {
		w := httptest.NewRecorder()

		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/stats?key="+key, nil))

		if w.Code != http.StatusUnauthorized {
			t.Fatal("invalid key accepted")
		}
	}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/stats?key="+d.token, nil))

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatal("unexpected method accepted")
	}

	w = httptest.NewRecorder()

	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/stats?key="+d.token, nil))

	var out map[string]json.RawMessage

	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("invalid statistics response")
	}

	for _, key := range []string{
		"serverTime",
		"uptimeSeconds",
		"connected",
		"inLobby",
		"activeLobbies",
		"peakConnected",
		"totalSessions",
		"server",
		"players",
		"gatherings",
	} {
		if out[key] == nil {
			t.Fatal("missing dashboard field", key)
		}
	}

	if string(out["players"]) != "[]" || string(out["gatherings"]) != "[]" || strings.Contains(w.Body.String(), d.token) {
		t.Fatal("empty arrays or token isolation failed")
	}
}

func TestDashboardAuthenticatedConnectionLifecycle(t *testing.T) {
	p, d := testDashboard(t)
	a := p.TagConn(context.Background(), &stats.ConnTagInfo{})
	b := p.TagConn(context.Background(), &stats.ConnTagInfo{})
	user := "tenants/current/users/test"
	token, err := (&authServer{
		key: p.identityKey,
	}).signSessionToken(user, "access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	p.TagRPC(a, &stats.RPCTagInfo{
		FullMethodName: "/test/unauthenticated",
	})

	if d.snapshot().Connected != 0 {
		t.Fatal("unauthenticated transport counted as a player")
	}

	for _, ctx := range []context.Context{
		a,
		a,
		b,
	} {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))

		p.TagRPC(ctx, &stats.RPCTagInfo{
			FullMethodName: "/test/authenticated",
		})
	}

	if out := d.snapshot(); out.Connected != 1 || out.TotalSessions != 1 || out.PeakConnected != 1 {
		t.Fatal("multiple channels duplicated a player", out)
	}

	p.HandleConn(a, &stats.ConnEnd{})

	if d.snapshot().Connected != 1 {
		t.Fatal("player removed while another channel remains")
	}

	p.HandleConn(b, &stats.ConnEnd{})

	if out := d.snapshot(); out.Connected != 0 || out.PeakConnected != 1 || out.TotalSessions != 1 {
		t.Fatal("disconnect did not preserve historical counters", out)
	}
}

func TestDashboardLiveRoomsAndHost(t *testing.T) {
	p, d := testDashboard(t)
	host := "tenants/current/users/host"
	guest := "tenants/current/users/guest"
	room := canonicalRoom("dashboard-room")
	p.accountIDs[host] = "000000000000000b"
	p.accountIDs[guest] = "0000000000000016"

	for _, user := range []string{
		host,
		guest,
	} {
		d.observe(d.connection(context.Background()), user)
	}

	d.rooms = &gameSessionServer{
		tickets: map[string]*creationRecord{
			"live": {
				owner:   host,
				expires: time.Now().Add(time.Hour),
				ticket: &mm.GameSessionCreationTicket{
					GameSession: &mm.GameSession{
						Name:                room,
						State:               mm.GameSession_ACTIVE,
						Password:            "private-room-password",
						MaxParticipantCount: 4,
						UserSessions: []*mm.UserSession{
							{
								User: host,
							},
							{
								User: guest,
							},
						},
					},
				},
			},
		},
	}
	d.gamesync = &gamesyncServer{
		subs: map[string]map[*gsSubscriber]bool{
			room: {
				{
					proof: host + "|" + room + "/userSessions/host",
				}: true,
				{
					proof: guest + "|" + room + "/userSessions/guest",
				}: true,
			},
		},
	}
	out := d.snapshot()

	if out.InLobby != 2 || out.ActiveLobbies != 1 || out.Gatherings[0].Count != 2 || out.Gatherings[0].HostPID != 11 || out.Gatherings[0].Max != 4 {
		t.Fatal("incorrect room counters", out)
	}

	if !out.Players[0].IsHost || out.Players[1].IsHost || out.Players[0].Gathering != out.Gatherings[0].ID {
		t.Fatal("host or gathering assignment failed", out)
	}

	wire, _ := json.Marshal(out)

	if strings.Contains(string(wire), "private-room-password") || strings.Contains(string(wire), host) {
		t.Fatal("private protocol data exposed")
	}

	d.gamesync.subs[room] = nil

	if out := d.snapshot(); out.InLobby != 0 || out.ActiveLobbies != 0 || out.Connected != 2 {
		t.Fatal("stale reservations reported as live lobby members", out)
	}
}

func TestDashboardConcurrentSnapshots(t *testing.T) {
	_, d := testDashboard(t)

	var workers sync.WaitGroup

	for i := 0; i < 8; i++ {
		workers.Go(func() {
			for j := 0; j < 20; j++ {
				ctx := d.connection(context.Background())

				d.observe(ctx, "tenants/current/users/shared")

				d.touch(ctx)

				d.snapshot()

				d.disconnect(ctx)
			}
		})
	}

	workers.Wait()

	if d.snapshot().Connected != 0 {
		t.Fatal("presence leaked after concurrent disconnects")
	}
}

func TestDashboardGRPCTransportIntegration(t *testing.T) {
	p, d := testDashboard(t)
	s := newServer(p)
	defer s.Stop()

	lis := bufconn.Listen(1 << 20)

	go s.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///dashboard-test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	token, err := (&authServer{
		key: p.identityKey,
	}).signSessionToken("tenants/current/users/transport", "access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))
	_, _ = mm.NewGameSessionServiceClient(conn).GetGameSession(ctx, &mm.GetGameSessionRequest{
		Name: canonicalRoom("missing"),
	})

	if d.snapshot().Connected != 1 {
		t.Fatal("authenticated gRPC transport was not tracked")
	}

	conn.Close()

	deadline := time.Now().Add(2 * time.Second)

	for d.snapshot().Connected != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	if d.snapshot().Connected != 0 {
		t.Fatal("closed gRPC transport retained its player")
	}
}
