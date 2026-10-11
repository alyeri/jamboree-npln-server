package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

type dashboardConnectionKey struct{}

type dashboardPresence struct {
	user        string
	started     time.Time
	activity    time.Time
	connections map[uint64]bool
}

type dashboardMonitor struct {
	mu          sync.Mutex
	p           *serverContext
	rooms       *gameSessionServer
	gamesync    *gamesyncServer
	started     time.Time
	token       string
	next        uint64
	connections map[uint64]map[string]bool
	players     map[string]*dashboardPresence
	peak        int
	total       uint64
}

type dashboardPlayer struct {
	PID           uint64 `json:"pid"`
	Name          string `json:"name"`
	IP            string `json:"ip"`
	State         string `json:"state"`
	Gathering     uint64 `json:"gathering"`
	OnlineSeconds int64  `json:"onlineSeconds"`
	IdleSeconds   int64  `json:"idleSeconds"`
	IsHost        bool   `json:"isHost"`
}

type dashboardMember struct {
	PID  uint64 `json:"pid"`
	Name string `json:"name"`
	Host bool   `json:"host"`
}

type dashboardGathering struct {
	ID       uint64            `json:"id"`
	HostPID  uint64            `json:"hostPid"`
	HostName string            `json:"hostName"`
	Mode     string            `json:"mode"`
	Count    int               `json:"count"`
	Max      int32             `json:"max"`
	State    string            `json:"state"`
	Players  []dashboardMember `json:"players"`
}

type dashboardStats struct {
	ServerTime    string `json:"serverTime"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
	Connected     int    `json:"connected"`
	InLobby       int    `json:"inLobby"`
	ActiveLobbies int    `json:"activeLobbies"`
	PeakConnected int    `json:"peakConnected"`
	TotalSessions uint64 `json:"totalSessions"`
	Server        struct {
		AccessKey string `json:"accessKey"`
	} `json:"server"`
	Players    []dashboardPlayer    `json:"players"`
	Gatherings []dashboardGathering `json:"gatherings"`
}

func dashboardFromEnvironment(p *serverContext) (*dashboardMonitor, error) {
	listen := os.Getenv("DASH_LISTEN")

	if listen == "" {
		return nil, nil
	}

	host, port, err := net.SplitHostPort(listen)
	ip := net.ParseIP(host)
	portNumber, portError := strconv.Atoi(port)

	if err != nil || ip == nil || (!ip.IsPrivate() && !ip.IsLoopback()) || ip.IsUnspecified() || portError != nil || portNumber < 1 || portNumber > 65535 {
		return nil, errors.New("DASH_LISTEN must use a private or loopback IP and a valid port")
	}

	token := os.Getenv("DASH_TOKEN")

	if len(token) < 32 {
		return nil, errors.New("DASH_TOKEN must contain at least 32 characters")
	}

	return &dashboardMonitor{
		p:           p,
		started:     time.Now(),
		token:       token,
		connections: map[uint64]map[string]bool{},
		players:     map[string]*dashboardPresence{},
	}, nil
}

func (d *dashboardMonitor) connection(ctx context.Context) context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.next++

	d.connections[d.next] = map[string]bool{}

	return context.WithValue(ctx, dashboardConnectionKey{}, d.next)
}

func (d *dashboardMonitor) observe(ctx context.Context, user string) {
	id, _ := ctx.Value(dashboardConnectionKey{}).(uint64)
	user = canonicalUserSession(user)

	d.mu.Lock()
	defer d.mu.Unlock()

	connection, ok := d.connections[id]

	if !ok || user == "" {
		return
	}

	now := time.Now()
	p := d.players[user]

	if p == nil {
		p = &dashboardPresence{
			user:        user,
			started:     now,
			connections: map[uint64]bool{},
		}
		d.players[user] = p

		d.total++

		if len(d.players) > d.peak {
			d.peak = len(d.players)
		}
	}

	p.activity = now
	p.connections[id] = true
	connection[user] = true
}

func (d *dashboardMonitor) touch(ctx context.Context) {
	id, _ := ctx.Value(dashboardConnectionKey{}).(uint64)

	d.mu.Lock()
	defer d.mu.Unlock()

	for user := range d.connections[id] {
		d.players[user].activity = time.Now()
	}
}

func (d *dashboardMonitor) disconnect(ctx context.Context) {
	id, _ := ctx.Value(dashboardConnectionKey{}).(uint64)

	d.mu.Lock()
	defer d.mu.Unlock()

	for user := range d.connections[id] {
		p := d.players[user]

		delete(p.connections, id)

		if len(p.connections) == 0 {
			delete(d.players, user)
		}
	}

	delete(d.connections, id)
}

// Session resources are opaque strings. Keep dashboard IDs within JavaScript's
// exact integer range, without returning matchmaking tokens or room passwords.
func dashboardGatheringID(name string) uint64 {
	digest := sha256.Sum256([]byte(name))

	return (binary.BigEndian.Uint64(digest[:8]) & ((1 << 53) - 1)) | 1
}

func dashboardMode(room *mm.GameSession) string {
	value := room.GetProperties().GetFields()["gameMode"]

	if value == nil {
		return "jamboree"
	}

	return "jamboree-mode-" + strconv.FormatInt(value.GetIntegerValue(), 10)
}

func (d *dashboardMonitor) snapshot() dashboardStats {
	now := time.Now()
	result := dashboardStats{
		ServerTime:    now.UTC().Format(time.RFC3339),
		UptimeSeconds: int64(now.Sub(d.started).Seconds()),
		Players:       []dashboardPlayer{},
		Gatherings:    []dashboardGathering{},
	}
	users := map[string]dashboardPlayer{}

	d.mu.Lock()

	result.PeakConnected = d.peak
	result.TotalSessions = d.total

	for user, presence := range d.players {
		users[user] = dashboardPlayer{
			State:         "connected",
			OnlineSeconds: int64(now.Sub(presence.started).Seconds()),
			IdleSeconds:   int64(now.Sub(presence.activity).Seconds()),
		}
	}

	d.mu.Unlock()

	d.p.mu.Lock()

	accountIDs := map[string]uint64{}

	for user, account := range d.p.accountIDs {
		accountIDs[canonicalUserSession(user)], _ = strconv.ParseUint(account, 16, 64)
	}

	d.p.mu.Unlock()

	for user, player := range users {
		player.PID = accountIDs[user]
		users[user] = player
	}

	live := map[string]map[string]bool{}

	if d.gamesync != nil {
		d.gamesync.mu.Lock()

		for room, subscribers := range d.gamesync.subs {
			live[room] = map[string]bool{}

			for sub := range subscribers {
				user := canonicalUserSession(strings.Split(sub.proof, "|")[0])
				live[room][user] = true
			}
		}

		d.gamesync.mu.Unlock()
	}

	if d.rooms != nil {
		d.rooms.mu.Lock()

		for _, record := range d.rooms.tickets {
			room := record.ticket.GetGameSession()

			if !record.expires.After(now) || room.GetState() != mm.GameSession_ACTIVE {
				continue
			}

			owner := canonicalUserSession(record.owner)
			gathering := dashboardGathering{
				ID:      dashboardGatheringID(room.Name),
				HostPID: accountIDs[owner],
				Mode:    dashboardMode(room),
				Max:     room.MaxParticipantCount,
				State:   strings.ToLower(room.State.String()),
				Players: []dashboardMember{},
			}

			for _, session := range room.UserSessions {
				user := canonicalUserSession(session.GetUser())
				player, connected := users[user]

				if !connected || !live[room.Name][user] || player.Gathering != 0 {
					continue
				}

				player.State = "in-lobby"
				player.Gathering = gathering.ID
				player.IsHost = user == owner
				users[user] = player
				gathering.Players = append(gathering.Players, dashboardMember{
					PID:  player.PID,
					Host: player.IsHost,
				})

				result.InLobby++
			}

			gathering.Count = len(gathering.Players)

			if gathering.Count > 0 {
				sort.Slice(gathering.Players, func(i, j int) bool {
					return gathering.Players[i].PID < gathering.Players[j].PID
				})

				result.Gatherings = append(result.Gatherings, gathering)
			}
		}

		d.rooms.mu.Unlock()
	}

	for _, player := range users {
		result.Players = append(result.Players, player)
	}

	sort.Slice(result.Players, func(i, j int) bool {
		return result.Players[i].PID < result.Players[j].PID
	})

	sort.Slice(result.Gatherings, func(i, j int) bool {
		return result.Gatherings[i].ID < result.Gatherings[j].ID
	})

	result.Connected = len(result.Players)
	result.ActiveLobbies = len(result.Gatherings)

	return result
}

func (d *dashboardMonitor) httpServer() *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")

		provided := sha256.Sum256([]byte(r.URL.Query().Get("key")))
		expected := sha256.Sum256([]byte(d.token))

		if subtle.ConstantTimeCompare(provided[:], expected[:]) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)

			return
		}

		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)

			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

			return
		}

		w.Header().Set("Content-Type", "application/json")

		_ = json.NewEncoder(w).Encode(d.snapshot())
	})

	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    4096,
	}
}
