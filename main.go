package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
	frpb "github.com/alyeri/jamboree-npln-server/proto/friends/v1"
	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	lbpb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
	mmpb "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
	ugcpb "github.com/alyeri/jamboree-npln-server/proto/ugcstore/v1"
)

var serverVersion = "development"

type rpcKey struct{}
type rpcState struct {
	id      uint64
	method  string
	started time.Time
	in, out atomic.Int64
}
type serverContext struct {
	prearrangedTokens prearrangedTokenCache
	capture           *rpcCapture
	identityKey       []byte
	accountIDs        map[string]string
	mu                sync.Mutex
	writer            io.Writer
	next              atomic.Uint64
}

func (p *serverContext) event(e map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()

	e["time"] = time.Now().UTC().Format(time.RFC3339Nano)
	e["build"] = serverVersion
	_ = json.NewEncoder(p.writer).Encode(e)
}

// Metadata values are never logged. Only this non-secret identifier is retained.
func tenant(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("npln-tenant-id")

	if len(v) == 1 && (v[0] == "t-adf89f68" || v[0] == "t-adf89f68-lp1") {
		return v[0]
	}

	if len(v) > 0 {
		return "other-or-invalid"
	}

	return "absent"
}
func safeMethod(s string) string {
	if len(s) > 256 || !strings.HasPrefix(s, "/") {
		return "invalid-method"
	}

	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("/._", c)) {
			return "invalid-method"
		}
	}

	return s
}
func (p *serverContext) TagRPC(ctx context.Context, i *stats.RPCTagInfo) context.Context {
	r := &rpcState{
		id:      p.next.Add(1),
		method:  safeMethod(i.FullMethodName),
		started: time.Now(),
	}

	p.event(map[string]any{
		"event":  "rpc_start",
		"rpc_id": r.id,
		"method": r.method,
		"tenant": tenant(ctx),
	})

	return context.WithValue(ctx, rpcKey{}, r)
}
func (p *serverContext) HandleRPC(ctx context.Context, s stats.RPCStats) {
	r, _ := ctx.Value(rpcKey{}).(*rpcState)

	if r == nil {
		return
	}

	switch v := s.(type) {
	case *stats.InPayload:
		p.captureRPC(ctx, r, "request", v.Payload, "")
		r.in.Add(1)
		p.event(map[string]any{
			"event":     "message",
			"rpc_id":    r.id,
			"direction": "in",
			"bytes":     v.Length,
		})

	case *stats.OutPayload:
		p.captureRPC(ctx, r, "response", v.Payload, "")
		r.out.Add(1)
		p.event(map[string]any{
			"event":     "message",
			"rpc_id":    r.id,
			"direction": "out",
			"bytes":     v.Length,
		})

	case *stats.End:
		if r.method == "/nn.npln.auth.v1.Auth/IssuePrearrangedUserToken" && v.Error != nil && r.in.Load() == 0 {
			p.event(map[string]any{
				"event":  "prearranged_transport_error",
				"rpc_id": r.id,
				"kind":   prearrangedTransportErrorKind(v.Error),
			})
		}
		p.captureRPC(ctx, r, "end", nil, status.Code(v.Error).String())
		p.event(map[string]any{
			"event":        "rpc_end",
			"rpc_id":       r.id,
			"method":       r.method,
			"code":         status.Code(v.Error).String(),
			"grpc_status":  int(status.Code(v.Error)),
			"elapsed_ms":   time.Since(r.started).Milliseconds(),
			"in_messages":  r.in.Load(),
			"out_messages": r.out.Load(),
		})
	}
}
func (p *serverContext) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (p *serverContext) HandleConn(_ context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnBegin:
		p.event(map[string]any{
			"event": "connection_open",
		})

	case *stats.ConnEnd:
		p.event(map[string]any{
			"event": "connection_close",
		})
	}
}
func (p *serverContext) unknown(_ any, stream grpc.ServerStream) error {
	method, _ := grpc.MethodFromServerStream(stream)
	r, _ := stream.Context().Value(rpcKey{}).(*rpcState)

	var id uint64

	if r != nil {
		id = r.id
	}

	if method == findApplicationConfigMethod {
		// Empty retains the unknown protobuf fields without inventing a schema.
		// Reading triggers the existing InPayload capture. Keep the original
		// Unimplemented result until the response contract is understood.
		if _, err := callerIdentity(stream.Context(), p.identityKey); err == nil {
			request := &emptypb.Empty{}

			if err := stream.RecvMsg(request); err != nil {
				p.event(map[string]any{
					"event":  "appconfig_capture_read_failed",
					"rpc_id": id,
					"code":   status.Code(err).String(),
				})
			}
		}
	}

	p.event(map[string]any{
		"event":       "unimplemented",
		"rpc_id":      id,
		"method":      safeMethod(method),
		"grpc_status": 12,
	})

	return status.Error(codes.Unimplemented, "RPC not implemented by Jamboree diagnostic server")
}
func newServer(p *serverContext, extra ...grpc.ServerOption) *grpc.Server {
	if p.capture == nil {
		p.capture = &rpcCapture{
			dir: "/data/npln-captures",
		}
	}
	// Jamboree's native Gamesync channel sends HTTP/2 pings every 5000 ms.
	// The grpc-go default (five minutes) closes it with too_many_pings.
	opts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime: 5 * time.Second,
		}),
		grpc.StatsHandler(p),
		grpc.UnknownServiceHandler(p.unknown),
		grpc.MaxRecvMsgSize(4 << 20),
		grpc.MaxSendMsgSize(4 << 20),
		grpc.MaxConcurrentStreams(64),
	}
	opts = append(opts, grpc.ForceServerCodec(&prearrangedDiagnosticCodec{
		p: p,
	}))
	opts = append(opts, extra...)
	s := grpc.NewServer(opts...)

	authpb.RegisterAuthServer(s, &authServer{
		serverContext: p,
		key:           p.identityKey,
	})

	authpb.RegisterUserServiceServer(s, &userServiceServer{
		p: p,
	})

	ugc, err := newUgcServer(p, os.Getenv("UGC_STORE_FILE"))

	if err != nil {
		panic("cannot load diagnostic UGC store")
	}

	ugcpb.RegisterUgcstoreServer(s, ugc)

	holderPath := os.Getenv("LEADERBOARD_STORE_FILE")

	if holderPath == "" && os.Getenv("UGC_STORE_FILE") != "" {
		holderPath = os.Getenv("UGC_STORE_FILE") + ".holders"
	}

	rankings, err := newLeaderboardServer(p, holderPath)

	if err != nil {
		panic("cannot load leaderboard holder store")
	}

	lbpb.RegisterLeaderboardServiceServer(s, rankings)

	rooms := &gameSessionServer{
		p:       p,
		tickets: map[string]*creationRecord{},
	}

	frpb.RegisterFriendsServer(s, &friendsServer{
		p:     p,
		rooms: rooms,
	})

	mmpb.RegisterGameSessionServiceServer(s, rooms)

	mmpb.RegisterMatchmakerServer(s, &matchmakerServer{
		rooms:   rooms,
		tickets: map[string]*searchRecord{},
	})

	gspb.RegisterGamesyncServer(s, &gamesyncServer{
		p:         p,
		rooms:     rooms,
		documents: map[string]map[string]*gspb.Document{},
	})

	return s
}
func main() {
	version := flag.Bool("version", false, "print build ID")
	stunListen := flag.String("stun-listen", "", "serve basic UDP STUN Binding only")
	turnListen := flag.String("turn-listen", "", "serve authenticated UDP TURN only")
	latencyListen := flag.String("latency-listen", "", "serve HTTP latency probes only")
	gamesyncRouterListen := flag.String("gamesync-router-listen", "", "route Gamesync by tenant over HTTP/2")

	flag.Parse()

	if *version {
		println(serverVersion)

		return
	}

	if *gamesyncRouterListen != "" {
		if err := serveGamesyncRouter(*gamesyncRouterListen, &serverContext{
			writer: os.Stdout,
		}); err != nil {
			log.Fatal(err)
		}

		return
	}

	if *latencyListen != "" {
		if err := serveLatency(*latencyListen, &serverContext{
			writer: os.Stdout,
		}); err != nil {
			log.Fatal(err)
		}

		return
	}

	if *stunListen != "" {
		if err := serveSTUN(*stunListen, &serverContext{
			writer: os.Stdout,
		}); err != nil {
			log.Fatal(err)
		}

		return
	}

	if *turnListen != "" {
		key, err := os.ReadFile(os.Getenv("AUTH_ID_KEY_FILE"))

		if err != nil || len(key) != 32 {
			log.Fatal("TURN diagnostic identity key unavailable")
		}

		if err = serveJamboreeTURN(context.Background(), &serverContext{
			writer:      os.Stdout,
			identityKey: key,
		}, *turnListen); err != nil {
			log.Fatal(err)
		}

		return
	}

	addr := os.Getenv("NPLN_LISTEN")

	if addr == "" {
		addr = "127.0.0.1:7443"
	}

	cert, key := os.Getenv("CERT_FILE"), os.Getenv("KEY_FILE")

	if cert == "" || key == "" {
		log.Fatal("CERT_FILE and KEY_FILE are required; diagnostic listener always uses TLS")
	}

	pair, err := tls.LoadX509KeyPair(cert, key)

	if err != nil {
		log.Fatal("cannot load TLS certificate/key")
	}

	identityKey, err := os.ReadFile(os.Getenv("AUTH_ID_KEY_FILE"))

	if err != nil || len(identityKey) != 32 {
		log.Fatal("AUTH_ID_KEY_FILE must contain a private 32-byte diagnostic key")
	}

	p := &serverContext{
		writer:      os.Stdout,
		identityKey: identityKey,
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		Certificates: []tls.Certificate{
			pair,
		},
		NextProtos: []string{"h2"},
	}
	s := newServer(p, grpc.Creds(credentials.NewTLS(cfg)))
	lis, err := net.Listen("tcp", addr)

	if err != nil {
		log.Fatal(err)
	}

	p.event(map[string]any{
		"event":          "server_start",
		"listen":         addr,
		"tenant":         "t-adf89f68",
		"mode":           "diagnostic-only",
		"unknown_status": 12,
	})

	if err = s.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
