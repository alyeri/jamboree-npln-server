package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	commonpb "github.com/alyeri/jamboree-npln-server/proto/common"
	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

type gamesyncServer struct {
	gspb.UnimplementedGamesyncServer
	p           *serverContext
	rooms       *gameSessionServer
	mu          sync.Mutex
	documents   map[string]map[string]*gspb.Document
	deferments  map[string]map[string]*gspb.Deferment
	publication sync.Mutex
	subs        map[string]map[*gsSubscriber]bool
	globals     map[string]map[string]*commonpb.Value
	epochs      map[string]uint64
}

func matchmakingProof(token string, key []byte) (string, error) {
	return verifySessionProof(token, key, "matchmaking")
}

func verifySessionProof(token string, key []byte, kind string) (string, error) {
	bad := func() (string, error) {
		return "", status.Error(codes.Unauthenticated, "invalid matchmaking proof")
	}
	parts := strings.Split(token, ".")

	if len(parts) != 3 || len(token) > 8192 || len(key) != 32 {
		return bad()
	}

	sig, e := base64.RawURLEncoding.DecodeString(parts[2])
	if e != nil {
		return bad()
	}

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(parts[0] + "." + parts[1]))

	if !hmac.Equal(sig, mac.Sum(nil)) {
		return bad()
	}

	payload, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return bad()
	}

	var c struct {
		Iss, Aud, Sub, Kind, Tenant string
		Exp                         int64
	}

	if json.Unmarshal(payload, &c) != nil ||
		c.Iss != "nextendo-jamboree-diagnostic" ||
		c.Aud != "jamboree-diagnostic-only" ||
		c.Kind != kind ||
		c.Tenant != "t-adf89f68-lp1" ||
		c.Exp <= time.Now().Unix() {
		return bad()
	}

	if len(strings.Split(c.Sub, "|")) != 2 {
		return bad()
	}

	return c.Sub, nil
}

func (s *gamesyncServer) KeepUserSession(stream grpc.BidiStreamingServer[gspb.KeepUserSessionRequest, gspb.KeepUserSessionResponse]) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	v := md.Get("authorization")

	if len(v) != 1 {
		return status.Error(codes.Unauthenticated, "session token required")
	}

	raw := strings.TrimSpace(v[0])

	if len(raw) >= 7 && strings.EqualFold(raw[:7], "Bearer ") {
		raw = strings.TrimSpace(raw[7:])
	}

	proof, err := verifySessionProof(raw, s.p.identityKey, "gamesync-access")
	if err != nil {
		return err
	}

	expected := strings.Split(proof, "|")[1]
	s.publication.Lock()

	if err := s.activateBoundRoom(proof); err != nil {
		s.publication.Unlock()

		return err
	}

	s.p.event(map[string]any{
		"event":            "gamesync_keep_open",
		"recipient_digest": identityDigest(proof),
		"caller_digest":    identityDigest(strings.Split(proof, "|")[0]),
	})
	sub := &gsSubscriber{
		stream:  stream,
		targets: map[string]*gspb.Target{},
		proof:   proof,
	}

	room := expected[:strings.LastIndex(expected, "/userSessions/")]
	s.mu.Lock()

	if s.epochs == nil {
		s.epochs = map[string]uint64{}
	}

	s.epochs[proof]++

	if s.subs == nil {
		s.subs = map[string]map[*gsSubscriber]bool{}
	}

	if s.subs[room] == nil {
		s.subs[room] = map[*gsSubscriber]bool{}
	}

	s.subs[room][sub] = true
	s.mu.Unlock()
	s.publication.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.subs[room], sub)
		s.mu.Unlock()
		s.scheduleSessionCleanup(proof, room)
	}()

	targets := sub.targets
	sendChange := func(id string, kind gspb.TargetChange_TargetChangeType) error {
		return sub.Send(&gspb.KeepUserSessionResponse{
			ResponseType: &gspb.KeepUserSessionResponse_TargetChange{
				TargetChange: &gspb.TargetChange{TargetId: id, TargetChangeType: kind},
			},
		})
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		name := strings.TrimRight(req.GetName(), "\x00")

		if name != "" && !sessionNameMatches(name, expected, strings.Split(proof, "|")[0]) {
			s.p.event(map[string]any{
				"event":         "gamesync_keep_name_mismatch",
				"name_segments": len(strings.Split(name, "/")),
				"name_length":   len(name),
			})

			return status.Error(codes.PermissionDenied, "session does not match token")
		}

		err = func() error {
			s.publication.Lock()
			defer s.publication.Unlock()

			switch x := req.RequestType.(type) {
			case *gspb.KeepUserSessionRequest_UpdateTarget:
				target := x.UpdateTarget.GetTarget()

				if target == nil || target.GetName() == "" || len(target.GetName()) > 512 {
					return status.Error(codes.InvalidArgument, "subscription target required")
				}

				idParts := strings.Split(target.GetName(), "/")
				id := idParts[len(idParts)-1]

				if id == "" {
					return status.Error(codes.InvalidArgument, "target identifier required")
				}

				if len(targets) >= 64 && targets[id] == nil {
					return status.Error(codes.ResourceExhausted, "subscription target limit reached")
				}

				targets[id] = target

				for _, wanted := range target.GetDocuments().GetDocuments() {
					s.p.event(map[string]any{
						"event": "gamesync_target_document",
						"path":  redactedDocumentPath(wanted),
					})
				}

				if collection := target.GetCollection(); collection != nil {
					s.p.event(map[string]any{
						"event": "gamesync_target_collection",
						"path":  redactedDocumentPath(collection.GetCollection()),
					})
				}

				if err = sendChange(id, gspb.TargetChange_UPDATED); err != nil {
					return err
				}

				snapshot := s.sessionSnapshot(expected, target)

				for _, doc := range snapshot {
					s.logDocument(doc, "snapshot")
					err = sub.Send(&gspb.KeepUserSessionResponse{
						ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{
							DocumentChange: &gspb.DocumentChange{
								TargetId:           id,
								DocumentChangeType: gspb.DocumentChange_EXIST,
								Document:           doc,
							},
						},
					})
					s.logDelivery(sub, doc, "EXIST", err)

					if err != nil {
						return err
					}
				}

				if err = sendChange(id, gspb.TargetChange_LISTED); err != nil {
					return err
				}

				s.p.event(map[string]any{
					"event":              "gamesync_target_registered",
					"document_count":     len(target.GetDocuments().GetDocuments()),
					"collection_target":  target.GetCollection() != nil,
					"snapshot_documents": len(snapshot),
				})

			case *gspb.KeepUserSessionRequest_DeleteTarget:
				idParts := strings.Split(x.DeleteTarget.GetName(), "/")
				id := idParts[len(idParts)-1]
				delete(targets, id)

				if err = sendChange(id, gspb.TargetChange_DELETED); err != nil {
					return err
				}

			case *gspb.KeepUserSessionRequest_Echo:
				if err = sub.Send(&gspb.KeepUserSessionResponse{
					ResponseType: &gspb.KeepUserSessionResponse_Echo{Echo: x.Echo},
				}); err != nil {
					return err
				}

				s.p.event(map[string]any{"event": "gamesync_keep_echo"})

			default:
				s.p.event(map[string]any{
					"event":         "gamesync_keep_request",
					"update_target": req.GetUpdateTarget() != nil,
					"delete_target": req.GetDeleteTarget() != nil,
					"unknown_bytes": len(req.ProtoReflect().GetUnknown()),
				})

				return status.Error(codes.Unimplemented, "session subscription operation not implemented")
			}

			return nil
		}()

		if err != nil {
			return err
		}
	}
}

func (s *gamesyncServer) RefreshToken(ctx context.Context, req *gspb.RefreshTokenRequest) (*gspb.RefreshTokenResponse, error) {
	proof, err := verifySessionProof(req.GetRefreshToken(), s.p.identityKey, "gamesync-refresh")
	if err != nil {
		return nil, err
	}

	parts := strings.Split(proof, "|")

	if !sessionNameMatches(req.GetUserSession(), parts[1], parts[0]) {
		return nil, status.Error(codes.PermissionDenied, "refresh token belongs to another session")
	}

	if err := s.activateBoundRoom(proof); err != nil {
		return nil, err
	}

	signer := &authServer{key: s.p.identityKey}
	access, err := signer.signSessionToken(proof, "gamesync-access", 15*time.Minute)
	if err != nil {
		return nil, err
	}

	refresh, err := signer.signSessionToken(proof, "gamesync-refresh", time.Hour)
	if err != nil {
		return nil, err
	}

	s.p.event(map[string]any{
		"event":       "gamesync_token_refreshed",
		"ttl_seconds": 900,
	})

	return &gspb.RefreshTokenResponse{
		Token: &gspb.Token{
			UserSession:  req.GetUserSession(),
			AccessToken:  access,
			RefreshToken: refresh,
			Ttl:          durationpb.New(15 * time.Minute),
		},
	}, nil
}

func canonicalUserSession(name string) string {
	name = strings.TrimRight(name, "\x00")

	for _, prefix := range []string{"tenants/current/", "tenants/t-adf89f68/"} {
		if strings.HasPrefix(name, prefix) {
			return "tenants/t-adf89f68-lp1/" + strings.TrimPrefix(name, prefix)
		}
	}

	if strings.HasPrefix(name, "gameSessions/") {
		return "tenants/t-adf89f68-lp1/" + name
	}

	return name
}

func (s *gamesyncServer) IssueToken(ctx context.Context, req *gspb.IssueTokenRequest) (*gspb.IssueTokenResponse, error) {
	proof, err := matchmakingProof(req.GetMatchmakingIdToken(), s.p.identityKey)
	if err != nil {
		return nil, err
	}

	parts := strings.Split(proof, "|")
	expected := parts[1]
	requested := strings.TrimRight(req.GetUserSession(), "\x00")
	expectedParts := strings.Split(expected, "/")

	if expected == "" || !sessionNameMatches(requested, expected, parts[0]) {
		s.p.event(map[string]any{
			"event":              "gamesync_session_mismatch",
			"requested_segments": len(strings.Split(requested, "/")),
			"requested_length":   len(requested),
			"expected_segments":  len(expectedParts),
		})

		return nil, status.Error(codes.PermissionDenied, "matchmaking proof belongs to another session")
	}

	signer := &authServer{key: s.p.identityKey}

	if err = s.initializeSessionDocuments(proof); err != nil {
		return nil, err
	}

	access, err := signer.signSessionToken(proof, "gamesync-access", 15*time.Minute)
	if err != nil {
		return nil, err
	}

	refresh, err := signer.signSessionToken(proof, "gamesync-refresh", time.Hour)
	if err != nil {
		return nil, err
	}

	s.p.event(map[string]any{
		"event":                 "gamesync_issue_token",
		"ttl_seconds":           900,
		"request_unknown_bytes": len(req.ProtoReflect().GetUnknown()),
	})

	return &gspb.IssueTokenResponse{
		Token: &gspb.Token{
			UserSession:  req.GetUserSession(),
			AccessToken:  access,
			RefreshToken: refresh,
			Ttl:          durationpb.New(15 * time.Minute),
		},
	}, nil
}

func sessionNameMatches(requested, expected, owner string) bool {
	requested = canonicalUserSession(requested)
	expected = canonicalUserSession(expected)

	if requested == expected {
		return true
	}

	marker := "/userSessions/"
	cut := strings.LastIndex(expected, marker)

	if cut < 0 {
		return false
	}

	ownerParts := strings.Split(owner, "/")
	ownerID := ownerParts[len(ownerParts)-1]

	for _, id := range []string{expected[cut+len(marker):], "current", ownerID} {
		for _, alias := range []string{
			"userSessions/" + id,
			"tenants/t-adf89f68-lp1/userSessions/" + id,
			expected[:cut] + marker + id,
			"tenants/t-adf89f68-lp1/gameSessions/current/userSessions/" + id,
		} {
			if requested == alias {
				return true
			}
		}
	}

	return false
}
