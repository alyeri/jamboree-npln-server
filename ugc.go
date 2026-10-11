package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	ugcpb "github.com/alyeri/jamboree-npln-server/proto/ugcstore/v1"
)

type ugcServer struct {
	ugcpb.UnimplementedUgcstoreServer
	p    *serverContext
	mu   sync.Mutex
	docs map[string]*ugcpb.Document
	path string
}

func verifyAccess(ctx context.Context, key []byte) error {
	if tenant(ctx) != "t-adf89f68" && tenant(ctx) != "t-adf89f68-lp1" {
		return status.Error(codes.PermissionDenied, "Jamboree tenant required")
	}

	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")

	if len(v) != 1 {
		return status.Error(codes.Unauthenticated, "access token required")
	}

	rawToken := strings.TrimSpace(v[0])

	if len(rawToken) >= 7 && strings.EqualFold(rawToken[:7], "Bearer ") {
		rawToken = strings.TrimSpace(rawToken[7:])
	}

	parts := strings.Split(rawToken, ".")

	if len(parts) != 3 || len(v[0]) > 8192 || len(key) != 32 {
		return status.Error(codes.Unauthenticated, "invalid token shape")
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])

	if err != nil {
		return status.Error(codes.Unauthenticated, "invalid token signature encoding")
	}

	mac := hmac.New(sha256.New, key)

	mac.Write([]byte(parts[0] + "." + parts[1]))

	if !hmac.Equal(sig, mac.Sum(nil)) {
		return status.Error(codes.Unauthenticated, "invalid token signature")
	}

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])

	if err != nil {
		return status.Error(codes.Unauthenticated, "invalid token payload encoding")
	}

	var c struct {
		Iss, Aud, Sub, Kind, Tenant string
		Exp                         int64
	}

	if json.Unmarshal(raw, &c) != nil || c.Iss != "nextendo-jamboree-diagnostic" || c.Aud != "jamboree-diagnostic-only" || c.Kind != "access" || c.Tenant != "t-adf89f68-lp1" || c.Sub == "" || c.Exp <= time.Now().Unix() {
		return status.Error(codes.Unauthenticated, "invalid token claims")
	}

	return nil
}
func (s *ugcServer) GetDocument(ctx context.Context, req *ugcpb.GetDocumentRequest) (*ugcpb.Document, error) {
	if err := verifyAccess(ctx, s.p.identityKey); err != nil {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		lengths := make([]int, len(values))

		for i, v := range values {
			lengths[i] = len(v)
		}

		s.p.event(map[string]any{
			"event":                 "ugc_auth_rejected",
			"reason":                status.Convert(err).Message(),
			"authorization_count":   len(values),
			"authorization_lengths": lengths,
		})

		return nil, err
	}

	name := req.GetName()
	valid := false

	for _, prefix := range []string{"tenants/current/documents/", "tenants/t-adf89f68-lp1/documents/", "tenants/t-adf89f68/documents/"} {
		if strings.HasPrefix(name, prefix) && len(name) > len(prefix) {
			valid = true
		}
	}

	if !valid || len(name) > 1024 || strings.Contains(name, "..") {
		return nil, status.Error(codes.InvalidArgument, "document resource required")
	}

	digest := sha256.Sum256([]byte(name))

	var id uint64

	if r, _ := ctx.Value(rpcKey{}).(*rpcState); r != nil {
		id = r.id
	}

	s.p.event(map[string]any{
		"event":                 "ugc_get_document",
		"rpc_id":                id,
		"resource_digest":       hex.EncodeToString(digest[:8]),
		"resource_segments":     len(strings.Split(name, "/")),
		"read_mask_paths":       len(req.GetReadMask().GetPaths()),
		"rule_context_present":  req.GetRuleContext() != nil,
		"request_unknown_bytes": len(req.ProtoReflect().GetUnknown()),
		"result":                "not_found",
	})

	s.mu.Lock()
	defer s.mu.Unlock()

	if d := s.docs[name]; d != nil {
		return proto.Clone(d).(*ugcpb.Document), nil
	}

	return nil, status.Error(codes.NotFound, "document does not exist in diagnostic store")
}
