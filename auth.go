package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

// Identities and tokens are scoped to this server, not verified Nextendo accounts.
type suppressIdentityLog struct{}

type authServer struct {
	authpb.UnimplementedAuthServer
	state *serverContext
	key   []byte
}

func (s *authServer) CreateUser(ctx context.Context, req *authpb.CreateUserRequest) (*authpb.User, error) {
	if tenant(ctx) != "t-adf89f68-lp1" && tenant(ctx) != "t-adf89f68" {
		return nil, status.Error(codes.PermissionDenied, "Jamboree tenant required")
	}

	parent := req.GetParent()
	valid := false

	for _, prefix := range []string{
		"tenants/current",
		"tenants/t-adf89f68-lp1",
		"tenants/t-adf89f68",
	} {
		if parent == prefix {
			valid = true
		}
	}

	if !valid {
		return nil, status.Error(codes.InvalidArgument, "unsupported parent tenant")
	}

	if len(s.key) != 32 {
		return nil, status.Error(codes.FailedPrecondition, "diagnostic identity key unavailable")
	}

	ext := req.GetExternalIdToken()
	kind, identity := "", ""

	if raw := ext.GetNsaIdToken(); raw != "" {
		kind = "nsa_jwt_unverified"
		parts := strings.Split(raw, ".")

		if len(parts) != 3 || len(raw) > 65536 {
			return nil, status.Error(codes.Unauthenticated, "malformed external token")
		}

		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "malformed external token")
		}

		var claims struct {
			Sub  string `json:"sub"`
			Nnex string `json:"nnex"`
		}

		if json.Unmarshal(payload, &claims) != nil || claims.Sub == "" || len(claims.Sub) > 512 {
			return nil, status.Error(codes.Unauthenticated, "external token subject required")
		}

		identity = "nsa:" + claims.Sub

		if stable, ok := parseExternalIdentity(claims.Nnex); ok {
			identity = stable
			kind = "nextendo_claim_unverified"
		}
	} else if dummy := ext.GetDummyExtIdToken(); dummy != "" && len(dummy) <= 512 {
		kind = "dummy_diagnostic"
		identity = "dummy:" + dummy
	} else {
		return nil, status.Error(codes.Unauthenticated, "external identity token required")
	}

	// Keep the existing hash namespace so persisted user identities stay stable.
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte("jamboree-diagnostic\x00" + identity))
	digest := mac.Sum(nil)
	id := hex.EncodeToString(digest[:16])
	shortID := int64(binary.BigEndian.Uint64(digest[16:24]) & 0x7fffffffffffffff)

	if shortID == 0 {
		shortID = 1
	}

	r, _ := ctx.Value(rpcKey{}).(*rpcState)

	var rpcID uint64

	if r != nil {
		rpcID = r.id
	}

	if ctx.Value(suppressIdentityLog{}) == nil {
		s.state.event(map[string]any{
			"event":                 "create_user",
			"rpc_id":                rpcID,
			"token_kind":            kind,
			"identity_verified":     false,
			"parent":                parent,
			"request_unknown_bytes": len(req.ProtoReflect().GetUnknown()),
			"user_present":          req.GetUser() != nil,
		})
	}

	// The Friends protocol carries a 64-bit account identifier, not the
	// unrelated 128-bit NPLN user identifier. Keep this association internal.
	if s.state != nil {
		accountID := "000000000000cafe"

		if strings.HasPrefix(identity, "nextendo-diagnostic:") {
			pid, _ := strconv.ParseUint(strings.TrimPrefix(identity, "nextendo-diagnostic:"), 10, 64)
			accountID = fmt.Sprintf("%016x", pid)
		} else if kind == "dummy_diagnostic" {
			accountID = hex.EncodeToString(digest[16:24])
		}

		s.state.mu.Lock()

		if s.state.accountIDs == nil {
			s.state.accountIDs = map[string]string{}
		}

		if len(s.state.accountIDs) < 256 || s.state.accountIDs[canonicalUserSession(parent+"/users/"+id)] != "" {
			s.state.accountIDs[canonicalUserSession(parent+"/users/"+id)] = accountID
			s.state.accountIDs[canonicalUserSession(parent+"/users/u-"+id)] = accountID
			s.state.accountIDs[canonicalUserSession(parent+"/users/"+strconv.FormatInt(shortID, 10))] = accountID
		}

		s.state.mu.Unlock()
	}

	if s.state != nil {
		s.state.prearrangedTokens.remember(ext.GetNsaIdToken())
	}

	return &authpb.User{
		Name:    parent + "/users/" + id,
		Account: parent,
		ShortId: shortID,
	}, nil
}

func (s *authServer) signSessionToken(user, kind string, ttl time.Duration) (string, error) {
	nonce := make([]byte, 16)

	if _, err := rand.Read(nonce); err != nil {
		return "", status.Error(codes.Internal, "token entropy unavailable")
	}

	now := time.Now().Unix()
	claims := map[string]any{
		"iss":               "nextendo-jamboree-diagnostic",
		"aud":               "jamboree-diagnostic-only",
		"sub":               user,
		"tenant":            "t-adf89f68-lp1",
		"iat":               now,
		"exp":               now + int64(ttl/time.Second),
		"jti":               hex.EncodeToString(nonce),
		"kind":              kind,
		"identity_verified": false,
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", status.Error(codes.Internal, "token serialization failed")
	}

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	body := header + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(body))

	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (s *authServer) IssueToken(ctx context.Context, req *authpb.IssueTokenRequest) (*authpb.IssueTokenResponse, error) {
	// Some guest SDK paths carry the C-string terminator into protobuf.
	// Normalize only trailing NULs; identity comparisons remain exact.
	user := strings.TrimRight(req.GetUser(), "\x00")
	cut := strings.Index(user, "/users/")

	if cut < 0 || len(user) > 256 {
		return nil, status.Error(codes.InvalidArgument, "user resource required")
	}

	derived, err := s.CreateUser(context.WithValue(ctx, suppressIdentityLog{}, true), &authpb.CreateUserRequest{
		Parent:          user[:cut],
		ExternalIdToken: req.GetExternalIdToken(),
	})

	if err != nil {
		return nil, err
	}
	// Clients may reconstruct a cached user resource from User.ShortId.
	// Accept that alias only when it belongs to this same external identity.
	shortName := user[:cut] + "/users/" + strconv.FormatInt(derived.ShortId, 10)
	legacyName := user[:cut] + "/users/u-" + strings.TrimPrefix(derived.Name, user[:cut]+"/users/")

	if !hmac.Equal([]byte(user), []byte(derived.Name)) && !hmac.Equal([]byte(user), []byte(shortName)) && !hmac.Equal([]byte(user), []byte(legacyName)) {
		s.state.event(map[string]any{
			"event":           "issue_token_rejected",
			"reason":          "identity_mismatch",
			"numeric_user_id": isDecimalUserID(user[cut+7:]),
			"user_id_length":  len(user[cut+7:]),
		})

		if (len(user[cut+7:]) == 33 || (strings.HasPrefix(user[cut+7:], "u-") && len(user[cut+7:]) == 34)) && req.GetExternalIdToken().GetNsaIdToken() != "" {
			// The client caches users derived from the old, randomly generated BAAS
			// subject. This store has no such user for the current identity. NOT_FOUND
			// lets the SDK register again; never issue a token for an unrelated user.
			return nil, status.Error(codes.NotFound, "cached diagnostic user no longer exists; register user again")
		}

		return nil, status.Error(codes.PermissionDenied, "user does not match external identity")
	}

	ttl := 15 * time.Minute
	access, err := s.signSessionToken(user, "access", ttl)
	if err != nil {
		return nil, err
	}

	refresh, err := s.signSessionToken(user, "refresh", time.Hour)
	if err != nil {
		return nil, err
	}

	var rpcID uint64

	if r, _ := ctx.Value(rpcKey{}).(*rpcState); r != nil {
		rpcID = r.id
	}

	s.state.event(map[string]any{
		"event":                 "issue_token",
		"rpc_id":                rpcID,
		"identity_verified":     false,
		"ttl_seconds":           900,
		"request_unknown_bytes": len(req.ProtoReflect().GetUnknown()),
	})

	return &authpb.IssueTokenResponse{
		Token: &authpb.Token{
			User:         user,
			AccessToken:  access,
			RefreshToken: refresh,
			Ttl:          durationpb.New(ttl),
		},
	}, nil
}

func isDecimalUserID(id string) bool {
	if id == "" {
		return false
	}

	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}

	return true
}

func (s *authServer) IssuePrearrangedUserToken(
	ctx context.Context,
	req *authpb.IssuePrearrangedUserTokenRequest,
) (*authpb.IssuePrearrangedUserTokenResponse, error) {
	if req.GetUserIndex() < 0 || req.GetUserIndex() > 3 {
		return nil, status.Error(codes.InvalidArgument, "player index must be 0 through 3")
	}

	user, err := s.CreateUser(context.WithValue(ctx, suppressIdentityLog{}, true), &authpb.CreateUserRequest{
		Parent:          req.GetTenant(),
		ExternalIdToken: req.GetExternalIdToken(),
	})

	if err != nil {
		return nil, err
	}

	if req.GetUserIndex() != 0 {
		mac := hmac.New(sha256.New, s.key)
		mac.Write([]byte("prearranged\x00" + user.Name + "\x00" + strconv.Itoa(int(req.GetUserIndex()))))
		digest := mac.Sum(nil)
		user.Name = req.GetTenant() + "/users/" + hex.EncodeToString(digest[:16])
		user.ShortId = int64(binary.BigEndian.Uint64(digest[16:24]) & 0x7fffffffffffffff)

		if user.ShortId == 0 {
			user.ShortId = 1
		}
	}

	access, err := s.signSessionToken(user.Name, "access", 15*time.Minute)
	if err != nil {
		return nil, err
	}

	refresh, err := s.signSessionToken(user.Name, "refresh", time.Hour)
	if err != nil {
		return nil, err
	}

	s.state.event(map[string]any{
		"event":                 "issue_prearranged_token",
		"player_index":          req.GetUserIndex(),
		"identity_verified":     false,
		"request_unknown_bytes": len(req.ProtoReflect().GetUnknown()),
	})

	return &authpb.IssuePrearrangedUserTokenResponse{
		User: user,
		Token: &authpb.Token{
			User:         user.Name,
			AccessToken:  access,
			RefreshToken: refresh,
			Ttl:          durationpb.New(15 * time.Minute),
		},
	}, nil
}

// This private diagnostic server does not verify Nextendo's nx2 signature.
// Use its stable account claim only for isolated identity continuity, never
// as proof of ownership or authorization to a production account.
func parseExternalIdentity(token string) (string, bool) {
	parts := strings.Split(token, ".")

	if len(parts) != 3 || parts[0] != "nx2" || len(token) > 4096 {
		return "", false
	}

	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", false
	}

	fields := strings.SplitN(string(raw), ".", 3)

	if len(fields) != 3 || fields[1] == "" {
		return "", false
	}

	pid, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil || pid == 0 {
		return "", false
	}

	return "nextendo-diagnostic:" + strconv.FormatUint(pid, 10), true
}

func (s *authServer) RefreshToken(ctx context.Context, req *authpb.RefreshTokenRequest) (*authpb.RefreshTokenResponse, error) {
	if tenant(ctx) != "t-adf89f68-lp1" && tenant(ctx) != "t-adf89f68" {
		return nil, status.Error(codes.PermissionDenied, "Jamboree tenant required")
	}

	invalid := func() (*authpb.RefreshTokenResponse, error) {
		return nil, status.Error(codes.Unauthenticated, "invalid refresh token")
	}

	raw := req.GetRefreshToken()
	parts := strings.Split(raw, ".")

	if len(parts) != 3 || len(raw) > 8192 || len(s.key) != 32 {
		return invalid()
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return invalid()
	}

	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(parts[0] + "." + parts[1]))

	if !hmac.Equal(sig, mac.Sum(nil)) {
		return invalid()
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return invalid()
	}

	var c struct {
		Iss, Aud, Sub, Kind, Tenant string
		Exp                         int64
	}

	if json.Unmarshal(payload, &c) != nil ||
		c.Iss != "nextendo-jamboree-diagnostic" ||
		c.Aud != "jamboree-diagnostic-only" ||
		c.Kind != "refresh" ||
		c.Tenant != "t-adf89f68-lp1" ||
		c.Exp <= time.Now().Unix() ||
		c.Sub == "" {
		return invalid()
	}

	normalize := func(user string) string {
		user = strings.TrimRight(user, "\x00")

		for _, prefix := range []string{"tenants/current/", "tenants/t-adf89f68/"} {
			if strings.HasPrefix(user, prefix) {
				return "tenants/t-adf89f68-lp1/" + strings.TrimPrefix(user, prefix)
			}
		}

		return user
	}

	if normalize(req.GetUser()) != normalize(c.Sub) {
		return nil, status.Error(codes.PermissionDenied, "refresh token belongs to another user")
	}

	access, err := s.signSessionToken(c.Sub, "access", 15*time.Minute)
	if err != nil {
		return nil, err
	}

	refresh, err := s.signSessionToken(c.Sub, "refresh", time.Hour)
	if err != nil {
		return nil, err
	}

	s.state.event(map[string]any{
		"event":       "auth_refresh_token",
		"ttl_seconds": 900,
	})

	return &authpb.RefreshTokenResponse{
		Token: &authpb.Token{
			User:         c.Sub,
			AccessToken:  access,
			RefreshToken: refresh,
			Ttl:          durationpb.New(15 * time.Minute),
		},
	}, nil
}
