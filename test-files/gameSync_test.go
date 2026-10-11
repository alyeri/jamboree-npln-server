package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func TestGamesyncProofBindingWithoutTenantMetadata(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{7}, 32)
	s := &gamesyncServer{
		p: &serverContext{
			writer:      &logs,
			identityKey: key,
		},
	}
	signer := &authServer{
		key: key,
	}
	session := "tenants/t-adf89f68-lp1/gameSessions/test/userSessions/host"
	proof, _ := signer.signSessionToken("tenants/current/users/owner|"+session, "matchmaking", time.Minute)
	req := &gspb.IssueTokenRequest{
		UserSession:        "tenants/current/gameSessions/test/userSessions/host",
		MatchmakingIdToken: proof,
	}
	r, err := s.IssueToken(context.Background(), req)

	if err != nil || r.GetToken().GetUserSession() != req.UserSession || r.GetToken().GetTtl().GetSeconds() != 900 {
		t.Fatal(err)
	}

	req.UserSession = session + "-other"
	req.UserSession = "userSessions/host"
	_, shortErr := s.IssueToken(context.Background(), req)

	if shortErr != nil {
		t.Fatal("bound short user session rejected", shortErr)
	}

	req.UserSession = "userSessions/other"
	_, err = s.IssueToken(context.Background(), req)

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("unbound session accepted", err)
	}

	req.UserSession = session
	req.MatchmakingIdToken = proof + "x"
	_, err = s.IssueToken(context.Background(), req)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}

	req.MatchmakingIdToken, _ = signer.signSessionToken("owner|"+session, "matchmaking", -time.Minute)
	_, err = s.IssueToken(context.Background(), req)

	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}

	if bytes.Contains(logs.Bytes(), []byte(proof)) {
		t.Fatal("proof leaked")
	}
}

func TestGamesyncRefreshTokenBinding(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	s := &gamesyncServer{
		p: &serverContext{
			writer:      &bytes.Buffer{},
			identityKey: key,
		},
	}
	signer := &authServer{
		key: key,
	}
	session := "tenants/t-adf89f68-lp1/gameSessions/test/userSessions/host"
	proof := "tenants/current/users/owner|" + session
	refresh, _ := signer.signSessionToken(proof, "gamesync-refresh", time.Hour)
	r, err := s.RefreshToken(context.Background(), &gspb.RefreshTokenRequest{
		UserSession:  "userSessions/current",
		RefreshToken: refresh,
	})

	if err != nil || r.GetToken().GetTtl().GetSeconds() != 900 {
		t.Fatal("refresh failed", err)
	}

	for kind, token := range map[string]string{
		"gamesync-access":  r.Token.AccessToken,
		"gamesync-refresh": r.Token.RefreshToken,
	} {
		got, e := verifySessionProof(token, key, kind)

		if e != nil || got != proof {
			t.Fatal("renewed identity invalid", e)
		}
	}

	_, err = s.RefreshToken(context.Background(), &gspb.RefreshTokenRequest{
		UserSession:  session + "-other",
		RefreshToken: refresh,
	})

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("foreign session admitted", err)
	}

	for _, kind := range []string{"gamesync-access", "gamesync-refresh"} {
		ttl := time.Hour

		if kind == "gamesync-refresh" {
			ttl = -time.Minute
		}

		invalid, _ := signer.signSessionToken(proof, kind, ttl)
		_, err = s.RefreshToken(context.Background(), &gspb.RefreshTokenRequest{
			UserSession:  session,
			RefreshToken: invalid,
		})

		if status.Code(err) != codes.Unauthenticated {
			t.Fatal("invalid refresh admitted", err)
		}
	}
}
