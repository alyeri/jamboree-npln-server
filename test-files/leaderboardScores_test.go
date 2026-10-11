package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

func TestRankingScoreUploadBrowseAndPersistence(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	path := filepath.Join(t.TempDir(), "holders.json")
	s, e := newLeaderboardServer(p, path)

	if e != nil {
		t.Fatal(e)
	}

	ctxFor := func(user string) context.Context {
		token, _ := (&authServer{
			key: key,
		}).signSessionToken("tenants/current/users/"+user, "access", time.Minute)

		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	}
	a, b := ctxFor("a"), ctxFor("b")

	for _, ctx := range []context.Context{
		a,
		b,
	} {
		if _, e = s.UpdateHolder(ctx, &lb.UpdateHolderRequest{
			Holder: &lb.Holder{
				Name: "tenants/current/holders/default",
			},
		}); e != nil {
			t.Fatal(e)
		}
	}

	season := "tenants/current/categories/Minigame_00000000/seasons/current"
	req := &lb.RegisterScoreRequest{
		Score: &lb.Score{
			Name:   season + "/scoreHolders/default",
			Holder: "tenants/current/holders/default",
			Score:  999999991000,
		},
	}
	score, e := s.RegisterScore(a, req)

	if e != nil {
		t.Fatal(e)
	}

	if score.HolderDetail == nil || score.UpdateTime == nil {
		t.Fatal("missing score details")
	}

	req.Score.Score--

	low, e := s.RegisterScore(a, req)

	if e != nil || low.Score != score.Score {
		t.Fatal("best score lost", e)
	}

	req.Score.Score = score.Score
	other, e := s.RegisterScore(b, req)

	if e != nil || other.Name == score.Name {
		t.Fatal("score shared", e)
	}

	req.Score.Name = score.Name
	req.Score.Holder = score.Holder
	_, e = s.RegisterScore(b, req)

	if status.Code(e) != codes.PermissionDenied {
		t.Fatal("cross-account score accepted", e)
	}

	req.Score.Name = season + "/scoreHolders/default"
	req.Score.Holder = "tenants/current/holders/default"
	req.Score.Score = 1
	req.Force = true

	if _, e = s.RegisterScore(a, req); e != nil {
		t.Fatal(e)
	}

	restored, e := newLeaderboardServer(p, path)

	if e != nil {
		t.Fatal(e)
	}

	board, e := restored.BrowseLeaderboard(a, &lb.BrowseLeaderboardRequest{
		Season: season,
		Filter: &lb.BrowseLeaderboardRequest_RangeLeaderboard{
			RangeLeaderboard: &lb.RangeLeaderboardFilter{
				PageSize: 10,
			},
		},
	})

	if e != nil || board.GetLeaderboard().GetTotalCount() != 2 || board.Leaderboard.Scores[0].Holder != other.Holder || board.Leaderboard.Scores[1].Score != 1 {
		t.Fatal("persisted ordering wrong", e)
	}

	restored.path = filepath.Join(t.TempDir(), "missing", "holders")
	req.Score.Score = 10

	if _, e = restored.RegisterScore(a, req); status.Code(e) != codes.Internal {
		t.Fatal(e)
	}

	got, e := restored.GetScore(a, &lb.GetScoreRequest{
		Name: score.Name,
	})

	if e != nil || got.Score != 1 {
		t.Fatal("failed write committed", e)
	}
}

func TestRankingExternalIdsKnownUsersOnly(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		accountIDs: map[string]string{
			"tenants/t-adf89f68-lp1/users/a": "0000000000000001",
		},
	}
	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/a", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	s := &userServiceServer{
		p: p,
	}
	r, e := s.QueryUserExternalIds(ctx, &authpb.QueryUserExternalIdsRequest{
		Tenant: "tenants/current",
		Condition: &authpb.QueryUserExternalIdsRequest_ExternalIdCondition_{
			ExternalIdCondition: &authpb.QueryUserExternalIdsRequest_ExternalIdCondition{
				Users: []string{"tenants/current/users/a", "tenants/current/users/unknown"},
			},
		},
	})

	if e != nil || len(r.GetUserExternalIds()) != 1 || r.UserExternalIds[0].ExternalId.Id != "0000000000000001" {
		t.Fatal("wrong identity mapping", e)
	}
}
