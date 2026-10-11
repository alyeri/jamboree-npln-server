package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

func TestRankingFixtureIsolationAndExpiry(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	dir := t.TempDir()
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
		capture: &rpcCapture{
			dir: dir,
		},
	}
	s, err := newLeaderboardServer(p, filepath.Join(dir, "store"))

	if err != nil {
		t.Fatal(err)
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
		if _, err := s.UpdateHolder(ctx, &lb.UpdateHolderRequest{
			Holder: &lb.Holder{
				Name: "tenants/current/holders/default",
			},
		}); err != nil {
			t.Fatal(err)
		}
	}

	season, _ := rankingSeason("tenants/current/categories/Mode_00000002/seasons/current")
	owner, _ := callerIdentity(a, key)
	f := rankingFixture{
		ExpiresAt:    time.Now().Add(time.Minute),
		CallerDigest: identityDigest(canonicalUserSession(owner)),
		Season:       season,
		Score:        1,
	}
	write := func() {
		raw, _ := json.Marshal(f)

		if err := os.WriteFile(filepath.Join(dir, "ranking-fixture.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}

	write()

	browse := func(ctx context.Context, season string, near bool) *lb.Leaderboard {
		r := &lb.BrowseLeaderboardRequest{
			Season: season,
			Filter: &lb.BrowseLeaderboardRequest_RangeLeaderboard{
				RangeLeaderboard: &lb.RangeLeaderboardFilter{
					PageSize: 10,
				},
			},
		}

		if near {
			r.Filter = &lb.BrowseLeaderboardRequest_NearLeaderboard{
				NearLeaderboard: &lb.NearLeaderboardFilter{
					PageSize:     10,
					TargetHolder: "tenants/current/holders/default",
				},
			}
		}

		out, err := s.BrowseLeaderboard(ctx, r)

		if err != nil {
			t.Fatal(err)
		}

		return out.Leaderboard
	}

	for _, near := range []bool{
		false,
		true,
	} {
		out := browse(a, season, near)

		if len(out.Scores) != 1 || out.Scores[0].Rank.Rank != 1 || out.Scores[0].HolderDetail == nil {
			t.Fatal("fixture absent")
		}
	}

	if len(browse(b, season, false).Scores) != 0 || len(browse(a, "tenants/current/categories/Mode_00000001/seasons/current", false).Scores) != 0 {
		t.Fatal("fixture leaked")
	}

	if len(s.scores) != 0 {
		t.Fatal("fixture persisted in memory")
	}

	if _, err := os.Stat(s.path + ".scores"); !os.IsNotExist(err) {
		t.Fatal("fixture persisted to disk")
	}

	f.ExpiresAt = time.Now().Add(-time.Second)

	write()

	if len(browse(a, season, false).Scores) != 0 {
		t.Fatal("expired fixture active")
	}
}
