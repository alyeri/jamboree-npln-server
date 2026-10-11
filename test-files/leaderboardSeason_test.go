package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

func TestSeasonAliasResponseAndLegacyScores(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	path := filepath.Join(t.TempDir(), "holders.json")
	old := "tenants/t-adf89f68-lp1/categories/Mode_00000002/seasons/current/scoreHolders/test"
	data, _ := proto.Marshal(&lb.Score{
		Name:       old,
		Holder:     "tenants/t-adf89f68-lp1/holders/test",
		Score:      10,
		UpdateTime: timestamppb.Now(),
	})
	disk, _ := json.Marshal(map[string][]byte{
		old: data,
	})

	if err := os.WriteFile(path+".scores", disk, 0600); err != nil {
		t.Fatal(err)
	}

	s, err := newLeaderboardServer(p, path)

	if err != nil {
		t.Fatal(err)
	}

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/a", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))

	for _, suffix := range []string{"current", "1"} {
		r, err := s.BrowseLeaderboard(ctx, &lb.BrowseLeaderboardRequest{
			Season: "tenants/current/categories/Mode_00000002/seasons/" + suffix,
			Filter: &lb.BrowseLeaderboardRequest_RangeLeaderboard{
				RangeLeaderboard: &lb.RangeLeaderboardFilter{
					PageSize: 30,
				},
			},
		})

		if err != nil || len(r.GetLeaderboard().GetScores()) != 1 || strings.HasSuffix(r.Leaderboard.Season, "/current") || r.Leaderboard.Scores[0].Name != strings.Replace(old, "/current/", "/1/", 1) {
			t.Fatal("alias or old score lost", err)
		}
	}

	name, season, err := rankingScoreName("tenants/current/categories/Mode_00000002/seasons/current/scoreHolders/default", "tenants/current/users/a")

	if err != nil || !strings.Contains(name, "/seasons/1/") || !strings.HasSuffix(season, "/seasons/1") {
		t.Fatal("score resource unresolved", err)
	}
}
