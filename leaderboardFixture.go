package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

// Explicit, expiring experiment. Never writes holders or scores to persistence.
type rankingFixture struct {
	ExpiresAt    time.Time `json:"expires_at"`
	CallerDigest string    `json:"caller_digest"`
	Season       string    `json:"season"`
	Score        int64     `json:"score"`
}

// Called under s.mu; applies only to an empty board for the selected caller.
func (s *leaderboardServer) fixtureScore(owner, season string) *lb.Score {
	if s.p.capture == nil {
		return nil
	}

	raw, err := os.ReadFile(filepath.Join(s.p.capture.dir, "ranking-fixture.json"))

	if err != nil || len(raw) > 4096 {
		return nil
	}

	var f rankingFixture

	if json.Unmarshal(raw, &f) != nil || !f.ExpiresAt.After(time.Now()) || f.ExpiresAt.After(time.Now().Add(time.Hour)) || f.CallerDigest != identityDigest(owner) || f.Season != season {
		return nil
	}

	h := s.holders[defaultHolder(owner)]

	if h == nil || h.Owner != owner {
		return nil
	}

	return &lb.Score{
		Name:         season + "/scoreHolders/" + filepath.Base(h.Name),
		Holder:       h.Name,
		Score:        f.Score,
		HolderDetail: proto.Clone(h).(*lb.Holder),
		UpdateTime:   timestamppb.Now(),
	}
}
