package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

func rankingSeason(name string) (string, error) {
	name = canonicalUserSession(name)
	p := strings.Split(name, "/")

	if len(p) != 6 || p[0] != "tenants" || p[1] != "t-adf89f68-lp1" || p[2] != "categories" || p[4] != "seasons" {
		return "", status.Error(codes.InvalidArgument, "invalid season")
	}

	if _, e := leaderboardResource("tenants/current/categories/"+p[3], "categories"); e != nil {
		return "", e
	}

	if _, e := leaderboardResource("tenants/current/seasons/"+p[5], "seasons"); e != nil {
		return "", e
	}
	// This diagnostic service has one active season. "current" is a client
	// alias; resource names returned by NPLN must identify the actual season.
	if p[5] == "current" {
		p[5] = "1"
	}

	return strings.Join(p, "/"), nil
}
func rankingScoreName(name, owner string) (string, string, error) {
	name = canonicalUserSession(name)
	p := strings.Split(name, "/")

	if len(p) != 8 || p[6] != "scoreHolders" {
		return "", "", status.Error(codes.InvalidArgument, "invalid score resource")
	}

	season, e := rankingSeason(strings.Join(p[:6], "/"))

	if e != nil {
		return "", "", e
	}

	p = append(strings.Split(season, "/"), p[6:]...)

	if p[7] == "default" {
		p[7] = strings.TrimPrefix(defaultHolder(owner), "tenants/t-adf89f68-lp1/holders/")
	}

	if _, e = leaderboardResource("tenants/current/holders/"+p[7], "holders"); e != nil {
		return "", "", e
	}

	return strings.Join(p, "/"), season, nil
}
func (s *leaderboardServer) persistScores(next map[string]*lb.Score) error {
	if s.path == "" {
		s.scores = next

		return nil
	}

	disk := map[string][]byte{}

	for n, v := range next {
		b, e := proto.Marshal(v)

		if e != nil {
			return status.Error(codes.Internal, "score serialization failed")
		}

		disk[n] = b
	}

	raw, e := json.Marshal(disk)

	if e != nil {
		return status.Error(codes.Internal, "score serialization failed")
	}

	f, e := os.CreateTemp(filepath.Dir(s.path), ".scores-*")

	if e != nil {
		return status.Error(codes.Internal, "score store unavailable")
	}

	name := f.Name()
	defer os.Remove(name)

	e = f.Chmod(0600)

	if e == nil {
		_, e = f.Write(raw)
	}

	if e == nil {
		e = f.Sync()
	}

	ce := f.Close()

	if e == nil {
		e = ce
	}

	if e == nil {
		e = os.Rename(name, s.path+".scores")
	}

	if e != nil {
		return status.Error(codes.Internal, "score persistence failed")
	}

	s.scores = next

	return nil
}
func (s *leaderboardServer) RegisterScore(ctx context.Context, r *lb.RegisterScoreRequest) (*lb.Score, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)

	if e != nil {
		return nil, e
	}

	owner = canonicalUserSession(owner)
	in := r.Score

	if in == nil || proto.Size(in) > 64*1024 || len(in.ResourceLink) > 1024 {
		return nil, status.Error(codes.InvalidArgument, "invalid score")
	}

	holder, _, e := holderName(in.Holder, owner)

	if e != nil {
		return nil, e
	}

	name, _, e := rankingScoreName(in.Name, owner)

	if e != nil {
		return nil, e
	}

	if strings.Split(name, "/")[7] != strings.Split(holder, "/")[3] {
		return nil, status.Error(codes.InvalidArgument, "score holder mismatch")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	h := s.holders[holder]

	if h == nil {
		return nil, status.Error(codes.NotFound, "holder not found")
	}

	if h.Owner != owner {
		return nil, status.Error(codes.PermissionDenied, "score belongs to another user")
	}

	old := s.scores[name]
	// Local diagnostic minigame boards keep the highest encoded score unless forced.
	// Jamboree sends inverted time values; no global Nintendo scores are invented.
	if old != nil && !r.Force && in.Score <= old.Score {
		return s.scoreDetail(old), nil
	}

	score := proto.Clone(in).(*lb.Score)
	score.Name = name
	score.Holder = holder
	score.HolderDetail = nil
	score.Rank = nil
	score.UpdateTime = timestamppb.Now()
	next := map[string]*lb.Score{}

	for n, v := range s.scores {
		next[n] = v
	}

	next[name] = score

	if e = s.persistScores(next); e != nil {
		return nil, e
	}

	s.p.event(map[string]any{
		"event":   "leaderboard_score_registered",
		"created": old == nil,
		"force":   r.Force,
	})

	return s.scoreDetail(score), nil
}
func (s *leaderboardServer) scoreDetail(in *lb.Score) *lb.Score {
	out := proto.Clone(in).(*lb.Score)

	if h := s.holders[in.Holder]; h != nil {
		out.HolderDetail = proto.Clone(h).(*lb.Holder)
	}

	return out
}
func (s *leaderboardServer) GetScore(ctx context.Context, r *lb.GetScoreRequest) (*lb.Score, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)

	if e != nil {
		return nil, e
	}

	name, _, e := rankingScoreName(r.Name, owner)

	if e != nil {
		return nil, e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	score := s.scores[name]

	if score == nil {
		return nil, status.Error(codes.NotFound, "score not found")
	}

	return s.scoreDetail(score), nil
}
func (s *leaderboardServer) BrowseLeaderboard(ctx context.Context, r *lb.BrowseLeaderboardRequest) (*lb.BrowseLeaderboardResponse, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)

	if e != nil {
		return nil, e
	}

	season, e := rankingSeason(r.Season)

	if e != nil {
		return nil, e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	all := []*lb.Score{}

	for n, v := range s.scores {
		if strings.HasPrefix(n, season+"/scoreHolders/") {
			all = append(all, s.scoreDetail(v))
		}
	}

	if len(all) == 0 {
		if score := s.fixtureScore(canonicalUserSession(owner), season); score != nil {
			all = append(all, score)

			s.p.event(map[string]any{
				"event": "leaderboard_synthetic_fixture",
				"count": 1,
			})
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Score != all[j].Score {
			return all[i].Score > all[j].Score
		}

		return all[i].Name < all[j].Name
	})

	rank := int32(0)

	for i, v := range all {
		if i == 0 || all[i-1].Score != v.Score {
			rank = int32(i + 1)
		}

		v.Rank = &lb.Rank{
			Rank:  rank,
			Index: int32(i),
		}
	}

	out := &lb.Leaderboard{
		Season:         season,
		TotalCount:     int32(len(all)),
		RankUpdateTime: timestamppb.Now(),
		UpdateType:     lb.Leaderboard_EXCEED,
		ShrinkState:    lb.ShrinkState_NOT_YET,
	}
	start, size := 0, 100
	allowed := map[string]bool{}

	switch f := r.Filter.(type) {
	case *lb.BrowseLeaderboardRequest_RangeLeaderboard:
		if f.RangeLeaderboard == nil {
			return nil, status.Error(codes.InvalidArgument, "filter required")
		}
		start = int(f.RangeLeaderboard.Offset)
		size = int(f.RangeLeaderboard.PageSize)
		if f.RangeLeaderboard.PageToken != "" || f.RangeLeaderboard.MoveTopHolder != "" {
			return nil, status.Error(codes.InvalidArgument, "unsupported range cursor")
		}

	case *lb.BrowseLeaderboardRequest_NearLeaderboard:
		if f.NearLeaderboard == nil || f.NearLeaderboard.PageToken != "" {
			return nil, status.Error(codes.InvalidArgument, "invalid near filter")
		}
		size = int(f.NearLeaderboard.PageSize)
		target, _, err := holderName(f.NearLeaderboard.TargetHolder, owner)
		if err != nil {
			return nil, err
		}
		found := false
		for i, v := range all {
			if v.Holder == target {
				start = i - size/2

				if start < 0 {
					start = 0
				}

				found = true

				break
			}
		}
		if !found {
			return &lb.BrowseLeaderboardResponse{
				Leaderboard: out,
			}, nil
		}

	case *lb.BrowseLeaderboardRequest_UserLeaderboard:
		if f.UserLeaderboard == nil || f.UserLeaderboard.OrderBy != "" {
			return nil, status.Error(codes.InvalidArgument, "unsupported user order")
		}
		for _, h := range f.UserLeaderboard.Holders {
			n, _, err := holderName(h, owner)

			if err != nil {
				return nil, err
			}

			allowed[n] = true
		}
		for _, v := range all {
			if allowed[v.Holder] {
				out.Scores = append(out.Scores, v)
			}
		}
		return &lb.BrowseLeaderboardResponse{
			Leaderboard: out,
		}, nil
	}

	if size == 0 {
		size = 100
	}

	if start < 0 || size < 0 || size > 100 {
		return nil, status.Error(codes.InvalidArgument, "invalid ranking range")
	}

	if start > len(all) {
		start = len(all)
	}

	end := start + size

	if end > len(all) {
		end = len(all)
	}

	out.Scores = all[start:end]

	return &lb.BrowseLeaderboardResponse{
		Leaderboard: out,
	}, nil
}
