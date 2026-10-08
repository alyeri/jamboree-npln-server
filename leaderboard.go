package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

// The default holder is an authenticated caller alias, never a shared record.
type leaderboardServer struct {
	lb.UnimplementedLeaderboardServiceServer
	p       *serverContext
	mu      sync.Mutex
	path    string
	holders map[string]*lb.Holder
	scores  map[string]*lb.Score
}

func newLeaderboardServer(p *serverContext, path string) (*leaderboardServer, error) {
	s := &leaderboardServer{
		p:       p,
		path:    path,
		holders: map[string]*lb.Holder{},
		scores:  map[string]*lb.Score{},
	}

	if path != "" {
		data, err := os.ReadFile(path + ".scores")
		if err == nil {
			var disk map[string][]byte

			if err = json.Unmarshal(data, &disk); err != nil {
				return nil, err
			}

			for name, b := range disk {
				score := &lb.Score{}

				if err = proto.Unmarshal(b, score); err != nil {
					return nil, err
				}

				if score.Name != name {
					return nil, fmt.Errorf("invalid score store")
				}
				// Preserve scores saved by older builds under the current alias.
				parts := strings.Split(name, "/")

				if len(parts) != 8 {
					return nil, fmt.Errorf("invalid score resource in store")
				}

				season, err := rankingSeason(strings.Join(parts[:6], "/"))
				if err != nil {
					return nil, err
				}

				name = season + "/" + strings.Join(parts[6:], "/")
				score.Name = name
				previous := s.scores[name]

				if previous == nil || score.GetUpdateTime().AsTime().After(previous.GetUpdateTime().AsTime()) {
					s.scores[name] = score
				}
			}
		} else if !os.IsNotExist(err) {
			return nil, err
		}
	}

	if path == "" {
		return s, nil
	}

	raw, e := os.ReadFile(path)

	if os.IsNotExist(e) {
		return s, nil
	}

	if e != nil {
		return nil, e
	}

	var disk map[string][]byte

	if e = json.Unmarshal(raw, &disk); e != nil {
		return nil, e
	}

	for name, b := range disk {
		h := &lb.Holder{}

		if e = proto.Unmarshal(b, h); e != nil {
			return nil, e
		}

		if h.Name != name || h.Owner == "" {
			return nil, fmt.Errorf("invalid holder store")
		}

		s.holders[name] = h
	}

	return s, nil
}

func leaderboardResource(name, kind string) (string, error) {
	name = canonicalUserSession(name)
	prefix := "tenants/t-adf89f68-lp1/" + kind + "/"

	if !strings.HasPrefix(name, prefix) {
		return "", status.Error(codes.InvalidArgument, "invalid ranking resource")
	}

	id := strings.TrimPrefix(name, prefix)

	if len(id) == 0 || len(id) > 128 {
		return "", status.Error(codes.InvalidArgument, "invalid ranking identifier")
	}

	for _, r := range id {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
			return "", status.Error(codes.InvalidArgument, "invalid ranking identifier")
		}
	}

	return name, nil
}

func validRankingTenant(name string) bool {
	return name == "tenants/current" || name == "tenants/t-adf89f68" || name == "tenants/t-adf89f68-lp1"
}

func defaultHolder(owner string) string {
	sum := sha256.Sum256([]byte(canonicalUserSession(owner)))

	return "tenants/t-adf89f68-lp1/holders/h-" + hex.EncodeToString(sum[:16])
}

func holderName(name, owner string) (string, bool, error) {
	n, e := leaderboardResource(name, "holders")
	if e != nil {
		return "", false, e
	}

	if strings.HasSuffix(n, "/holders/default") {
		return defaultHolder(owner), true, nil
	}

	return n, false, nil
}

func (s *leaderboardServer) persist(next map[string]*lb.Holder) error {
	if s.path == "" {
		s.holders = next

		return nil
	}

	disk := map[string][]byte{}

	for name, h := range next {
		b, e := proto.Marshal(h)
		if e != nil {
			return status.Error(codes.Internal, "holder serialization failed")
		}

		disk[name] = b
	}

	raw, e := json.Marshal(disk)
	if e != nil {
		return status.Error(codes.Internal, "holder serialization failed")
	}

	f, e := os.CreateTemp(filepath.Dir(s.path), ".holders-*")
	if e != nil {
		return status.Error(codes.Internal, "holder store unavailable")
	}

	tmp := f.Name()
	defer os.Remove(tmp)

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
		e = os.Rename(tmp, s.path)
	}

	if e != nil {
		return status.Error(codes.Internal, "holder persistence failed")
	}

	s.holders = next

	return nil
}

func (s *leaderboardServer) commit(h *lb.Holder) error {
	next := make(map[string]*lb.Holder, len(s.holders)+1)

	for n, v := range s.holders {
		next[n] = v
	}

	next[h.Name] = h

	return s.persist(next)
}

func checkedHolderOwner(h *lb.Holder, owner string) error {
	if h.Owner != "" && canonicalUserSession(h.Owner) != owner {
		return status.Error(codes.PermissionDenied, "holder belongs to another user")
	}

	if len(h.DisplayName) > 512 || len(h.ResourceLink) > 1024 || proto.Size(h) > 64*1024 {
		return status.Error(codes.InvalidArgument, "holder exceeds limits")
	}

	return nil
}

func (s *leaderboardServer) CreateHolder(ctx context.Context, r *lb.CreateHolderRequest) (*lb.Holder, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)
	if e != nil {
		return nil, e
	}

	owner = canonicalUserSession(owner)

	if !validRankingTenant(r.Parent) || r.Holder == nil {
		return nil, status.Error(codes.InvalidArgument, "tenant and holder required")
	}

	if e = checkedHolderOwner(r.Holder, owner); e != nil {
		return nil, e
	}

	h := proto.Clone(r.Holder).(*lb.Holder)

	if h.Name == "" {
		h.Name = defaultHolder(owner)
	}

	name, _, e := holderName(h.Name, owner)
	if e != nil {
		return nil, e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.holders[name] != nil {
		return nil, status.Error(codes.AlreadyExists, "holder already exists")
	}

	h.Name = name
	h.Owner = owner
	h.CreateTime = timestamppb.Now()
	h.UpdateTime = h.CreateTime

	if e = s.commit(h); e != nil {
		return nil, e
	}

	return proto.Clone(h).(*lb.Holder), nil
}

func (s *leaderboardServer) UpdateHolder(ctx context.Context, r *lb.UpdateHolderRequest) (*lb.Holder, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)
	if e != nil {
		return nil, e
	}

	owner = canonicalUserSession(owner)

	if r.Holder == nil {
		return nil, status.Error(codes.InvalidArgument, "holder required")
	}

	in := r.Holder

	if e = checkedHolderOwner(in, owner); e != nil {
		return nil, e
	}

	name, isDefault, e := holderName(in.Name, owner)
	if e != nil {
		return nil, e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.holders[name]
	// Native bex uses tenants/current/holders/default before any CreateHolder.
	if old == nil && !isDefault {
		return nil, status.Error(codes.NotFound, "holder not found")
	}

	if old != nil && old.Owner != owner {
		return nil, status.Error(codes.PermissionDenied, "holder belongs to another user")
	}

	h := &lb.Holder{
		Name:       name,
		Owner:      owner,
		CreateTime: timestamppb.Now(),
	}

	if old != nil {
		h = proto.Clone(old).(*lb.Holder)
	}

	paths := r.GetUpdateMask().GetPaths()

	if len(paths) == 0 {
		paths = []string{
			"type",
			"display_name",
			"fields",
			"resource_link",
		}
	}

	for _, path := range paths {
		switch path {
		case "*":
			h.Type = in.Type
			h.DisplayName = in.DisplayName
			h.Fields = in.Fields
			h.ResourceLink = in.ResourceLink

		case "type":
			h.Type = in.Type

		case "display_name":
			h.DisplayName = in.DisplayName

		case "fields":
			h.Fields = in.Fields

		case "resource_link":
			h.ResourceLink = in.ResourceLink

		default:
			return nil, status.Error(codes.InvalidArgument, "unsupported holder update mask")
		}
	}

	h = proto.Clone(h).(*lb.Holder)
	h.UpdateTime = timestamppb.Now()

	if e = s.commit(h); e != nil {
		return nil, e
	}

	s.p.event(map[string]any{
		"event":         "leaderboard_holder_updated",
		"default_alias": isDefault,
		"created":       old == nil,
		"mask_paths":    paths,
	})

	return proto.Clone(h).(*lb.Holder), nil
}

func (s *leaderboardServer) GetHolder(ctx context.Context, r *lb.GetHolderRequest) (*lb.Holder, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)
	if e != nil {
		return nil, e
	}

	name, _, e := holderName(r.Name, canonicalUserSession(owner))
	if e != nil {
		return nil, e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	h := s.holders[name]

	if h == nil {
		return nil, status.Error(codes.NotFound, "holder not found")
	}

	return proto.Clone(h).(*lb.Holder), nil
}

func (s *leaderboardServer) SearchHolders(ctx context.Context, r *lb.SearchHoldersRequest) (*lb.SearchHoldersResponse, error) {
	if _, e := callerIdentity(ctx, s.p.identityKey); e != nil {
		return nil, e
	}

	if !validRankingTenant(r.Parent) || r.PageSize < 0 || r.PageSize > 100 || r.Filter != "" {
		return nil, status.Error(codes.InvalidArgument, "invalid holder search")
	}

	size := int(r.PageSize)

	if size == 0 {
		size = 100
	}

	owners := map[string]bool{}

	for _, n := range r.Owners {
		owners[canonicalUserSession(n)] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	names := []string{}

	for n, h := range s.holders {
		if n > r.PageToken && (len(owners) == 0 || owners[h.Owner]) {
			names = append(names, n)
		}
	}

	sort.Strings(names)
	out := &lb.SearchHoldersResponse{}

	for i, n := range names {
		if i == size {
			out.NextPageToken = names[i-1]
			break
		}

		out.Holders = append(out.Holders, proto.Clone(s.holders[n]).(*lb.Holder))
	}

	return out, nil
}

func (s *leaderboardServer) DeleteHolder(ctx context.Context, r *lb.DeleteHolderRequest) (*emptypb.Empty, error) {
	owner, e := callerIdentity(ctx, s.p.identityKey)
	if e != nil {
		return nil, e
	}

	owner = canonicalUserSession(owner)
	name, _, e := holderName(r.Name, owner)
	if e != nil {
		return nil, e
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.holders[name]

	if old == nil {
		return nil, status.Error(codes.NotFound, "holder not found")
	}

	if old.Owner != owner {
		return nil, status.Error(codes.PermissionDenied, "holder belongs to another user")
	}

	next := map[string]*lb.Holder{}

	for n, h := range s.holders {
		if n != name {
			next[n] = h
		}
	}

	if e = s.persist(next); e != nil {
		return nil, e
	}

	return &emptypb.Empty{}, nil
}
