package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func (s *gamesyncServer) authorizedRoom(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")

	if len(v) != 1 {
		return "", status.Error(codes.Unauthenticated, "session token required")
	}

	raw := strings.TrimSpace(v[0])

	if len(raw) >= 7 && strings.EqualFold(raw[:7], "Bearer ") {
		raw = strings.TrimSpace(raw[7:])
	}

	proof, e := verifySessionProof(raw, s.p.identityKey, "gamesync-access")

	if e != nil {
		return "", e
	}

	session := strings.Split(proof, "|")[1]
	cut := strings.LastIndex(session, "/userSessions/")

	if cut < 0 {
		return "", status.Error(codes.Unauthenticated, "invalid session")
	}

	return session[:cut], nil
}
func (s *gamesyncServer) ListDocuments(ctx context.Context, req *gs.ListDocumentsRequest) (*gs.ListDocumentsResponse, error) {
	room, e := s.authorizedRoom(ctx)

	if e != nil {
		return nil, e
	}

	parent := req.GetParent()

	if !validGSName(parent+"/placeholder") || req.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "collection resource required")
	}

	size := int(req.GetPageSize())

	if size == 0 {
		size = 100
	}

	if size > 256 {
		size = 256
	}

	signature := func(last string) []byte {
		m := hmac.New(sha256.New, s.p.identityKey)

		m.Write([]byte("gamesync-list\x00" + room + "\x00" + parent + "\x00" + last))

		return m.Sum(nil)
	}
	after := ""

	if t := req.GetPageToken(); t != "" {
		if len(t) > 1024 {
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}

		parts := strings.Split(t, ".")

		if len(parts) != 2 {
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}

		last, e1 := base64.RawURLEncoding.DecodeString(parts[0])
		sig, e2 := base64.RawURLEncoding.DecodeString(parts[1])

		if e1 != nil || e2 != nil || !hmac.Equal(sig, signature(string(last))) {
			return nil, status.Error(codes.InvalidArgument, "page token belongs to another query")
		}

		after = string(last)
	}

	prefix := parent + "/"
	docs := map[string]*gs.Document{}

	s.mu.Lock()

	for name, d := range s.documents[room] {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		suffix := strings.TrimPrefix(name, prefix)

		if !strings.Contains(suffix, "/") {
			docs[name] = proto.Clone(d).(*gs.Document)
		} else if req.GetShowMissing() {
			direct := prefix + strings.Split(suffix, "/")[0]

			if docs[direct] == nil {
				docs[direct] = &gs.Document{
					Name: direct,
				}
			}
		}
	}

	s.mu.Unlock()

	names := []string{}

	for name := range docs {
		if name > after {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	out := &gs.ListDocumentsResponse{}

	if len(names) > size {
		last := names[size-1]
		out.NextPageToken = base64.RawURLEncoding.EncodeToString([]byte(last)) + "." + base64.RawURLEncoding.EncodeToString(signature(last))
		names = names[:size]
	}

	for _, name := range names {
		d := docs[name]

		if mask := req.GetReadMask(); mask != nil && !(len(mask.Paths) == 1 && mask.Paths[0] == "*") {
			fields := cloneFields(nil)

			for _, p := range mask.Paths {
				path, e := fieldPath(p)

				if e != nil {
					return nil, e
				}

				setField(fields, path, readField(d.Fields, path))
			}

			d.Fields = fields
		}

		s.logDocument(d, "list")

		out.Documents = append(out.Documents, d)
	}

	s.p.event(map[string]any{
		"event":         "gamesync_documents_listed",
		"documents":     len(out.Documents),
		"has_next_page": out.NextPageToken != "",
	})

	return out, nil
}
