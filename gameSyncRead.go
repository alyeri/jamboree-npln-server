package main

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func (s *gamesyncServer) GetDocument(ctx context.Context, req *gs.GetDocumentRequest) (*gs.Document, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")

	if len(v) != 1 {
		return nil, status.Error(codes.Unauthenticated, "session token required")
	}

	raw := strings.TrimSpace(v[0])

	if len(raw) >= 7 && strings.EqualFold(raw[:7], "Bearer ") {
		raw = strings.TrimSpace(raw[7:])
	}

	proof, e := verifySessionProof(raw, s.p.identityKey, "gamesync-access")

	if e != nil {
		return nil, e
	}

	session := strings.Split(proof, "|")[1]
	cut := strings.LastIndex(session, "/userSessions/")

	if cut < 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid session")
	}

	if !validGSName(req.GetName()) {
		return nil, status.Error(codes.InvalidArgument, "room document required")
	}

	if len(req.GetTransaction()) != 0 {
		return nil, status.Error(codes.Unimplemented, "transactional reads not implemented")
	}

	s.mu.Lock()

	d := s.documents[session[:cut]][req.GetName()]

	if d != nil {
		d = proto.Clone(d).(*gs.Document)
	}

	s.mu.Unlock()

	if d == nil {
		return nil, status.Error(codes.NotFound, "room document not found")
	}

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

	s.p.event(map[string]any{
		"event":           "gamesync_document_read",
		"read_mask_paths": len(req.GetReadMask().GetPaths()),
	})

	s.logDocument(d, "get")

	return d, nil
}
