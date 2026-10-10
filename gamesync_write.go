package main

import (
	"context"
	"math"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/alyeri/jamboree-npln-server/proto/common"
	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func cloneFields(fields *commonpb.MapValue) *commonpb.MapValue {
	if fields == nil {
		return &commonpb.MapValue{
			Fields: map[string]*commonpb.Value{},
		}
	}

	out := proto.Clone(fields).(*commonpb.MapValue)

	if out.Fields == nil {
		out.Fields = map[string]*commonpb.Value{}
	}

	return out
}

// Tag Match participants republish their lobby metadata after the match.
// This grants no access to admission flags, passwords, or configuration.
func tagLobbyPropertiesUpdate(record *creationRecord, proof string, old *gspb.Document, op *gspb.WriteOperation) bool {
	if matchmakingConfigID(record.ticket.GetMatchmakingConfig()) != "BqP4NoEasePaticipantConfigs" {
		return false
	}

	parts := strings.Split(proof, "|")

	if len(parts) != 2 {
		return false
	}

	participant := false

	for _, user := range record.ticket.GetGameSession().GetUserSessions() {
		participant = participant || (user.User == parts[0] && user.Name == parts[1])
	}

	if !participant {
		return false
	}

	update := op.GetUpdateDocument()

	if update == nil || len(update.GetUpdateMask().GetPaths()) != 1 {
		return false
	}

	path, err := fieldPath(update.UpdateMask.Paths[0])

	if err != nil || len(path) != 1 || path[0] != "prp" {
		return false
	}

	fields := update.GetDocument().GetFields().GetFields()

	if len(fields) != 1 {
		return false
	}

	properties := fields["prp"].GetMapValue()

	if properties == nil {
		return false
	}

	for key := range properties.GetFields() {
		switch key {
		case "_AliasSuffix", "_BaseConfigName", "_Bex_PropIsHost", "gameMode", "searchWord", "uniqueId":
		default:
			return false
		}
	}

	previous := old.GetFields().GetFields()["prp"].GetMapValue()

	for _, key := range []string{"_AliasSuffix", "_BaseConfigName", "gameMode"} {
		if !proto.Equal(previous.GetFields()[key], properties.GetFields()[key]) {
			return false
		}
	}

	return true
}

func fieldPath(path string) ([]string, error) {
	var parts []string

	var part strings.Builder

	quoted, escaped := false, false

	for _, c := range path {
		if escaped {
			part.WriteRune(c)
			escaped = false
			continue
		}

		if c == '\\' && quoted {
			escaped = true
			continue
		}

		if c == '`' {
			quoted = !quoted
			continue
		}

		if c == '.' && !quoted {
			if part.Len() == 0 {
				return nil, status.Error(codes.InvalidArgument, "empty field path")
			}

			parts = append(parts, part.String())
			part.Reset()
		} else {
			part.WriteRune(c)
		}
	}

	if quoted || escaped || part.Len() == 0 || path == "*" {
		return nil, status.Error(codes.InvalidArgument, "invalid field path")
	}

	return append(parts, part.String()), nil
}

func readField(fields *commonpb.MapValue, path []string) *commonpb.Value {
	for i, k := range path {
		v := fields.GetFields()[k]

		if i == len(path)-1 {
			return v
		}

		fields = v.GetMapValue()
	}

	return nil
}

func setField(fields *commonpb.MapValue, path []string, value *commonpb.Value) {
	for i, k := range path {
		if i == len(path)-1 {
			if value == nil {
				delete(fields.Fields, k)
			} else {
				fields.Fields[k] = proto.Clone(value).(*commonpb.Value)
			}

			return
		}

		next := fields.GetFields()[k].GetMapValue()

		if next == nil {
			next = cloneFields(nil)
			fields.Fields[k] = &commonpb.Value{
				ValueType: &commonpb.Value_MapValue{MapValue: next},
			}
		}

		if next.Fields == nil {
			next.Fields = map[string]*commonpb.Value{}
		}

		fields = next
	}
}

func applyFieldTransform(doc *gspb.Document, t *gspb.FieldTransform, globals map[string]*commonpb.Value, now *timestamppb.Timestamp) (*commonpb.Value, error) {
	path, err := fieldPath(t.FieldPath)
	if err != nil {
		return nil, err
	}

	current := readField(doc.Fields, path)
	integer := func(v *commonpb.Value) (int64, error) {
		if v == nil {
			return 0, nil
		}

		x, ok := v.GetValueType().(*commonpb.Value_IntegerValue)

		if !ok {
			return 0, status.Error(codes.InvalidArgument, "integer transform requires integer value")
		}

		return x.IntegerValue, nil
	}

	global := func(name string) error {
		if name != "__upcsidn" {
			return status.Error(codes.Unimplemented, "unknown transform global")
		}

		return nil
	}

	switch x := t.GetTransformType().(type) {
	case *gspb.FieldTransform_LoadGlobalValue:
		if err := global(x.LoadGlobalValue); err != nil {
			return nil, err
		}

		if v := globals[x.LoadGlobalValue]; v != nil {
			current = v
		} else if current == nil {
			current = intValue(0)
		}

	case *gspb.FieldTransform_StoreGlobalValue:
		if err := global(x.StoreGlobalValue); err != nil {
			return nil, err
		}

		if current == nil {
			return nil, status.Error(codes.FailedPrecondition, "cannot store a missing field")
		}

		globals[x.StoreGlobalValue] = proto.Clone(current).(*commonpb.Value)

	case *gspb.FieldTransform_ClearGlobalValue:
		if err := global(x.ClearGlobalValue); err != nil {
			return nil, err
		}

		delete(globals, x.ClearGlobalValue)

	case *gspb.FieldTransform_Increment:
		a, err := integer(current)
		if err != nil {
			return nil, err
		}

		b, err := integer(x.Increment)
		if err != nil {
			return nil, err
		}

		if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
			return nil, status.Error(codes.OutOfRange, "integer transform overflow")
		}

		current = intValue(a + b)

	case *gspb.FieldTransform_Maximum:
		a, err := integer(current)
		if err != nil {
			return nil, err
		}

		b, err := integer(x.Maximum)
		if err != nil {
			return nil, err
		}

		if b > a {
			a = b
		}

		current = intValue(a)

	case *gspb.FieldTransform_Minimum:
		a, err := integer(current)
		if err != nil {
			return nil, err
		}

		b, err := integer(x.Minimum)
		if err != nil {
			return nil, err
		}

		if b < a {
			a = b
		}

		current = intValue(a)

	case *gspb.FieldTransform_SetServerValue:
		if x.SetServerValue != gspb.FieldTransform_REQUEST_TIME {
			return nil, status.Error(codes.InvalidArgument, "unknown server transform")
		}

		current = &commonpb.Value{
			ValueType: &commonpb.Value_TimestampValue{TimestampValue: now},
		}

	default:
		return nil, status.Error(codes.Unimplemented, "unsupported field transform")
	}

	if current == nil {
		return nil, status.Error(codes.FailedPrecondition, "transform result missing")
	}

	setField(doc.Fields, path, current)

	return proto.Clone(current).(*commonpb.Value), nil
}

type gsSubscriber struct {
	proof   string
	mu      sync.Mutex
	stream  grpc.BidiStreamingServer[gspb.KeepUserSessionRequest, gspb.KeepUserSessionResponse]
	targets map[string]*gspb.Target
}

func (b *gsSubscriber) Send(r *gspb.KeepUserSessionResponse) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.stream.Send(r)
}

func targetMatches(t *gspb.Target, name string) bool {
	for _, n := range t.GetDocuments().GetDocuments() {
		if n == name {
			return true
		}
	}

	if c := t.GetCollection(); c != nil {
		prefix := strings.TrimRight(c.Collection, "/") + "/"

		return strings.HasPrefix(name, prefix) && !strings.Contains(strings.TrimPrefix(name, prefix), "/")
	}

	return false
}

// A room document can contain nested collection/document pairs.
func validGSName(name string) bool {
	p := strings.Split(name, "/")

	if len(name) > 512 || len(p) < 3 || len(p) > 33 || len(p)%2 != 1 || p[0] != "docs" {
		return false
	}

	for _, segment := range p[1:] {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}

		for _, c := range segment {
			if c < 32 || c == 127 {
				return false
			}
		}
	}

	return true
}

func gsPrecondition(p *gspb.Precondition, d *gspb.Document) error {
	if p == nil {
		return nil
	}

	switch x := p.ConditionType.(type) {
	case *gspb.Precondition_Exists:
		if x.Exists == (d != nil) {
			return nil
		}

	case *gspb.Precondition_UpdateTime:
		if d != nil && x.UpdateTime != nil && proto.Equal(x.UpdateTime, d.UpdateTime) {
			return nil
		}
	}

	return status.Error(codes.FailedPrecondition, "document precondition failed")
}

func (s *gamesyncServer) WriteDocuments(ctx context.Context, req *gspb.WriteDocumentsRequest) (response *gspb.WriteDocumentsResponse, resultErr error) {
	defer func() {
		if resultErr != nil {
			s.p.event(map[string]any{
				"event":  "gamesync_write_rejected",
				"reason": status.Convert(resultErr).Message(),
			})
		}
	}()

	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")

	if len(v) != 1 {
		return nil, status.Error(codes.Unauthenticated, "session token required")
	}

	raw := strings.TrimSpace(v[0])

	if len(raw) >= 7 && strings.EqualFold(raw[:7], "Bearer ") {
		raw = strings.TrimSpace(raw[7:])
	}

	proof, err := verifySessionProof(raw, s.p.identityKey, "gamesync-access")
	if err != nil {
		return nil, err
	}

	session := strings.Split(proof, "|")[1]
	cut := strings.LastIndex(session, "/userSessions/")

	if cut < 0 {
		return nil, status.Error(codes.Unauthenticated, "invalid session")
	}

	room := session[:cut]
	s.p.event(map[string]any{
		"event":      "gamesync_write_requested",
		"operations": len(req.GetWriteOperations()),
		"deferments": len(req.GetDefermentOperations()),
	})

	if len(req.GetWriteOperations())+len(req.GetDefermentOperations()) > 128 {
		return nil, status.Error(codes.ResourceExhausted, "write limit")
	}

	s.publication.Lock()
	defer s.publication.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err = verifySessionProof(raw, s.p.identityKey, "gamesync-access"); err != nil {
		return nil, err
	}

	if s.documents[room] == nil {
		return nil, status.Error(codes.NotFound, "room no longer exists")
	}

	docs := map[string]*gspb.Document{}

	for n, d := range s.documents[room] {
		docs[n] = proto.Clone(d).(*gspb.Document)
	}

	globals := map[string]*commonpb.Value{}

	for n, v := range s.globals[room] {
		globals[n] = proto.Clone(v).(*commonpb.Value)
	}

	deferred := map[string]*gspb.Deferment{}

	for n, d := range s.deferments[proof] {
		deferred[n] = proto.Clone(d).(*gspb.Deferment)
	}

	now := timestamppb.Now()
	results := []*gspb.WriteResult{}
	changed := map[string]bool{}

	for _, op := range req.GetWriteOperations() {
		known := []int{}
		op.ProtoReflect().Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			known = append(known, int(f.Number()))

			return true
		})
		s.p.event(map[string]any{
			"event":         "gamesync_write_shape",
			"known_fields":  known,
			"unknown_bytes": len(op.ProtoReflect().GetUnknown()),
			"encoded_bytes": proto.Size(op),
		})

		var name string

		var pre *gspb.Precondition

		switch x := op.GetOperationType().(type) {
		case *gspb.WriteOperation_UpdateDocument:
			name = x.UpdateDocument.GetDocument().GetName()
			pre = x.UpdateDocument.GetCurrentDocument()

		case *gspb.WriteOperation_MergeDocument:
			name = x.MergeDocument.GetDocument().GetName()

		case *gspb.WriteOperation_DeleteDocument:
			name = x.DeleteDocument.GetName()
			pre = x.DeleteDocument.GetCurrentDocument()

		case *gspb.WriteOperation_TransformDocument:
			name = x.TransformDocument.GetName()

		default:
			return nil, status.Error(codes.InvalidArgument, "empty write")
		}

		parts := strings.Split(name, "/")
		prefix := "other"

		if len(parts) > 0 && parts[0] == "docs" {
			prefix = "docs"
		}

		s.p.event(map[string]any{
			"event":        "gamesync_document_shape",
			"path":         redactedDocumentPath(name),
			"segments":     len(parts),
			"length":       len(name),
			"prefix":       prefix,
			"trailing_nul": strings.HasSuffix(name, "\x00"),
		})

		if !validGSName(name) {
			return nil, status.Error(codes.InvalidArgument, "invalid room document")
		}

		if name == "docs/__gs/m" && s.rooms != nil {
			s.rooms.mu.Lock()
			owner := false

			for _, record := range s.rooms.tickets {
				if record.ticket.GetGameSession().GetName() == room {
					owner = record.owner == strings.Split(proof, "|")[0] || tagLobbyPropertiesUpdate(record, proof, docs[name], op)
					if owner {
						break
					}
				}
			}

			s.rooms.mu.Unlock()

			if !owner {
				return nil, status.Error(codes.PermissionDenied, "session control belongs to the host")
			}

			if op.GetDeleteDocument() != nil {
				return nil, status.Error(codes.FailedPrecondition, "session control cannot be deleted")
			}
		}

		old := docs[name]

		if err = gsPrecondition(pre, old); err != nil {
			return nil, err
		}

		d := &gspb.Document{
			Name:       name,
			Fields:     cloneFields(nil),
			CreateTime: now,
			UpdateTime: now,
		}

		if old != nil {
			d = proto.Clone(old).(*gspb.Document)
			d.Fields = cloneFields(d.Fields)
			d.UpdateTime = now
		}

		result := &gspb.WriteResult{
			ResultType: &gspb.WriteResult_UpdateResult{
				UpdateResult: &gspb.UpdateResult{},
			},
		}

		switch x := op.GetOperationType().(type) {
		case *gspb.WriteOperation_UpdateDocument:
			input := x.UpdateDocument.GetDocument()

			if input.GetFields() == nil {
				return nil, status.Error(codes.InvalidArgument, "fields required")
			}

			mask := x.UpdateDocument.GetUpdateMask()

			if mask == nil || len(mask.Paths) == 1 && mask.Paths[0] == "*" {
				d.Fields = cloneFields(input.Fields)
			} else {
				for _, p := range mask.Paths {
					path, e := fieldPath(p)
					if e != nil {
						return nil, e
					}

					setField(d.Fields, path, readField(input.Fields, path))
				}
			}

		case *gspb.WriteOperation_MergeDocument:
			if x.MergeDocument.GetDocument().GetFields() == nil {
				return nil, status.Error(codes.InvalidArgument, "fields required")
			}

			for k, v := range x.MergeDocument.Document.Fields.Fields {
				if v == nil {
					return nil, status.Error(codes.InvalidArgument, "nil field")
				}

				d.Fields.Fields[k] = proto.Clone(v).(*commonpb.Value)
			}

		case *gspb.WriteOperation_TransformDocument:
			if old == nil {
				return nil, status.Error(codes.FailedPrecondition, "transform document missing")
			}

			values := []*commonpb.Value{}

			for _, t := range x.TransformDocument.FieldTransforms {
				v, e := applyFieldTransform(d, t, globals, now)
				if e != nil {
					return nil, e
				}

				values = append(values, v)
			}

			result.ResultType = &gspb.WriteResult_TransformResult{
				TransformResult: &gspb.TransformResult{Results: values},
			}

		case *gspb.WriteOperation_DeleteDocument:
			result.ResultType = &gspb.WriteResult_DeleteResult{
				DeleteResult: &gspb.DeleteResult{},
			}
		}

		if proto.Size(d) > 65536 {
			return nil, status.Error(codes.ResourceExhausted, "document too large")
		}

		if op.GetDeleteDocument() != nil {
			delete(docs, name)
		} else {
			docs[name] = d
			s.logDocument(d, "write")
		}

		changed[name] = true
		results = append(results, result)
	}

	for _, op := range req.GetDefermentOperations() {
		var name string

		if d := op.GetDeleteDeferment(); d != nil {
			name = d.Name
		} else {
			name = op.GetUpdateDeferment().GetDeferment().GetName()
		}

		k := strings.LastIndex(name, "/deferments/")

		if k < 0 || len(name) > 512 || name[k+len("/deferments/"):] == "" || !sessionNameMatches(name[:k], session, strings.Split(proof, "|")[0]) {
			return nil, status.Error(codes.PermissionDenied, "foreign deferment")
		}

		if op.GetDeleteDeferment() != nil {
			delete(deferred, name)
			continue
		}

		d := op.GetUpdateDeferment().GetDeferment()

		if len(d.GetWriteOperations()) == 0 || len(d.GetWriteOperations()) > 128 {
			return nil, status.Error(codes.InvalidArgument, "cleanup operations required")
		}

		for _, w := range d.WriteOperations {
			del := w.GetDeleteDocument()

			if del == nil {
				return nil, status.Error(codes.Unimplemented, "only deferred cleanup deletes supported")
			}

			if !validGSName(del.Name) {
				return nil, status.Error(codes.InvalidArgument, "invalid cleanup document")
			}
		}

		deferred[name] = proto.Clone(d).(*gspb.Deferment)
	}

	if len(deferred) > 128 {
		return nil, status.Error(codes.ResourceExhausted, "deferment limit")
	}

	if len(docs) > 1024 {
		return nil, status.Error(codes.ResourceExhausted, "room document limit")
	}
	// Commit host control to both the matchmaking resource and the subscribed
	// Gamesync state. Publishing only __gs/m leaves clients waiting on __gs/s.
	if changed["docs/__gs/m"] && docs["docs/__gs/m"] != nil {
		control := docs["docs/__gs/m"]
		state := proto.Clone(control).(*gspb.Document)
		state.Name = "docs/__gs/s"

		if old := docs[state.Name]; old != nil {
			state.CreateTime = old.CreateTime
		}

		docs[state.Name] = state
		changed[state.Name] = true

		if s.rooms != nil {
			s.rooms.mu.Lock()

			for _, record := range s.rooms.tickets {
				resource := record.ticket.GetGameSession()

				if resource.GetName() != room {
					continue
				}

				fields := control.GetFields().GetFields()

				if value := fields["cp"]; value != nil {
					resource.CanParticipate = value.GetBooleanValue()
				}

				if value := fields["ip"]; value != nil {
					resource.IsPublic = value.GetBooleanValue()
				}

				if value := fields["pw"]; value != nil {
					resource.Password = value.GetStringValue()
				}

				if value := fields["prp"]; value.GetMapValue() != nil {
					resource.Properties = proto.Clone(value.GetMapValue()).(*commonpb.MapValue)
				}
			}

			s.rooms.mu.Unlock()
		}
	}

	if s.deferments == nil {
		s.deferments = map[string]map[string]*gspb.Deferment{}
	}

	s.deferments[proof] = deferred
	s.documents[room] = docs

	if s.globals == nil {
		s.globals = map[string]map[string]*commonpb.Value{}
	}

	s.globals[room] = globals
	s.captureCommittedPIA(room, docs, changed)
	s.publishChangesLocked(room, docs, changed, now)
	s.p.event(map[string]any{
		"event":             "gamesync_write_committed",
		"operations":        len(results),
		"changed_documents": len(changed),
	})

	return &gspb.WriteDocumentsResponse{WriteResults: results, CommitTime: now}, nil
}

func (s *gamesyncServer) publishChangesLocked(room string, docs map[string]*gspb.Document, changed map[string]bool, now *timestamppb.Timestamp) {
	for sub := range s.subs[room] {
		for id, target := range sub.targets {
			for n := range changed {
				if !targetMatches(target, n) {
					continue
				}

				d := docs[n]
				kind := gspb.DocumentChange_UPDATED

				if d == nil {
					kind = gspb.DocumentChange_DELETED
					d = &gspb.Document{Name: n, UpdateTime: now}
				}

				err := sub.Send(&gspb.KeepUserSessionResponse{
					ResponseType: &gspb.KeepUserSessionResponse_DocumentChange{
						DocumentChange: &gspb.DocumentChange{
							TargetId:           id,
							DocumentChangeType: kind,
							Document:           proto.Clone(d).(*gspb.Document),
						},
					},
				})
				s.logDelivery(sub, d, kind.String(), err)
			}
		}
	}
}

func (s *gamesyncServer) cleanupDeferments(proof, room string) {
	s.publication.Lock()
	defer s.publication.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupDefermentsLocked(proof, room)
}

func (s *gamesyncServer) cleanupDefermentsLocked(proof, room string) {
	// Only subscriptions of this participant keep its deferments alive.
	for sub := range s.subs[room] {
		if sub.proof == proof {
			return
		}
	}

	changed := map[string]bool{}
	docs := s.documents[room]

	for _, d := range s.deferments[proof] {
		for _, w := range d.WriteOperations {
			del := w.GetDeleteDocument()

			if del != nil && gsPrecondition(del.GetCurrentDocument(), docs[del.Name]) == nil {
				delete(docs, del.Name)
				changed[del.Name] = true
			}
		}
	}

	delete(s.deferments, proof)
	s.publishChangesLocked(room, docs, changed, timestamppb.Now())
}
