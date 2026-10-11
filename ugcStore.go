package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	ugcpb "github.com/alyeri/jamboree-npln-server/proto/ugcstore/v1"
)

func newUgcServer(p *serverContext, path string) (*ugcServer, error) {
	s := &ugcServer{
		p:    p,
		path: path,
		docs: map[string]*ugcpb.Document{},
	}

	if path == "" {
		return s, nil
	}

	raw, err := os.ReadFile(path)

	if os.IsNotExist(err) {
		return s, nil
	}

	if err != nil {
		return nil, err
	}

	var disk map[string][]byte

	if err = json.Unmarshal(raw, &disk); err != nil {
		return nil, err
	}

	for name, data := range disk {
		d := &ugcpb.Document{}

		if err = proto.Unmarshal(data, d); err != nil {
			return nil, err
		}

		s.docs[name] = d
	}

	return s, nil
}
func validDocument(name string) bool {
	for _, p := range []string{"tenants/current/documents/", "tenants/t-adf89f68-lp1/documents/", "tenants/t-adf89f68/documents/"} {
		if strings.HasPrefix(name, p) && len(name) > len(p) && len(name) <= 1024 && !strings.Contains(name, "..") {
			return true
		}
	}

	return false
}
func checkPrecondition(p *ugcpb.Precondition, d *ugcpb.Document) error {
	if p == nil {
		return nil
	}

	switch x := p.ConditionType.(type) {
	case *ugcpb.Precondition_Exists:
		if x.Exists != (d != nil) {
			return status.Error(codes.FailedPrecondition, "document existence precondition failed")
		}

	case *ugcpb.Precondition_UpdateTime:
		if d == nil || !proto.Equal(x.UpdateTime, d.UpdateTime) {
			return status.Error(codes.FailedPrecondition, "document update precondition failed")
		}
	}

	return nil
}
func (s *ugcServer) CommitDocuments(ctx context.Context, req *ugcpb.CommitDocumentsRequest) (*ugcpb.CommitDocumentsResponse, error) {
	if err := verifyAccess(ctx, s.p.identityKey); err != nil {
		return nil, err
	}

	if req.GetTenant() != "tenants/current" && req.GetTenant() != "tenants/t-adf89f68-lp1" && req.GetTenant() != "tenants/t-adf89f68" {
		return nil, status.Error(codes.InvalidArgument, "Jamboree tenant resource required")
	}

	if len(req.WriteOperations) == 0 || len(req.WriteOperations) > 128 {
		return nil, status.Error(codes.InvalidArgument, "commit must contain 1 to 128 writes")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	next := make(map[string]*ugcpb.Document, len(s.docs))

	for k, v := range s.docs {
		next[k] = v
	}

	now := timestamppb.Now()
	response := &ugcpb.CommitDocumentsResponse{
		CommitTime: now,
	}

	s.p.event(map[string]any{
		"event":       "ugc_commit",
		"write_count": len(req.WriteOperations),
	})

	for _, op := range req.WriteOperations {
		known := []int{}

		op.ProtoReflect().Range(func(f protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			known = append(known, int(f.Number()))

			return true
		})

		unknown := []int{}
		raw := op.ProtoReflect().GetUnknown()

		for len(raw) > 0 {
			n, t, k := protowire.ConsumeTag(raw)

			if k < 0 {
				break
			}

			raw = raw[k:]
			k = protowire.ConsumeFieldValue(n, t, raw)

			if k < 0 {
				break
			}

			unknown = append(unknown, int(n))
			raw = raw[k:]
		}

		s.p.event(map[string]any{
			"event":          "ugc_write_shape",
			"known_fields":   known,
			"unknown_fields": unknown,
			"encoded_bytes":  proto.Size(op),
		})

		if u := op.GetUpdateDocument(); u != nil {
			d := u.GetDocument()

			if d == nil || !validDocument(d.Name) {
				return nil, status.Error(codes.InvalidArgument, "document resource required")
			}

			old := next[d.Name]

			if err := checkPrecondition(u.CurrentDocument, old); err != nil {
				return nil, err
			}

			copy := proto.Clone(d).(*ugcpb.Document)

			if u.UpdateMask != nil {
				fields := &common.MapValue{
					Fields: map[string]*common.Value{},
				}

				if old != nil && old.Fields != nil {
					fields = proto.Clone(old.Fields).(*common.MapValue)
				}

				for _, path := range u.UpdateMask.Paths {
					field := strings.Trim(path, "`")

					if field == "" || strings.Contains(field, ".") {
						return nil, status.Error(codes.Unimplemented, "nested field masks not implemented")
					}

					if v := d.GetFields().GetFields()[field]; v != nil {
						fields.Fields[field] = proto.Clone(v).(*common.Value)
					} else {
						delete(fields.Fields, field)
					}
				}

				copy.Fields = fields
			}

			copy.CreateTime = now

			if old != nil {
				copy.CreateTime = old.CreateTime
			}

			copy.UpdateTime = now
			next[copy.Name] = copy
		} else if d := op.GetDeleteDocument(); d != nil {
			if !validDocument(d.Name) {
				return nil, status.Error(codes.InvalidArgument, "document resource required")
			}

			if err := checkPrecondition(d.CurrentDocument, next[d.Name]); err != nil {
				return nil, err
			}

			delete(next, d.Name)
		} else if unset := op.GetUnsetAttachment(); unset != nil {
			if !validDocument(unset.Parent) {
				return nil, status.Error(codes.InvalidArgument, "attachment parent document required")
			}
			// Removing an optional attachment is idempotent, including a first-time
			// document registration that has no attachment yet.
			delete(next, "\x00attachment/"+unset.Parent)
		} else if set := op.GetSetAttachment(); set != nil {
			if !validDocument(set.Parent) {
				return nil, status.Error(codes.InvalidArgument, "attachment parent document required")
			}

			key := "\x00attachment/" + set.Parent

			if condition, ok := set.Precondition.(*ugcpb.SetAttachmentRequest_Exists); ok && condition.Exists != (next[key] != nil) {
				return nil, status.Error(codes.FailedPrecondition, "attachment existence precondition failed")
			}

			next[key] = &ugcpb.Document{
				Name:       set.Parent,
				CreateTime: now,
				UpdateTime: now,
				Fields: &common.MapValue{
					Fields: map[string]*common.Value{
						"body": {
							ValueType: &common.Value_BytesValue{
								BytesValue: append([]byte(nil), set.Body...),
							},
						},
						"mime_type": {
							ValueType: &common.Value_StringValue{
								StringValue: set.MimeType,
							},
						},
					},
				},
			}
		} else {
			s.p.event(map[string]any{
				"event":     "ugc_write_unsupported",
				"transform": op.GetTransformDocument() != nil,
			})

			return nil, status.Error(codes.Unimplemented, "document write operation not implemented")
		}

		response.WriteResults = append(response.WriteResults, &ugcpb.WriteResult{
			UpdateTime: now,
		})
	}

	if s.path != "" {
		disk := map[string][]byte{}

		for k, v := range next {
			b, e := proto.Marshal(v)

			if e != nil {
				return nil, status.Error(codes.Internal, "store serialization failed")
			}

			disk[k] = b
		}

		raw, e := json.Marshal(disk)

		if e != nil {
			return nil, status.Error(codes.Internal, "store serialization failed")
		}

		f, e := os.OpenFile(s.path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)

		if e != nil {
			return nil, status.Error(codes.Internal, "store unavailable")
		}

		_, e = f.Write(raw)

		if e == nil {
			e = f.Sync()
		}

		closeErr := f.Close()

		if e == nil {
			e = closeErr
		}

		if e == nil {
			e = os.Rename(s.path+".tmp", s.path)
		}

		if e != nil {
			return nil, status.Error(codes.Internal, "store persistence failed")
		}
	}

	s.docs = next

	return response, nil
}
