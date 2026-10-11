package main

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/alyeri/jamboree-npln-server/proto/common"
	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

type bundleTestStream struct {
	grpc.ServerStream
	messages []*gspb.KeepUserSessionResponse
}

func (s *bundleTestStream) Send(response *gspb.KeepUserSessionResponse) error {
	s.messages = append(s.messages, proto.Clone(response).(*gspb.KeepUserSessionResponse))

	return nil
}

func (*bundleTestStream) Recv() (*gspb.KeepUserSessionRequest, error) {
	return nil, io.EOF
}

func TestBezelBundleCommitDeliveryAndRollback(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	room := "tenants/t-adf89f68-lp1/gameSessions/bundle"
	proof := "tenants/current/users/host|" + room + "/userSessions/host"
	token, err := (&authServer{
		key: key,
	}).signSessionToken(proof, "gamesync-access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
	stream := &bundleTestStream{}
	target := &gspb.Target{
		TargetType: &gspb.Target_Documents{
			Documents: &gspb.DocumentsTarget{
				Documents: []string{
					bezelBundleDocument,
				},
			},
		},
	}
	subscriber := &gsSubscriber{
		proof:  proof,
		stream: stream,
		targets: map[string]*gspb.Target{
			"bundle": target,
		},
	}
	server := &gamesyncServer{
		p: &serverContext{
			writer:      &bytes.Buffer{},
			identityKey: key,
		},
		documents: map[string]map[string]*gspb.Document{
			room:            {},
			room + "-other": {},
		},
		subs: map[string]map[*gsSubscriber]bool{
			room: {
				subscriber: true,
			},
		},
	}

	write := func(name, field string, data []byte) *gspb.WriteOperation {
		return &gspb.WriteOperation{
			OperationType: &gspb.WriteOperation_UpdateDocument{
				UpdateDocument: &gspb.UpdateDocumentRequest{
					Document: &gspb.Document{
						Name: name,
						Fields: &commonpb.MapValue{Fields: map[string]*commonpb.Value{
							field: {
								ValueType: &commonpb.Value_BytesValue{
									BytesValue: data,
								},
							},
						}},
					},
					UpdateMask: &fieldmaskpb.FieldMask{
						Paths: []string{"*"},
					},
				},
			},
		}
	}

	update := write(bezelEntityPrefix+"20", "mg_pick", []byte{0, 255, 4})
	result, err := server.WriteDocuments(ctx, &gspb.WriteDocumentsRequest{
		WriteOperations: []*gspb.WriteOperation{
			update,
		},
	})

	if err != nil || len(result.GetWriteResults()) != 1 {
		t.Fatal("source write failed", err)
	}

	bundle := server.documents[room][bezelBundleDocument]

	if !bytes.Equal(bundle.GetFields().GetFields()["20"].GetMapValue().GetFields()["mg_pick"].GetBytesValue(), []byte{0, 255, 4}) {
		t.Fatal("bundle did not preserve binary field")
	}

	if len(stream.messages) != 1 || stream.messages[0].GetDocumentChange().GetDocument().GetName() != bezelBundleDocument {
		t.Fatal("bundle subscriber did not receive committed document")
	}

	if len(server.documents[room+"-other"]) != 0 {
		t.Fatal("bundle leaked into another room")
	}

	snapshot := server.sessionSnapshot(room+"/userSessions/late", target)

	if len(snapshot) != 1 || !proto.Equal(snapshot[0], bundle) {
		t.Fatal("late subscriber did not receive current bundle")
	}

	before := proto.Clone(bundle).(*gspb.Document)
	invalid := &gspb.WriteOperation{
		OperationType: &gspb.WriteOperation_DeleteDocument{
			DeleteDocument: &gspb.DeleteDocumentRequest{
				Name: "docs/../invalid",
			},
		},
	}
	_, err = server.WriteDocuments(ctx, &gspb.WriteDocumentsRequest{
		WriteOperations: []*gspb.WriteOperation{
			write(bezelEntityPrefix+"0", "safety", []byte{1}),
			invalid,
		},
	})

	if status.Code(err) != codes.InvalidArgument || !proto.Equal(before, server.documents[room][bezelBundleDocument]) || len(stream.messages) != 1 {
		t.Fatal("failed transaction changed or published bundle", err)
	}

	_, err = server.WriteDocuments(ctx, &gspb.WriteDocumentsRequest{
		WriteOperations: []*gspb.WriteOperation{
			write(bezelBundleDocument, "forged", []byte{1}),
		},
	})

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("client overwrote derived bundle", err)
	}

	deletion := &gspb.WriteOperation{
		OperationType: &gspb.WriteOperation_DeleteDocument{
			DeleteDocument: &gspb.DeleteDocumentRequest{
				Name: bezelEntityPrefix + "20",
			},
		},
	}
	_, err = server.WriteDocuments(ctx, &gspb.WriteDocumentsRequest{
		WriteOperations: []*gspb.WriteOperation{
			deletion,
		},
	})

	if err != nil || len(server.documents[room][bezelBundleDocument].GetFields().GetFields()) != 0 || len(stream.messages) != 2 {
		t.Fatal("entity deletion did not update bundle", err)
	}
}

func TestBezelBundleSnapshotsAndSizeLimit(t *testing.T) {
	now := timestamppb.Now()
	source := &gspb.Document{
		Name: bezelEntityPrefix + "20",
		Fields: &commonpb.MapValue{
			Fields: map[string]*commonpb.Value{
				"count": intValue(2),
			},
		},
	}
	docs := map[string]*gspb.Document{
		source.Name: source,
	}
	changed := map[string]bool{
		source.Name: true,
	}

	if err := updateBezelBundle(docs, changed, now); err != nil {
		t.Fatal(err)
	}

	source.Fields.Fields["count"] = intValue(9)

	if docs[bezelBundleDocument].Fields.Fields["20"].GetMapValue().Fields["count"].GetIntegerValue() != 2 {
		t.Fatal("source mutation changed previously committed bundle")
	}

	before := proto.Clone(docs[bezelBundleDocument]).(*gspb.Document)
	source.Fields.Fields["large"] = &commonpb.Value{
		ValueType: &commonpb.Value_BytesValue{
			BytesValue: bytes.Repeat([]byte{1}, 65536),
		},
	}

	if err := updateBezelBundle(docs, changed, now); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized bundle accepted", err)
	}

	if !proto.Equal(before, docs[bezelBundleDocument]) {
		t.Fatal("size failure replaced existing bundle")
	}
}

func TestBezelBundleDeferredCleanupPreservesOtherEntities(t *testing.T) {
	room := "tenants/t-adf89f68-lp1/gameSessions/cleanup"
	proof := "host|" + room + "/userSessions/host"
	docs := map[string]*gspb.Document{}
	changed := map[string]bool{}

	for _, id := range []string{"0", "20"} {
		name := bezelEntityPrefix + id
		docs[name] = &gspb.Document{
			Name: name,
			Fields: &commonpb.MapValue{
				Fields: map[string]*commonpb.Value{
					"value": intValue(1),
				},
			},
		}
		changed[name] = true
	}

	if err := updateBezelBundle(docs, changed, timestamppb.Now()); err != nil {
		t.Fatal(err)
	}

	server := &gamesyncServer{
		p: &serverContext{
			writer: &bytes.Buffer{},
		},
		documents: map[string]map[string]*gspb.Document{
			room: docs,
		},
		deferments: map[string]map[string]*gspb.Deferment{
			proof: {"cleanup": {
				WriteOperations: []*gspb.WriteOperation{{
					OperationType: &gspb.WriteOperation_DeleteDocument{
						DeleteDocument: &gspb.DeleteDocumentRequest{
							Name: bezelEntityPrefix + "20",
						},
					},
				}},
			}},
		},
	}

	server.cleanupDeferments(proof, room)

	fields := docs[bezelBundleDocument].Fields.Fields

	if len(fields) != 1 || fields["0"].GetMapValue().Fields["value"].GetIntegerValue() != 1 {
		t.Fatal("cleanup left deleted entity or removed unrelated entity")
	}
}
