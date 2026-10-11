package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func TestGSAtomicWritesAndDeferredCleanup(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)

	var logs bytes.Buffer

	room := "tenants/t-adf89f68-lp1/gameSessions/test"
	proof := "owner|" + room + "/userSessions/host"
	token, _ := (&authServer{
		key: key,
	}).signSessionToken(proof, "gamesync-access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token))
	s := &gamesyncServer{
		p: &serverContext{
			writer:      &logs,
			identityKey: key,
		},
		documents: map[string]map[string]*gs.Document{
			room: {},
		},
	}
	update := &gs.WriteOperation{
		OperationType: &gs.WriteOperation_UpdateDocument{
			UpdateDocument: &gs.UpdateDocumentRequest{
				Document: &gs.Document{
					Name: "docs/__us/host/c/test",
					Fields: &common.MapValue{
						Fields: map[string]*common.Value{
							"x": intValue(1),
						},
					},
				},
			},
		},
	}
	invalid := &gs.WriteOperation{
		OperationType: &gs.WriteOperation_DeleteDocument{
			DeleteDocument: &gs.DeleteDocumentRequest{
				Name: "docs/../test",
			},
		},
	}
	_, e := s.WriteDocuments(ctx, &gs.WriteDocumentsRequest{
		WriteOperations: []*gs.WriteOperation{
			update,
			invalid,
		},
	})

	if status.Code(e) != codes.InvalidArgument || len(s.documents[room]) != 0 {
		t.Fatal("partial commit", e)
	}

	cleanup := &gs.DefermentOperation{
		OperationType: &gs.DefermentOperation_UpdateDeferment{
			UpdateDeferment: &gs.UpdateDefermentRequest{
				Deferment: &gs.Deferment{
					Name: "userSessions/current/deferments/test",
					WriteOperations: []*gs.WriteOperation{
						{
							OperationType: &gs.WriteOperation_DeleteDocument{
								DeleteDocument: &gs.DeleteDocumentRequest{
									Name: "docs/__us/host/c/test",
								},
							},
						},
					},
				},
			},
		},
	}
	r, e := s.WriteDocuments(ctx, &gs.WriteDocumentsRequest{
		WriteOperations: []*gs.WriteOperation{
			update,
		},
		DefermentOperations: []*gs.DefermentOperation{
			cleanup,
		},
	})

	if e != nil || len(r.GetWriteResults()) != 1 || s.documents[room]["docs/__us/host/c/test"] == nil {
		t.Fatal(e)
	}

	token2, _ := (&authServer{
		key: key,
	}).signSessionToken("other|tenants/t-adf89f68-lp1/gameSessions/other/userSessions/host", "gamesync-access", time.Minute)
	_, e = s.WriteDocuments(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token2)), &gs.WriteDocumentsRequest{
		WriteOperations: []*gs.WriteOperation{
			update,
		},
	})

	if status.Code(e) != codes.NotFound {
		t.Fatal("foreign room access", e)
	}

	doc, readErr := s.GetDocument(ctx, &gs.GetDocumentRequest{
		Name: "docs/__us/host/c/test",
	})

	if readErr != nil || doc.GetFields().GetFields()["x"].GetIntegerValue() != 1 {
		t.Fatal("stored nested read failed", readErr)
	}

	_, readErr = s.GetDocument(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token2)), &gs.GetDocumentRequest{
		Name: "docs/__us/host/c/test",
	})

	if status.Code(readErr) != codes.NotFound {
		t.Fatal("foreign read accepted", readErr)
	}

	s.cleanupDeferments(proof, room)

	if s.documents[room]["docs/__us/host/c/test"] != nil {
		t.Fatal("cleanup not applied")
	}

	if bytes.Contains(logs.Bytes(), []byte(token)) {
		t.Fatal("token leaked")
	}
}

func TestGSNestedDocumentPaths(t *testing.T) {
	for _, n := range []string{"docs/c/test", "docs/__us/host/c/test", "docs/a/b/c/d/e/f"} {
		if !validGSName(n) {
			t.Errorf("nested document rejected %q", n)
		}
	}

	for _, n := range []string{
		"docs/c",
		"docs/c/test/nested",
		"docs/__us/../c/test",
		"docs/__us//c/test",
		"docs/c/test\x00",
		"tenants/other/docs/c/test",
		"/docs/c/test",
	} {
		if validGSName(n) {
			t.Errorf("invalid path accepted %q", n)
		}
	}
}
