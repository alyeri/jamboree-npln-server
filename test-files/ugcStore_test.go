package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	ugcpb "github.com/alyeri/jamboree-npln-server/proto/ugcstore/v1"
)

func TestCommitPersistsAndIsAtomic(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &logs,
	}
	path := filepath.Join(t.TempDir(), "store.json")
	s, err := newUgcServer(p, path)

	if err != nil {
		t.Fatal(err)
	}

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/test", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	name := "tenants/current/documents/profile/test"
	d := &ugcpb.Document{
		Name: name,
		Fields: &common.MapValue{
			Fields: map[string]*common.Value{
				"name": {
					ValueType: &common.Value_StringValue{
						StringValue: "private fixture",
					},
				},
			},
		},
	}
	write := &ugcpb.WriteOperation{
		OperationType: &ugcpb.WriteOperation_UpdateDocument{
			UpdateDocument: &ugcpb.UpdateDocumentRequest{
				Document: d,
			},
		},
	}
	unset := &ugcpb.WriteOperation{
		OperationType: &ugcpb.WriteOperation_UnsetAttachment{
			UnsetAttachment: &ugcpb.UnsetAttachmentRequest{
				Parent: name,
			},
		},
	}
	req := &ugcpb.CommitDocumentsRequest{
		Tenant: "tenants/current",
		WriteOperations: []*ugcpb.WriteOperation{
			write,
			unset,
		},
	}
	r, err := s.CommitDocuments(ctx, req)

	if err != nil || len(r.GetWriteResults()) != 2 || r.GetCommitTime() == nil {
		t.Fatal("commit failed", err)
	}

	restored, err := newUgcServer(p, path)

	if err != nil {
		t.Fatal(err)
	}

	got, err := restored.GetDocument(ctx, &ugcpb.GetDocumentRequest{
		Name: name,
	})

	if err != nil || got.GetFields().GetFields()["name"].GetStringValue() != "private fixture" {
		t.Fatal("not persisted", err)
	}

	d.Name = name + "-atomic"
	req.WriteOperations = append(req.WriteOperations, &ugcpb.WriteOperation{})
	_, err = s.CommitDocuments(ctx, req)

	if status.Code(err) != codes.Unimplemented {
		t.Fatal(err)
	}

	_, err = s.GetDocument(ctx, &ugcpb.GetDocumentRequest{
		Name: d.Name,
	})

	if status.Code(err) != codes.NotFound {
		t.Fatal("partial transaction applied")
	}
}

func TestAttachmentPersistenceAndRemoval(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{8}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &logs,
	}
	path := filepath.Join(t.TempDir(), "store.json")
	s, _ := newUgcServer(p, path)
	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/test", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	name := "tenants/current/documents/attachment-test"
	body := []byte("private-body")
	_, err := s.CommitDocuments(ctx, &ugcpb.CommitDocumentsRequest{
		Tenant: "tenants/current",
		WriteOperations: []*ugcpb.WriteOperation{
			{
				OperationType: &ugcpb.WriteOperation_SetAttachment{
					SetAttachment: &ugcpb.SetAttachmentRequest{
						Parent:   name,
						Body:     body,
						MimeType: "application/octet-stream",
					},
				},
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	restored, err := newUgcServer(p, path)

	if err != nil || !bytes.Equal(restored.docs["\x00attachment/"+name].Fields.Fields["body"].GetBytesValue(), body) {
		t.Fatal("attachment not persisted", err)
	}

	for i := 0; i < 2; i++ {
		_, err = restored.CommitDocuments(ctx, &ugcpb.CommitDocumentsRequest{
			Tenant: "tenants/current",
			WriteOperations: []*ugcpb.WriteOperation{
				{
					OperationType: &ugcpb.WriteOperation_UnsetAttachment{
						UnsetAttachment: &ugcpb.UnsetAttachmentRequest{
							Parent: name,
						},
					},
				},
			},
		})

		if err != nil {
			t.Fatal(err)
		}
	}

	reloaded, err := newUgcServer(p, path)

	if err != nil || reloaded.docs["\x00attachment/"+name] != nil {
		t.Fatal("attachment deletion not persisted", err)
	}

	if bytes.Contains(logs.Bytes(), body) {
		t.Fatal("attachment leaked to logs")
	}
}
