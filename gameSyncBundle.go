package main

import (
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "github.com/alyeri/jamboree-npln-server/proto/common"
	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

const bezelEntityPrefix = "docs/BezelEntity/"
const bezelBundleDocument = "docs/BezelEntityBundles/DocBundled"

// Jamboree 2.3.0 registers each entity's leaf ID with BundleSnapshots. Its
// reader at 0x145f30 retrieves that ID as a map field from DocBundled through
// 0xd61620. Each map contains the committed fields of the source entity.
// Call with the room's staged documents, before committing or publishing.
func updateBezelBundle(docs map[string]*gspb.Document, changed map[string]bool, now *timestamppb.Timestamp) error {
	needed := false

	for name := range changed {
		if strings.HasPrefix(name, bezelEntityPrefix) {
			needed = true

			break
		}
	}

	if !needed {
		return nil
	}

	fields := &commonpb.MapValue{
		Fields: map[string]*commonpb.Value{},
	}

	for name, document := range docs {
		if !strings.HasPrefix(name, bezelEntityPrefix) {
			continue
		}

		id := strings.TrimPrefix(name, bezelEntityPrefix)

		if id == "" || strings.Contains(id, "/") || document == nil {
			continue
		}

		fields.Fields[id] = &commonpb.Value{
			ValueType: &commonpb.Value_MapValue{
				MapValue: cloneFields(document.GetFields()),
			},
		}
	}

	bundle := &gspb.Document{
		Name:       bezelBundleDocument,
		Fields:     fields,
		CreateTime: now,
		UpdateTime: now,
	}

	if old := docs[bezelBundleDocument]; old != nil {
		bundle.CreateTime = old.CreateTime

		if proto.Equal(old.GetFields(), bundle.Fields) {
			return nil
		}
	}

	if proto.Size(bundle) > 65536 {
		return status.Error(codes.ResourceExhausted, "entity bundle too large")
	}

	docs[bezelBundleDocument] = bundle
	changed[bezelBundleDocument] = true

	return nil
}
