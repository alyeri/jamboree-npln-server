package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/status"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

var piaCaptureSequence atomic.Uint32

// Capture only committed signalling, paired with this room's fixed seed.
// Private decoding material stays out of the public diagnostic log.
func (s *gamesyncServer) captureCommittedPIA(room string, docs map[string]*gs.Document, changed map[string]bool) {
	if runtime.GOOS != "linux" || serverVersion != "jamboree-diag-20261006-52" {
		return
	}

	for path := range changed {
		doc := docs[path]
		payload := doc.GetFields().GetFields()["pl"].GetBytesValue()

		if len(payload) == 0 || len(payload) > 4096 || !strings.Contains(path, "/__stu/") {
			continue
		}

		seq := piaCaptureSequence.Add(1)

		if seq > 32 {
			return
		}

		f := doc.GetFields().GetFields()
		fixed := docs["docs/__gs/f"].GetFields().GetFields()
		roomHash := sha256.Sum256([]byte(room))
		record := map[string]any{
			"room_digest":       fmt.Sprintf("%x", roomHash[:8]),
			"seed":              fixed["rs"].GetBytesValue(),
			"payload":           payload,
			"sender_uid":        f["suid"].GetStringValue(),
			"sender_sid":        f["susid"].GetStringValue(),
			"sender_sequence":   f["sussid"].GetIntegerValue(),
			"sender_connection": f["suscid"].GetIntegerValue(),
			"recipient_sid":     path[strings.LastIndex(path, "/")+1:],
		}
		encoded, err := json.Marshal(record)
		if err != nil {
			continue
		}

		dir := "/data/pia-captures/build52"

		if err := os.MkdirAll(dir, 0700); err != nil {
			continue
		}

		if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("%02d.json", seq)), encoded, 0600); err != nil {
			continue
		}

		s.p.event(map[string]any{
			"event":          "pia_private_capture",
			"capture_number": seq,
			"bytes":          len(payload),
			"path":           redactedDocumentPath(path),
		})
	}
}

// Report protocol shape without logging ICE credentials, identities or payloads.
func summarizeFields(fields *common.MapValue, depth int) []map[string]any {
	if depth > 3 {
		return nil
	}

	names := make([]string, 0, len(fields.GetFields()))

	for name := range fields.GetFields() {
		names = append(names, name)
	}

	sort.Strings(names)

	if len(names) > 64 {
		names = names[:64]
	}

	out := []map[string]any{}

	for _, name := range names {
		safe := fmt.Sprintf("<%d>", len(name))

		switch name {
		case "uid", "ussid", "ucsid", "upcsid", "pgn", "suid",
			"susid", "sussid", "suscid", "pl", "mp", "st",
			"att", "ltc", "cfb", "offer", "answer", "candidates",
			"ice", "sdp", "type", "state", "status", "result",
			"cp", "ip", "maxu", "gsid", "addr", "p",
			"mcn", "rs", "pw", "ebf", "bfmin", "bfmax",
			"prp", "__upcsidn":
			safe = name
		}

		item := map[string]any{"key": safe}
		v := fields.Fields[name]

		switch x := v.GetValueType().(type) {
		case *common.Value_IntegerValue:
			item["type"] = "integer"

			if safe == name && name != "uid" && name != "suid" && name != "susid" {
				item["value"] = x.IntegerValue
			}

		case *common.Value_BooleanValue:
			item["type"] = "boolean"
			item["value"] = x.BooleanValue

		case *common.Value_StringValue:
			item["type"] = "string"
			item["length"] = len(x.StringValue)

			if safe == name && name != "pw" {
				sum := sha256.Sum256([]byte(x.StringValue))
				item["digest"] = fmt.Sprintf("%x", sum[:8])
			}

		case *common.Value_BytesValue:
			item["type"] = "bytes"
			item["length"] = len(x.BytesValue)

			if name == "pl" {
				item["pia_header"] = summarizePIAHeader(x.BytesValue)
			}

		case *common.Value_MapValue:
			item["type"] = "map"
			item["fields"] = summarizeFields(x.MapValue, depth+1)

		case *common.Value_ArrayValue:
			item["type"] = "array"
			item["length"] = len(x.ArrayValue.GetValues())

		default:
			item["type"] = "other"
		}

		out = append(out, item)
	}

	return out
}

// Native 2.3.0: 10c6414 strips the final route byte; fdc3c0 reads
// version, message type and big-endian uint16 body size. Never log bodies.
func summarizePIAHeader(payload []byte) map[string]any {
	// Common Packet header: native fcb940/fcbb0c, encrypted flag fcc284.
	if len(payload) >= 30 && payload[len(payload)-1] == 0 && binary.BigEndian.Uint32(payload[:4]) == 0x32ab9864 && payload[4]&0x7f == 15 {
		return map[string]any{
			"format":        "common_packet",
			"version":       15,
			"encrypted":     payload[4]&0x80 != 0,
			"padding_bytes": payload[5] >> 4,
			"body_bytes":    len(payload) - 30,
			"route":         0,
		}
	}

	if len(payload) < 5 || payload[len(payload)-1] > 1 || payload[0] != 1 {
		return nil
	}

	size := int(binary.BigEndian.Uint16(payload[2:4]))

	if size > len(payload)-1 || payload[1] == 0 || payload[1] > 32 {
		return nil
	}

	out := map[string]any{
		"version":        1,
		"message_type":   payload[1],
		"declared_bytes": size,
		"route":          payload[len(payload)-1],
	}

	return out
}

func (s *gamesyncServer) logDocument(doc *gs.Document, operation string) {
	if doc == nil || !strings.HasPrefix(doc.Name, "docs/") {
		return
	}

	s.p.event(map[string]any{
		"event":     "gamesync_protocol_document",
		"operation": operation,
		"path":      redactedDocumentPath(doc.Name),
		"fields":    summarizeFields(doc.Fields, 0),
	})
}

func identityDigest(value string) string {
	sum := sha256.Sum256([]byte(value))

	return fmt.Sprintf("%x", sum[:8])
}

// A successful stream send proves server transport submission, not client parsing.
func (s *gamesyncServer) logDelivery(sub *gsSubscriber, doc *gs.Document, operation string, err error) {
	event := map[string]any{
		"event":            "gamesync_document_delivery",
		"operation":        operation,
		"recipient_digest": identityDigest(sub.proof),
		"document_digest":  identityDigest(doc.GetName()),
		"path":             redactedDocumentPath(doc.GetName()),
		"submitted":        err == nil,
	}

	if err != nil {
		event["code"] = status.Code(err).String()
	}

	payload := doc.GetFields().GetFields()["pl"].GetBytesValue()

	if len(payload) != 0 {
		sum := sha256.Sum256(payload)
		event["payload_digest"] = fmt.Sprintf("%x", sum[:8])
		event["payload_bytes"] = len(payload)
	}

	s.p.event(event)
}
