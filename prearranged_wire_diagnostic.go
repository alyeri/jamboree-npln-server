package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	authpb "github.com/alyeri/jamboree-npln-server/proto/auth/v1"
)

// Keep protobuf behavior unchanged. Inspect only field numbers, wire types and sizes
// when this particular request cannot be decoded; never retain authentication bytes.
type prearrangedDiagnosticCodec struct {
	p     *serverContext
	count atomic.Uint64
}

func (*prearrangedDiagnosticCodec) Name() string {
	return "proto"
}

func (*prearrangedDiagnosticCodec) Marshal(v any) ([]byte, error) {
	return proto.Marshal(v.(proto.Message))
}

func (c *prearrangedDiagnosticCodec) Unmarshal(b []byte, v any) error {
	err := proto.Unmarshal(b, v.(proto.Message))

	if _, ok := v.(*authpb.IssuePrearrangedUserTokenRequest); ok && err != nil && c.armed() && c.count.Add(1) <= 256 {
		c.p.event(map[string]any{
			"event":  "prearranged_decode_shape",
			"bytes":  len(b),
			"fields": wireShape(b, 0),
		})
	}

	return err
}

func (c *prearrangedDiagnosticCodec) armed() bool {
	if c.p.capture == nil {
		return false
	}

	b, err := os.ReadFile(filepath.Join(c.p.capture.dir, "arm.json"))

	if err != nil || len(b) > 4096 {
		return false
	}

	var arm rpcCaptureArm

	return json.Unmarshal(b, &arm) == nil && arm.ExpiresAt.After(time.Now()) && arm.ExpiresAt.Before(time.Now().Add(time.Hour))
}

func wireShape(b []byte, depth int) []map[string]any {
	out := []map[string]any{}
	for len(b) > 0 && len(out) < 32 {
		num, typ, n := protowire.ConsumeTag(b)

		if n < 0 {
			return append(out, map[string]any{"invalid_tag": true})
		}

		b = b[n:]
		size := protowire.ConsumeFieldValue(num, typ, b)

		if size < 0 {
			return append(out, map[string]any{
				"number":        int(num),
				"wire_type":     int(typ),
				"invalid_value": true,
			})
		}

		f := map[string]any{
			"number":       int(num),
			"wire_type":    int(typ),
			"encoded_size": size,
		}

		if typ == protowire.BytesType {
			value, _ := protowire.ConsumeBytes(b)
			f["length"] = len(value)

			if depth == 0 && num == 3 {
				f["nested_fields"] = wireShape(value, depth+1)
			}
		}

		out = append(out, f)
		b = b[size:]
	}

	return out
}

func prearrangedTransportErrorKind(err error) string {
	msg := status.Convert(err).Message()
	switch {
	case strings.Contains(msg, "unmarshalling"), strings.Contains(msg, "unmarshal"):
		return "protobuf_decode"
	case strings.Contains(msg, "decompress"), strings.Contains(msg, "Decompress"), strings.Contains(msg, "grpc-encoding"):
		return "compression"
	case strings.Contains(msg, "message larger"), strings.Contains(msg, "message too large"):
		return "message_size"
	case strings.Contains(msg, "unexpected EOF"):
		return "incomplete_message"
	default:
		return "other"
	}
}
