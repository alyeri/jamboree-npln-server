package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Opt-in capture at gRPC's decrypted message boundary, limited to these clients.
// No headers, access tokens or authentication RPCs are written.
type rpcCapture struct {
	mu    sync.Mutex
	dir   string
	epoch string
	count int
}

// Preserve diagnostic fields even when a well-known type (for example a
// Timestamp) is accepted on the wire but cannot be represented by protojson.
// This reads a message and never changes what the handler receives.
func captureReflectValue(fd protoreflect.FieldDescriptor, value protoreflect.Value) any {
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return captureReflectFields(value.Message())

	case protoreflect.BytesKind:
		return base64.StdEncoding.EncodeToString(value.Bytes())

	case protoreflect.EnumKind:
		if e := fd.Enum().Values().ByNumber(value.Enum()); e != nil {
			return string(e.Name())
		}

		return int32(value.Enum())

	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(value.Int(), 10)

	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(value.Uint(), 10)

	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return strconv.FormatFloat(value.Float(), 'g', -1, 64)

	default:
		return value.Interface()
	}
}

func captureReflectFields(message protoreflect.Message) map[string]any {
	out := map[string]any{}
	message.Range(func(fd protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		var item any

		if fd.IsMap() {
			values := map[string]any{}
			value.Map().Range(func(key protoreflect.MapKey, v protoreflect.Value) bool {
				values[fmt.Sprint(key.Interface())] = captureReflectValue(fd.MapValue(), v)

				return true
			})
			item = values
		} else if fd.IsList() {
			values := []any{}

			for i := 0; i < value.List().Len(); i++ {
				values = append(values, captureReflectValue(fd, value.List().Get(i)))
			}

			item = values
		} else {
			item = captureReflectValue(fd, value)
		}

		out[string(fd.Name())] = item

		return true
	})

	return out
}

type rpcCaptureArm struct {
	ExpiresAt     time.Time `json:"expires_at"`
	CallerDigests []string  `json:"caller_digests"`
	MaxMessages   int       `json:"max_messages"`
}

func captureMethod(method string) bool {
	if method == "/nn.npln.auth.v1.UserService/QueryUserExternalIds" {
		return true
	}

	if strings.HasPrefix(method, "/nn.npln.leaderboard.v1.LeaderboardService/") {
		return true
	}

	switch method {
	case findApplicationConfigMethod:
		return true

	case "/nn.npln.matchmaking.v1.Matchmaker/CreateMatchmakingTicket":
		return true

	case "/nn.npln.matchmaking.v1.Matchmaker/TrackMatchmakingTicket", "/nn.npln.matchmaking.v1.Matchmaker/CancelMatchmakingTicket":
		return true

	case "/nn.npln.matchmaking.v1.GameSessionService/GetGameSession",
		"/nn.npln.matchmaking.v1.GameSessionService/ListLatencyMeasurementServers",
		"/nn.npln.matchmaking.v1.GameSessionService/JoinGameSession",
		"/nn.npln.matchmaking.v1.GameSessionService/QueryGameSessions",
		"/nn.npln.matchmaking.v1.GameSessionService/GetGameSessionShortAlias",
		"/nn.npln.matchmaking.v1.GameSessionService/CreateGameSessionShortAlias":
		return true

	case "/nn.npln.matchmaking.v1.GameSessionService/CreateGameSessionCreationTicket",
		"/nn.npln.matchmaking.v1.GameSessionService/TrackGameSessionCreationTicket",
		"/nn.npln.matchmaking.v1.GameSessionService/BatchGetGameSessions",
		"/nn.npln.friends.v1.Friends/SubscribeFriendUsers",
		"/nn.npln.friends.v1.Friends/ListFriendUsers":
		return true

	case "/nn.npln.gamesync.v1.Gamesync/GetDocument",
		"/nn.npln.gamesync.v1.Gamesync/ListDocuments",
		"/nn.npln.gamesync.v1.Gamesync/WriteDocuments":
		return true
	}

	return false
}

func redactCapture(value any) {
	switch v := value.(type) {
	case map[string]any:
		for key, item := range v {
			name := strings.ToLower(key)

			if strings.Contains(name, "token") || strings.Contains(name, "password") || name == "pw" || name == "bytes_value" {
				if text, ok := item.(string); !ok || text != "" {
					v[key] = "<redacted>"
				}
			} else {
				redactCapture(item)
			}
		}

	case []any:
		for _, item := range v {
			redactCapture(item)
		}
	}
}

func captureCaller(ctx context.Context, method string, key []byte) (string, error) {
	if !strings.HasPrefix(method, "/nn.npln.gamesync.v1.Gamesync/") {
		return callerIdentity(ctx, key)
	}

	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")

	if len(values) != 1 {
		return callerIdentity(ctx, key)
	}

	raw := strings.TrimSpace(values[0])

	if len(raw) >= 7 && strings.EqualFold(raw[:7], "Bearer ") {
		raw = strings.TrimSpace(raw[7:])
	}

	proof, err := verifySessionProof(raw, key, "gamesync-access")
	if err != nil {
		return "", err
	}

	return strings.SplitN(proof, "|", 2)[0], nil
}

func (p *serverContext) captureRPC(ctx context.Context, rpc *rpcState, direction string, payload any, code string) {
	if p.capture == nil || !captureMethod(rpc.method) {
		return
	}

	owner, err := captureCaller(ctx, rpc.method, p.identityKey)
	if err != nil {
		return
	}

	c := p.capture
	c.mu.Lock()
	defer c.mu.Unlock()

	armData, err := os.ReadFile(filepath.Join(c.dir, "arm.json"))
	if err != nil || len(armData) > 4096 {
		return
	}

	var arm rpcCaptureArm

	if json.Unmarshal(armData, &arm) != nil || !arm.ExpiresAt.After(time.Now()) || arm.MaxMessages < 1 || arm.MaxMessages > 256 {
		return
	}

	digest := identityDigest(owner)
	allowed := false

	for _, candidate := range arm.CallerDigests {
		if candidate == digest {
			allowed = true
		}
	}

	if !allowed {
		return
	}

	epoch := arm.ExpiresAt.UTC().Format("20060102T150405.000000000Z")

	if c.epoch != epoch {
		c.epoch, c.count = epoch, 0
	}

	if c.count >= arm.MaxMessages {
		return
	}

	record := map[string]any{
		"time":          time.Now().UTC().Format(time.RFC3339Nano),
		"build":         serverVersion,
		"rpc_id":        rpc.id,
		"method":        rpc.method,
		"direction":     direction,
		"caller_digest": digest,
	}

	if payload != nil {
		message, ok := payload.(proto.Message)

		if !ok || proto.Size(message) > 1<<20 {
			return
		}

		data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(message)

		var fields any

		if err != nil {
			fields = captureReflectFields(message.ProtoReflect())
			record["serialization_mode"] = "protobuf_fields"
		} else {
			if json.Unmarshal(data, &fields) != nil {
				return
			}

			record["serialization_mode"] = "protojson"
		}

		redactCapture(fields)
		record["message_type"] = string(message.ProtoReflect().Descriptor().FullName())
		record["fields"] = fields
		record["unknown_bytes"] = len(message.ProtoReflect().GetUnknown())

		if rpc.method == findApplicationConfigMethod {
			record["serialization_mode"] = "protobuf_wire_shape"
			shape, valid := appconfigWireShape(message.ProtoReflect().GetUnknown(), 0)
			record["wire_fields"] = shape
			record["wire_valid"] = valid
		}
	}

	if code != "" {
		record["grpc_status"] = code
	}

	file, err := os.OpenFile(filepath.Join(c.dir, "capture-"+epoch+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		p.event(map[string]any{
			"event": "npln_private_capture_unavailable",
		})

		return
	}

	err = json.NewEncoder(file).Encode(record)
	_ = file.Close()

	if err != nil {
		p.event(map[string]any{
			"event": "npln_private_capture_unavailable",
		})

		return
	}

	c.count++
	p.event(map[string]any{
		"event":          "npln_private_capture",
		"rpc_id":         rpc.id,
		"method":         rpc.method,
		"direction":      direction,
		"capture_number": c.count,
	})
}
