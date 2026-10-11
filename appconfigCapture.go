package main

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

const findApplicationConfigMethod = "/nn.npln.appconfig.v1.Appconfig/FindApplicationConfig"

// Decode structure only. Unknown byte/string values may contain credentials:
// retain lengths and hashes, not raw bytes. Only this game's tenant/config
// resource names are allowed in plaintext. Nested parsing is bounded.
func appconfigWireShape(data []byte, depth int) ([]map[string]any, bool) {
	if len(data) > 65536 || depth > 4 {
		return nil, false
	}

	fields := []map[string]any{}

	for len(data) > 0 {
		if len(fields) >= 128 {
			return fields, false
		}

		number, kind, used := protowire.ConsumeTag(data)

		if used < 0 || number < 1 {
			return fields, false
		}

		data = data[used:]
		field := map[string]any{
			"number":    int(number),
			"wire_type": int(kind),
		}

		switch kind {
		case protowire.VarintType:
			value, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fields, false
			}
			data = data[n:]
			field["uint64"] = strconv.FormatUint(value, 10)

		case protowire.Fixed32Type:
			value, n := protowire.ConsumeFixed32(data)
			if n < 0 {
				return fields, false
			}
			data = data[n:]
			field["fixed32"] = strconv.FormatUint(uint64(value), 10)

		case protowire.Fixed64Type:
			value, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return fields, false
			}
			data = data[n:]
			field["fixed64"] = strconv.FormatUint(value, 10)

		case protowire.BytesType:
			value, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return fields, false
			}
			data = data[n:]
			digest := sha256.Sum256(value)
			field["length"] = len(value)
			field["sha256"] = hex.EncodeToString(digest[:])
			if depth == 0 && number == 2 && string(value) == "Network" {
				field["selector"] = "Network"
			}

			if safeAppconfigResource(string(value)) {
				field["resource"] = string(value)
			}
			if len(value) > 0 && depth < 4 {
				if nested, ok := appconfigWireShape(value, depth+1); ok {
					field["nested_fields"] = nested
				}
			}

		default:
			return fields, false
		}

		fields = append(fields, field)
	}

	return fields, true
}

func safeAppconfigResource(value string) bool {
	if len(value) > 512 {
		return false
	}

	for _, tenant := range []string{"tenants/current", "tenants/t-adf89f68", "tenants/t-adf89f68-lp1"} {
		if value == tenant {
			return true
		}

		prefix := tenant + "/applicationConfigs/"

		if strings.HasPrefix(value, prefix) && len(value) > len(prefix) {
			for _, c := range value[len(prefix):] {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
					return false
				}
			}

			return true
		}
	}

	return false
}
