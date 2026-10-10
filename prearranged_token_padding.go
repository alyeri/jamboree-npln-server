package main

import (
	"bytes"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// Keep only recently accepted external tokens in memory. A repair must match
// an entire previously accepted token, including its original signature.
// Never infer the token length from its ASCII prefix or persist this cache.
type prearrangedTokenCache struct {
	mu     sync.Mutex
	tokens map[string]time.Time
}

func (c *prearrangedTokenCache) remember(token string) {
	if token == "" || len(token) > 8192 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for token, expiry := range c.tokens {
		if !expiry.After(now) {
			delete(c.tokens, token)
		}
	}

	if c.tokens == nil {
		c.tokens = map[string]time.Time{}
	}

	if len(c.tokens) < 64 || !c.tokens[token].IsZero() {
		c.tokens[token] = now.Add(15 * time.Minute)
	}
}

func (c *prearrangedTokenCache) original(value []byte) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	var match []byte
	for token, expiry := range c.tokens {
		if !expiry.After(now) {
			delete(c.tokens, token)
			continue
		}

		extra := len(value) - len(token)
		if extra < 1 || extra > 64 || !bytes.HasPrefix(value, []byte(token)) {
			continue
		}

		if match != nil {
			return nil
		}

		match = []byte(token)
	}

	return match
}

func (c *prearrangedTokenCache) repair(request []byte) ([]byte, int) {
	if len(request) > 65536 {
		return nil, 0
	}

	var tenant string
	var external []byte
	start, end := -1, -1
	for offset, fields := 0, 0; offset < len(request); fields++ {
		if fields >= 32 {
			return nil, 0
		}

		number, kind, tagSize := protowire.ConsumeTag(request[offset:])
		if tagSize < 0 {
			return nil, 0
		}

		valueStart := offset + tagSize
		size := protowire.ConsumeFieldValue(number, kind, request[valueStart:])
		if size < 0 {
			return nil, 0
		}

		if number == 1 || number == 3 {
			if kind != protowire.BytesType {
				return nil, 0
			}

			value, used := protowire.ConsumeBytes(request[valueStart:])
			if used < 0 {
				return nil, 0
			}

			if number == 1 {
				if tenant != "" || !utf8.Valid(value) {
					return nil, 0
				}
				tenant = string(value)
			} else {
				if start >= 0 {
					return nil, 0
				}
				external = value
				start, end = offset, valueStart+size
			}
		}

		offset = valueStart + size
	}

	if tenant != "tenants/current" && tenant != "tenants/t-adf89f68-lp1" && tenant != "tenants/t-adf89f68" {
		return nil, 0
	}

	number, kind, tagSize := protowire.ConsumeTag(external)
	if tagSize < 0 || number != 1 || kind != protowire.BytesType {
		return nil, 0
	}

	value, used := protowire.ConsumeBytes(external[tagSize:])
	if used < 0 || tagSize+used != len(external) || utf8.Valid(value) {
		return nil, 0
	}

	original := c.original(value)
	if original == nil || strings.Count(string(original), ".") != 2 {
		return nil, 0
	}

	nested := protowire.AppendTag(nil, 1, protowire.BytesType)
	nested = protowire.AppendBytes(nested, original)
	fixed := append([]byte(nil), request[:start]...)
	fixed = protowire.AppendTag(fixed, 3, protowire.BytesType)
	fixed = protowire.AppendBytes(fixed, nested)
	fixed = append(fixed, request[end:]...)

	return fixed, len(value) - len(original)
}
