package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/protobuf/proto"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

type replayGamesyncBody struct {
	io.Reader
	io.Closer
}

// Only the small unary bootstrap message is inspected. Streaming request bodies
// are never buffered, and the original bytes are replayed unchanged downstream.
func readGamesyncRoutingFrame(r *http.Request) ([]byte, error) {
	var header [5]byte

	if _, err := io.ReadFull(r.Body, header[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[1:])

	if header[0] != 0 || length > 64*1024 {
		return nil, fmt.Errorf("unsupported routing frame")
	}

	payload := make([]byte, length)

	if _, err := io.ReadFull(r.Body, payload); err != nil {
		return nil, err
	}

	frame := append(header[:], payload...)
	r.Body = &replayGamesyncBody{
		Reader: io.MultiReader(bytes.NewReader(frame), r.Body),
		Closer: r.Body,
	}

	return payload, nil
}

func gamesyncResourceTenant(resource string) string {
	parts := strings.Split(resource, "/")

	if len(parts) >= 2 && parts[0] == "tenants" && parts[1] != "current" {
		return parts[1]
	}

	return ""
}

// These claims select a backend only. They are not authentication: the selected
// server still verifies the signature, expiry and ownership of every token.
func gamesyncTokenRoutingTenant(token string) string {
	parts := strings.Split(strings.TrimSpace(token), ".")

	if len(parts) != 3 || len(token) > 8192 {
		return ""
	}

	payload, err := base64.RawURLEncoding.DecodeString(parts[1])

	if err != nil {
		return ""
	}

	var claims struct {
		Tenant string
		NPLN   struct {
			Tenant string `json:"tid"`
		} `json:"npln"`
		Game struct {
			Tenant string `json:"tid"`
		} `json:"game"`
	}

	if json.Unmarshal(payload, &claims) != nil {
		return ""
	}

	for _, tenant := range []string{
		claims.Tenant,
		claims.NPLN.Tenant,
		claims.Game.Tenant,
	} {
		if tenant != "" {
			return tenant
		}
	}

	return ""
}

func gamesyncRequestTenant(r *http.Request) (string, error) {
	tenants := r.Header.Values("npln-tenant-id")

	if len(tenants) > 1 {
		return "", fmt.Errorf("ambiguous tenant header")
	}

	if len(tenants) == 1 {
		return tenants[0], nil
	}

	const service = "/nn.npln.gamesync.v1.Gamesync/"

	if r.URL.Path == service+"IssueToken" || r.URL.Path == service+"RefreshToken" {
		payload, err := readGamesyncRoutingFrame(r)

		if err != nil {
			return "", err
		}

		var resource, token string

		if r.URL.Path == service+"IssueToken" {
			request := &gs.IssueTokenRequest{}

			if err := proto.Unmarshal(payload, request); err != nil {
				return "", err
			}

			resource, token = request.UserSession, request.MatchmakingIdToken
		} else {
			request := &gs.RefreshTokenRequest{}

			if err := proto.Unmarshal(payload, request); err != nil {
				return "", err
			}

			resource, token = request.UserSession, request.RefreshToken
		}

		resourceTenant := gamesyncResourceTenant(resource)
		tokenTenant := gamesyncTokenRoutingTenant(token)

		if resourceTenant != "" && tokenTenant != "" && resourceTenant != tokenTenant {
			return "", fmt.Errorf("conflicting routing tenants")
		}

		if resourceTenant != "" {
			return resourceTenant, nil
		}

		return tokenTenant, nil
	}

	authorization := r.Header.Values("Authorization")

	if len(authorization) != 1 {
		return "", fmt.Errorf("routing credential required")
	}

	token := strings.TrimSpace(authorization[0])

	if len(token) >= 7 && strings.EqualFold(token[:7], "Bearer ") {
		token = strings.TrimSpace(token[7:])
	}

	return gamesyncTokenRoutingTenant(token), nil
}
