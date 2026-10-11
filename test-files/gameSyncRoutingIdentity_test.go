package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

func TestNativeGamesyncBootstrapWithoutTenantHeader(t *testing.T) {
	token, err := (&authServer{
		key: bytes.Repeat([]byte{7}, 32),
	}).signSessionToken("guest|session", "matchmaking", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	payload, _ := proto.Marshal(&gs.IssueTokenRequest{
		UserSession:        "tenants/t-adf89f68-lp1/gameSessions/room/userSessions/guest",
		MatchmakingIdToken: token,
	})
	frame := make([]byte, 5)

	binary.BigEndian.PutUint32(frame[1:], uint32(len(payload)))

	frame = append(frame, payload...)
	request := httptest.NewRequest(http.MethodPost, "/nn.npln.gamesync.v1.Gamesync/IssueToken", bytes.NewReader(frame))
	request.ProtoMajor = 2

	request.Header.Set("Content-Type", "application/grpc")

	called := false
	router := gamesyncTenantRouter(map[string]gamesyncRoute{
		"t-adf89f68-lp1": {
			label: "jamboree",
			proxy: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				got, _ := io.ReadAll(r.Body)

				if !bytes.Equal(got, frame) {
					t.Fatal("bootstrap frame changed")
				}
			}),
		},
	}, &serverContext{
		writer: &bytes.Buffer{},
	})

	router.ServeHTTP(httptest.NewRecorder(), request)

	if !called {
		t.Fatal("native bootstrap was not routed")
	}
}

func TestNativeGamesyncStreamUsesTokenTenantWithoutReadingBody(t *testing.T) {
	for _, claims := range []struct {
		payload string
		tenant  string
	}{
		{
			`{"Tenant":"t-adf89f68-lp1"}`,
			"t-adf89f68-lp1",
		},
		{
			`{"npln":{"tid":"t-ba973ec6-lp1"}}`,
			"t-ba973ec6-lp1",
		},
		{
			`{"game":{"tid":"t-ba973ec6-lp1"}}`,
			"t-ba973ec6-lp1",
		},
	} {
		reader, writer := io.Pipe()
		request := httptest.NewRequest(http.MethodPost, "/nn.npln.gamesync.v1.Gamesync/KeepUserSession", reader)

		request.Header.Set("Authorization", "Bearer header."+base64.RawURLEncoding.EncodeToString([]byte(claims.payload))+".signature")

		got, err := gamesyncRequestTenant(request)

		reader.Close()

		writer.Close()

		if err != nil || got != claims.tenant {
			t.Fatalf("native stream tenant missing: %v", err)
		}
	}
}
