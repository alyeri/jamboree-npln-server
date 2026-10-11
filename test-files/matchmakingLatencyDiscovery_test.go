package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestLatencyDiscoveryAuthenticationAndTenant(t *testing.T) {
	t.Setenv("NPLN_PUBLIC_IP", "192.0.2.10")

	key := bytes.Repeat([]byte{7}, 32)
	g := &gameSessionServer{
		p: &serverContext{
			identityKey: key,
			writer:      &bytes.Buffer{},
		},
	}
	r := &mm.ListLatencyMeasurementServersRequest{
		Parent: "tenants/current",
	}
	unauth := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1"))

	if _, err := g.ListLatencyMeasurementServers(unauth, r); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/a", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))

	for _, parent := range []string{"tenants/current", "tenants/t-adf89f68", "tenants/t-adf89f68-lp1"} {
		r.Parent = parent
		out, err := g.ListLatencyMeasurementServers(ctx, r)

		if err != nil || len(out.GetLatencyMeasurementServers()) != 1 {
			t.Fatal(err)
		}

		s := out.LatencyMeasurementServers[0]

		if s.Host != "192.0.2.10" || s.Port != 17480 || s.Protocol != mm.LatencyMeasurementServer_HTTP || s.Name != "tenants/t-adf89f68-lp1/latencyMeasurementServers/local" {
			t.Fatal("invalid endpoint")
		}
	}

	r.Parent = "tenants/other"

	if _, err := g.ListLatencyMeasurementServers(ctx, r); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}

	r.Parent = "tenants/current"
	r.PageToken = "unknown"

	if _, err := g.ListLatencyMeasurementServers(ctx, r); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func TestLatencyHTTPProbe(t *testing.T) {
	h := latencyHandler(&serverContext{
		writer: &bytes.Buffer{},
	})

	for _, method := range []string{
		http.MethodGet,
		http.MethodHead,
	} {
		w := httptest.NewRecorder()

		h.ServeHTTP(w, httptest.NewRequest(method, "http://localhost/probe", nil))

		if w.Code != 200 || w.Header().Get("Content-Length") != "2" {
			t.Fatal("invalid probe response")
		}

		if method == http.MethodGet && w.Body.String() != "OK" {
			t.Fatal("body missing")
		}

		if method == http.MethodHead && w.Body.Len() != 0 {
			t.Fatal("HEAD body")
		}
	}

	w := httptest.NewRecorder()

	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "http://localhost/", nil))

	if w.Code != 405 {
		t.Fatal("unexpected method accepted")
	}
}
