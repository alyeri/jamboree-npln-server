package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestPublicServerIPConfiguration(t *testing.T) {
	for _, host := range []string{
		"",
		"not-an-ip",
		"0.0.0.0",
		"::",
		"224.0.0.1",
		"ff02::1",
	} {
		t.Setenv("NPLN_PUBLIC_IP", host)

		if _, err := publicServerIP(); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("invalid address %q accepted: %v", host, err)
		}
	}

	for _, host := range []string{"192.0.2.10", "2001:db8::10"} {
		t.Setenv("NPLN_PUBLIC_IP", " "+host+" ")

		got, err := publicServerIP()

		if err != nil || got != host {
			t.Fatalf("configured address %q: got %q, error %v", host, got, err)
		}
	}
}

func TestDiscoveryUsesConfiguredServerIP(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	s := &gameSessionServer{
		p: &serverContext{
			identityKey: key,
			writer:      &bytes.Buffer{},
		},
	}
	token, err := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/a", "access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	iceRequest := &mm.AllocateIceServerSetRequest{
		Tenant: "tenants/current",
	}
	latencyRequest := &mm.ListLatencyMeasurementServersRequest{
		Parent: "tenants/current",
	}

	for _, host := range []string{"192.0.2.10", "2001:db8::10"} {
		t.Setenv("NPLN_PUBLIC_IP", host)

		ice, err := s.AllocateIceServerSet(ctx, iceRequest)

		if err != nil {
			t.Fatal(err)
		}

		if ice.StunServer.Host != host || len(ice.TurnServers) != 1 || ice.TurnServers[0].Host != host {
			t.Fatal("ICE servers do not use the configured address")
		}

		if ice.StunServer.Port != 17478 || ice.TurnServers[0].Port != 17479 || ice.TurnServers[0].Username == "" || ice.TurnServers[0].Password == "" {
			t.Fatal("ICE ports or credentials changed")
		}

		latency, err := s.ListLatencyMeasurementServers(ctx, latencyRequest)

		if err != nil || len(latency.GetLatencyMeasurementServers()) != 1 || latency.LatencyMeasurementServers[0].Host != host {
			t.Fatalf("latency server does not use the configured address: %v", err)
		}
	}

	t.Setenv("NPLN_PUBLIC_IP", "")

	if _, err := s.AllocateIceServerSet(ctx, iceRequest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ICE accepted missing configuration: %v", err)
	}

	if _, err := s.ListLatencyMeasurementServers(ctx, latencyRequest); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("latency discovery accepted missing configuration: %v", err)
	}
}
