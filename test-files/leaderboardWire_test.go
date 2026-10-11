package main

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	lb "github.com/alyeri/jamboree-npln-server/proto/leaderboard/v1"
)

func TestJamboreeLeaderboardWrappedWireResponse(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	rankings, err := newLeaderboardServer(&serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}, "")

	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()

	lb.RegisterLeaderboardServiceServer(server, rankings)

	go server.Serve(listener)
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///rankings", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}))

	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/a", "access", time.Minute)
	md := metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	incoming := metadata.NewIncomingContext(context.Background(), md)
	client := lb.NewLeaderboardServiceClient(conn)
	holder, err := client.UpdateHolder(ctx, &lb.UpdateHolderRequest{
		Holder: &lb.Holder{
			Name: "tenants/current/holders/default",
		},
	})

	if err != nil {
		t.Fatal("other methods changed", err)
	}

	season := "tenants/current/categories/Mode_00000002/seasons/current"

	for _, withScore := range []bool{
		false,
		true,
	} {
		if withScore {
			if _, err := rankings.RegisterScore(incoming, &lb.RegisterScoreRequest{
				Score: &lb.Score{
					Name:   season + "/scoreHolders/default",
					Holder: holder.Name,
					Score:  10,
				},
			}); err != nil {
				t.Fatal(err)
			}
		}

		for _, req := range []*lb.BrowseLeaderboardRequest{
			{
				Season: season,
				Filter: &lb.BrowseLeaderboardRequest_RangeLeaderboard{
					RangeLeaderboard: &lb.RangeLeaderboardFilter{
						PageSize: 30,
					},
				},
			},
			{
				Season: season,
				Filter: &lb.BrowseLeaderboardRequest_NearLeaderboard{
					NearLeaderboard: &lb.NearLeaderboardFilter{
						TargetHolder: holder.Name,
						PageSize:     1,
					},
				},
			},
		} {
			// Native root parser e77f90 decodes field 1 as a nested Leaderboard;
			// e726e0 parses the child, not the RPC response itself.
			response, err := client.BrowseLeaderboard(ctx, req)

			if err != nil {
				t.Fatal(err)
			}

			got := response.GetLeaderboard()

			if got == nil {
				t.Fatal("missing nested leaderboard")
			}

			want := 0

			if withScore {
				want = 1
			}

			if got.Season != "tenants/t-adf89f68-lp1/categories/Mode_00000002/seasons/1" || len(got.Scores) != want || got.TotalCount != int32(want) {
				t.Fatal("wrong wrapped wire response")
			}
		}
	}
}
