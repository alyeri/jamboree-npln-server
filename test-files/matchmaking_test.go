package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	gspb "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mmpb "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestFourPlayerRoomTicketAndOwnership(t *testing.T) {
	var logs bytes.Buffer

	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		writer:      &logs,
		identityKey: key,
	}
	lis := bufconn.Listen(1 << 20)
	s := newServer(p)

	go s.Serve(lis)
	defer s.Stop()

	c, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/host", "access", time.Minute)
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", token))
	client := mmpb.NewGameSessionServiceClient(c)
	latency := &mmpb.LatencyData{
		Latencies: map[string]*durationpb.Duration{
			"test-region": {
				Seconds: 1,
				Nanos:   20_000_000,
			},
		},
	}
	ticket, err := client.CreateGameSessionCreationTicket(ctx, &mmpb.CreateGameSessionCreationTicketRequest{
		Parent: "tenants/current",
		GameSessionCreationTicket: &mmpb.GameSessionCreationTicket{
			GameSession: &mmpb.GameSession{
				MaxParticipantCount: 4,
				Password:            "1234",
			},
			UserDefinitions: []*mmpb.UserDefinition{
				{
					LatencyData: latency,
				},
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	alias := strings.Replace(ticket.Name, "tenants/t-adf89f68-lp1/", "tenants/current/", 1)
	stream, err := client.TrackGameSessionCreationTicket(ctx, &mmpb.TrackGameSessionCreationTicketRequest{
		Name: alias,
	})

	if err != nil {
		t.Fatal(err)
	}

	done, err := stream.Recv()

	if done.GetGameSession().GetHost() != "t-adf89f68-lp1.lp1.t.npln.srv.nintendo.net" || done.GetGameSession().GetPort() != 443 {
		t.Fatal("assigned session has no reachable TLS destination")
	}

	if err != nil || done.GetState() != mmpb.GameSessionCreationTicket_SUCCEEDED || done.GetGameSession().GetMaxParticipantCount() != 4 || done.GetGameSession().GetPassword() != "1234" || len(done.MatchedUserSessions) != 1 || done.MatchedUserSessions[0].MatchmakingIdToken == "" {
		t.Fatal("invalid assigned room", err)
	}

	creatorSession := done.MatchedUserSessions[0].UserSession
	creatorID := creatorSession[strings.LastIndex(creatorSession, "/")+1:]

	if decoded, e := hex.DecodeString(creatorID); e != nil || len(decoded) != 16 {
		t.Fatal("creator participant identifier is not 128-bit hexadecimal")
	}

	short, aliasErr := client.CreateGameSessionShortAlias(ctx, &mmpb.CreateGameSessionShortAliasRequest{
		Parent: "tenants/current",
		GameSessionShortAlias: &mmpb.GameSessionShortAlias{
			GameSession: done.GetGameSession().GetName(),
		},
	})

	if aliasErr != nil || short.GetName() == "" || short.GetGameSession() != done.GetGameSession().GetName() {
		t.Fatal("alias creation failed", aliasErr)
	}

	code := strings.Split(short.GetName(), "/")
	id := code[len(code)-1]

	if len(id) != 6 {
		t.Fatal("room code length")
	}

	for _, c := range id {
		if !(c >= 'A' && c <= 'Z' || c >= '2' && c <= '9') {
			t.Fatal("room code alphabet")
		}
	}

	stored, getErr := client.GetGameSession(ctx, &mmpb.GetGameSessionRequest{
		Name: done.GetGameSession().GetName(),
	})

	if getErr != nil || stored.GetPassword() != "1234" {
		t.Fatal("private room password lost", getErr)
	}

	if stored.GetState() != mmpb.GameSession_CREATING || stored.GetUserSessions()[0].GetState() != mmpb.UserSession_RESERVED {
		t.Fatal("unconnected session must remain reserved")
	}

	if !proto.Equal(stored.UserSessions[0].LatencyData, latency) {
		t.Fatal("creator latency lost between creation and session read")
	}
	// Jamboree 2.3.0's parser at 0xdeb140 compares this segment case-sensitively.
	aliasParts := strings.Split(short.GetName(), "/")

	if len(aliasParts) != 4 || aliasParts[0] != "tenants" || aliasParts[2] != "GameSessionShortAliases" || aliasParts[3] == "" {
		t.Fatal("alias violates the game resource parser")
	}

	repeated, aliasErr := client.CreateGameSessionShortAlias(ctx, &mmpb.CreateGameSessionShortAliasRequest{
		Parent: "tenants/current",
		GameSessionShortAlias: &mmpb.GameSessionShortAlias{
			GameSession: done.GetGameSession().GetName(),
		},
	})

	if aliasErr != nil || repeated.GetName() != short.GetName() {
		t.Fatal("alias not stable", aliasErr)
	}

	lookup, aliasErr := client.GetGameSessionShortAlias(ctx, &mmpb.GetGameSessionShortAliasRequest{
		Name: strings.Replace(short.GetName(), "tenants/t-adf89f68-lp1/", "tenants/current/", 1),
	})

	if aliasErr != nil || lookup.GetGameSession() != short.GetGameSession() {
		t.Fatal("alias resolution failed", aliasErr)
	}

	other, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/other", "access", time.Minute)
	matched := done.MatchedUserSessions[0]
	gsToken, gsErr := gspb.NewGamesyncClient(c).IssueToken(ctx, &gspb.IssueTokenRequest{
		UserSession:        matched.UserSession,
		MatchmakingIdToken: matched.MatchmakingIdToken,
	})

	if gsErr != nil {
		t.Fatal(gsErr)
	}

	gsContext := metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", gsToken.Token.AccessToken))
	keep, keepErr := gspb.NewGamesyncClient(c).KeepUserSession(gsContext)

	if keepErr != nil {
		t.Fatal(keepErr)
	}

	for _, echo := range []string{"first", "second"} {
		if err = keep.Send(&gspb.KeepUserSessionRequest{
			Name: "userSessions/current",
			RequestType: &gspb.KeepUserSessionRequest_Echo{
				Echo: echo,
			},
		}); err != nil {
			t.Fatal(err)
		}

		r, e := keep.Recv()

		if e != nil || r.GetEcho() != echo {
			t.Fatal("session echo failed", e)
		}
	}

	targetName := "userSessions/current/targets/test"
	active, activeErr := client.GetGameSession(ctx, &mmpb.GetGameSessionRequest{
		Name: done.GetGameSession().GetName(),
	})

	if activeErr != nil || active.GetState() != mmpb.GameSession_ACTIVE || active.GetUserSessions()[0].GetState() != mmpb.UserSession_ACTIVE || active.GetPassword() != "1234" {
		t.Fatal("authenticated Gamesync did not activate the reserved room", activeErr)
	}

	if err = keep.Send(&gspb.KeepUserSessionRequest{
		Name: "userSessions/current",
		RequestType: &gspb.KeepUserSessionRequest_UpdateTarget{
			UpdateTarget: &gspb.UpdateTargetRequest{
				Target: &gspb.Target{
					Name: targetName,
					TargetType: &gspb.Target_Documents{
						Documents: &gspb.DocumentsTarget{},
					},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	for _, kind := range []gspb.TargetChange_TargetChangeType{
		gspb.TargetChange_UPDATED,
		gspb.TargetChange_LISTED,
	} {
		r, e := keep.Recv()

		if e != nil || r.GetTargetChange().GetTargetId() != "test" || r.GetTargetChange().GetTargetChangeType() != kind {
			t.Fatal("initial target snapshot failed", e)
		}
	}

	if err = keep.Send(&gspb.KeepUserSessionRequest{
		Name: "userSessions/current",
		RequestType: &gspb.KeepUserSessionRequest_DeleteTarget{
			DeleteTarget: &gspb.DeleteTargetRequest{
				Name: targetName,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	deleted, e := keep.Recv()

	if e != nil || deleted.GetTargetChange().GetTargetChangeType() != gspb.TargetChange_DELETED {
		t.Fatal("target removal failed", e)
	}

	keep.CloseSend()

	systemKeep, systemErr := gspb.NewGamesyncClient(c).KeepUserSession(gsContext)

	if systemErr != nil {
		t.Fatal(systemErr)
	}

	if err = systemKeep.Send(&gspb.KeepUserSessionRequest{
		Name: "userSessions/current",
		RequestType: &gspb.KeepUserSessionRequest_UpdateTarget{
			UpdateTarget: &gspb.UpdateTargetRequest{
				Target: &gspb.Target{
					Name: "targets/fixed",
					TargetType: &gspb.Target_Documents{
						Documents: &gspb.DocumentsTarget{
							Documents: []string{"docs/__gs/f"},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	updated, e := systemKeep.Recv()

	if e != nil || updated.GetTargetChange().GetTargetChangeType() != gspb.TargetChange_UPDATED {
		t.Fatal(e)
	}

	initial, e := systemKeep.Recv()

	if e != nil || initial.GetDocumentChange().GetDocument().GetName() != "docs/__gs/f" {
		t.Fatal("missing session system document", e)
	}

	fields := initial.GetDocumentChange().GetDocument().GetFields().GetFields()

	if fields["maxu"].GetIntegerValue() != 4 || fields["p"].GetIntegerValue() != 443 || len(fields["rs"].GetBytesValue()) != 16 || fields["addr"].GetStringValue() == "" {
		t.Fatal("invalid fixed session fields")
	}

	listed, e := systemKeep.Recv()

	if e != nil || listed.GetTargetChange().GetTargetChangeType() != gspb.TargetChange_LISTED {
		t.Fatal(e)
	}

	if err = systemKeep.Send(&gspb.KeepUserSessionRequest{
		Name: "userSessions/current",
		RequestType: &gspb.KeepUserSessionRequest_UpdateTarget{
			UpdateTarget: &gspb.UpdateTargetRequest{
				Target: &gspb.Target{
					Name: "targets/mutable",
					TargetType: &gspb.Target_Documents{
						Documents: &gspb.DocumentsTarget{
							Documents: []string{"docs/__gs/s"},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	updated, e = systemKeep.Recv()

	if e != nil || updated.GetTargetChange().GetTargetChangeType() != gspb.TargetChange_UPDATED {
		t.Fatal("mutable target not registered", e)
	}

	initial, e = systemKeep.Recv()

	if e != nil || initial.GetDocumentChange().GetDocument().GetName() != "docs/__gs/s" {
		t.Fatal("missing mutable session snapshot", e)
	}

	mutable := initial.GetDocumentChange().GetDocument().GetFields().GetFields()

	if mutable["pw"].GetStringValue() != "1234" || !mutable["cp"].GetBooleanValue() || mutable["ip"].GetBooleanValue() || mutable["prp"].GetMapValue() == nil {
		t.Fatal("mutable snapshot does not match private password-protected room")
	}

	listed, e = systemKeep.Recv()

	if e != nil || listed.GetTargetChange().GetTargetChangeType() != gspb.TargetChange_LISTED {
		t.Fatal("mutable snapshot not completed", e)
	}

	systemKeep.CloseSend()

	foreign := metadata.NewOutgoingContext(ctx, metadata.Pairs("npln-tenant-id", "t-adf89f68-lp1", "authorization", other))
	stream, err = client.TrackGameSessionCreationTicket(foreign, &mmpb.TrackGameSessionCreationTicketRequest{
		Name: ticket.Name,
	})

	if err == nil {
		_, err = stream.Recv()
	}

	if status.Code(err) != codes.PermissionDenied {
		t.Fatal("foreign caller admitted", err)
	}

	if bytes.Contains(logs.Bytes(), []byte("1234")) {
		t.Fatal("password logged")
	}
}

func TestRoomCapacityByMode(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	s := &gameSessionServer{
		p: &serverContext{
			writer:      &bytes.Buffer{},
			identityKey: key,
		},
		tickets: map[string]*creationRecord{},
	}
	token, _ := (&authServer{
		key: key,
	}).signSessionToken("tenants/current/users/host", "access", time.Minute)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", token, "npln-tenant-id", "t-adf89f68-lp1"))

	for _, tc := range []struct {
		config          string
		requested, want int32
	}{
		{
			"friend_room4",
			0,
			4,
		},
		{
			"friend_room8",
			0,
			8,
		},
		{
			"tenants/current/matchmakingConfigs/BqP8MatchmakingConfigs",
			0,
			8,
		},
		{
			"BqP4MatchmakingConfigs",
			0,
			4,
		},
		{
			"friend_room8",
			8,
			8,
		},
		{
			"friend_room4",
			4,
			4,
		},
		{
			"",
			9,
			0,
		},
	} {
		r, e := s.CreateGameSessionCreationTicket(ctx, &mmpb.CreateGameSessionCreationTicketRequest{
			Parent: "tenants/current",
			GameSessionCreationTicket: &mmpb.GameSessionCreationTicket{
				MatchmakingConfig: tc.config,
				GameSession: &mmpb.GameSession{
					MaxParticipantCount: tc.requested,
				},
			},
		})

		if tc.want == 0 {
			if status.Code(e) != codes.InvalidArgument {
				t.Fatal("invalid limit accepted", e)
			}

			continue
		}

		if e != nil || r.GetGameSession().GetMaxParticipantCount() != tc.want || r.GetGameSession().GetPassword() != "" {
			t.Fatal("incorrect mode capacity", tc, e)
		}
	}
}
