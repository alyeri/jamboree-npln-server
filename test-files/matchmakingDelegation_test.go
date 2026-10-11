package main

import (
	"bytes"
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func TestParticipationDelegationRestrictsCallerAndExpires(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	p := &serverContext{
		identityKey: key,
		writer:      &bytes.Buffer{},
	}
	rooms := &gameSessionServer{
		p:       p,
		tickets: map[string]*creationRecord{},
	}
	leader := "tenants/t-adf89f68-lp1/users/leader"
	guest := "tenants/t-adf89f68-lp1/users/guest"

	token, err := (&authServer{
		key: key,
	}).signSessionToken(guest, "access", time.Minute)

	if err != nil {
		t.Fatal(err)
	}

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"npln-tenant-id", "t-adf89f68-lp1",
		"authorization", token,
	))

	grant, err := rooms.IssueUserDelegationToken(ctx, &mm.IssueUserDelegationTokenRequest{
		Parent:        "tenants/current",
		DelegatorUser: "users/current",
		MandataryUser: leader,
		DelegationActions: []mm.DelegationAction{
			mm.DelegationAction_ANY_PARTICIPATION,
		},
		Attributes: &common.MapValue{
			Fields: map[string]*common.Value{
				"gameMode": intValue(33796),
			},
		},
	})

	if err != nil {
		t.Fatal(err)
	}

	delegation := grant.UserDelegationDetail.UserDelegationToken
	definitions := []*mm.UserDefinition{
		{
			User: leader,
		},
		{
			User: guest,
		},
	}

	authorized, err := rooms.authorizeSearchUsers(leader, definitions, []string{
		delegation,
	})

	if err != nil {
		t.Fatal(err)
	}

	if authorized[1].Attributes.Fields["gameMode"].GetIntegerValue() != 33796 || definitions[1].Attributes != nil {
		t.Fatal("delegated attributes missing or request modified")
	}

	for _, caller := range []string{
		guest,
		"tenants/t-adf89f68-lp1/users/stranger",
	} {
		if _, err := rooms.delegatedParticipants(caller, []string{
			delegation,
		}); status.Code(err) != codes.PermissionDenied {
			t.Fatal("grant accepted for another caller", err)
		}
	}

	if _, err := rooms.authorizeSearchUsers(leader, definitions, nil); status.Code(err) != codes.PermissionDenied {
		t.Fatal("guest accepted without delegation", err)
	}

	if _, err := rooms.delegatedParticipants(leader, []string{
		delegation,
		delegation,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("duplicate grant accepted", err)
	}

	rooms.delegations[delegation].expires = time.Now().Add(-time.Second)

	if _, err := rooms.delegatedParticipants(leader, []string{
		delegation,
	}); status.Code(err) != codes.PermissionDenied {
		t.Fatal("expired grant accepted", err)
	}
}

func TestTagMatchRequiresOpposingPairAndKeepsProofsPrivate(t *testing.T) {
	p := &serverContext{
		identityKey: bytes.Repeat([]byte{7}, 32),
		writer:      &bytes.Buffer{},
	}
	rooms := &gameSessionServer{
		p:       p,
		tickets: map[string]*creationRecord{},
	}
	matcher := &matchmakerServer{
		rooms:   rooms,
		tickets: map[string]*searchRecord{},
	}

	for _, pair := range []string{"first", "second"} {
		leader := "tenants/t-adf89f68-lp1/users/" + pair
		guest := leader + "Guest"
		ticket := &mm.MatchmakingTicket{
			Name:              pair,
			State:             mm.MatchmakingTicket_SEARCHING,
			MatchmakingConfig: "tenants/t-adf89f68-lp1/matchmakingConfigs/BqP4NoEasePaticipantConfigs",
			UserDefinitions: []*mm.UserDefinition{
				{
					User: leader,
					Attributes: &common.MapValue{
						Fields: map[string]*common.Value{
							"gameMode": intValue(33796),
						},
					},
				},
				{
					User: guest,
					Attributes: &common.MapValue{
						Fields: map[string]*common.Value{
							"gameMode": intValue(33796),
						},
					},
				},
			},
		}

		matcher.tickets[pair] = &searchRecord{
			owner:    leader,
			ticket:   ticket,
			tracking: true,
			created:  time.Now().Add(-matchmakingSearchWindow),
		}

		if err := matcher.assignGroup(matcher.tickets["first"]); err != nil {
			t.Fatal(err)
		}

		if pair == "first" && len(rooms.tickets) != 0 {
			t.Fatal("lone pair matched against itself")
		}
	}

	if len(rooms.tickets) != 1 {
		t.Fatal("two pairs did not create one room")
	}

	for _, record := range matcher.tickets {
		if record.ticket.State != mm.MatchmakingTicket_SUCCEEDED || record.ticket.GameSession.CurrentParticipantCount != 4 {
			t.Fatal("incorrect Tag Match assignment")
		}

		for _, matched := range record.ticket.MatchedUserSessions {
			if matched.UserDefinition.User != record.owner {
				if matched.MatchmakingIdToken != "" {
					t.Fatal("teammate credential disclosed to leader")
				}

				continue
			}

			proof, err := verifySessionProof(matched.MatchmakingIdToken, p.identityKey, "matchmaking")

			if err != nil || proof != record.owner+"|"+matched.UserSession {
				t.Fatal("incorrect leader session proof", err)
			}
		}
	}
}
