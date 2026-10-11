package main

import (
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	common "github.com/alyeri/jamboree-npln-server/proto/common"
	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

// Caller holds both Gamesync and room locks. This permits only republication
// of the game's host marker after the original owner's stream has closed;
// it does not transfer server ownership or permission over admission/password.
// The owner may reopen admission immediately before leaving. Existing admission
// flags do not grant or revoke permission to update only the host marker.
func (s *gamesyncServer) postmatchHostPropertiesUpdate(record *creationRecord, proof string, old *gs.Document, op *gs.WriteOperation) bool {
	room := record.ticket.GetGameSession()

	if room.GetState() != mm.GameSession_ACTIVE || !record.expires.After(time.Now()) {
		return false
	}

	switch matchmakingConfigID(record.ticket.GetMatchmakingConfig()) {
	case "BqP4MatchmakingConfigs", "BqP4NoEasePaticipantConfigs":

	default:
		return false
	}

	fields := old.GetFields().GetFields()

	for _, key := range []string{"cp", "ip"} {
		_, ok := fields[key].GetValueType().(*common.Value_BooleanValue)

		if !ok {
			return false
		}
	}

	parts := strings.Split(proof, "|")

	if len(parts) != 2 || parts[0] == record.owner {
		return false
	}

	participant, ownerPreviouslyConnected := false, false

	for _, user := range room.GetUserSessions() {
		participant = participant || (user.User == parts[0] && user.Name == parts[1])

		if user.User == record.owner && s.epochs[user.User+"|"+user.Name] > 0 {
			ownerPreviouslyConnected = true
		}
	}

	if !participant || !ownerPreviouslyConnected {
		return false
	}

	callerConnected := false

	for sub, live := range s.subs[room.GetName()] {
		if !live || sub == nil {
			continue
		}

		identity := strings.Split(sub.proof, "|")

		if len(identity) == 2 && identity[0] == record.owner {
			return false
		}

		callerConnected = callerConnected || sub.proof == proof
	}

	if !callerConnected {
		return false
	}

	update := op.GetUpdateDocument()

	if update == nil || !update.GetCurrentDocument().GetExists() || len(update.GetUpdateMask().GetPaths()) != 1 {
		return false
	}

	path, err := fieldPath(update.UpdateMask.Paths[0])

	if err != nil || len(path) != 1 || path[0] != "prp" {
		return false
	}

	input := update.GetDocument().GetFields().GetFields()

	if len(input) != 1 {
		return false
	}

	properties := input["prp"].GetMapValue()
	previous := fields["prp"].GetMapValue()

	if properties == nil || previous == nil || len(properties.GetFields()) != len(previous.GetFields()) {
		return false
	}

	marker, ok := properties.Fields["_Bex_PropIsHost"].GetValueType().(*common.Value_StringValue)

	if !ok || len(marker.StringValue) == 0 || len(marker.StringValue) > 64 {
		return false
	}

	if _, ok := previous.Fields["_Bex_PropIsHost"].GetValueType().(*common.Value_StringValue); !ok {
		return false
	}

	for key, value := range previous.Fields {
		next, exists := properties.Fields[key]

		if !exists || (key != "_Bex_PropIsHost" && !proto.Equal(value, next)) {
			return false
		}
	}

	return true
}
