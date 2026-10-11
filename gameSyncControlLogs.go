package main

import (
	"context"
	"strings"

	gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"
)

// These events are independent of the temporary private capture budget.
// Identifiers are digests; document values use the existing redacted formatter.
func gamesyncDebugContext(ctx context.Context, proof, room string) map[string]any {
	event := map[string]any{
		"room_digest": identityDigest(room),
	}
	parts := strings.SplitN(proof, "|", 2)
	event["caller_digest"] = identityDigest(parts[0])
	event["recipient_digest"] = identityDigest(proof)

	if rpc, ok := ctx.Value(rpcKey{}).(*rpcState); ok {
		event["rpc_id"] = rpc.id
	}

	return event
}

func (s *gamesyncServer) diagnosticControlWrite(ctx context.Context, proof, room, owner, decision string, op *gs.WriteOperation, old *gs.Document) {
	event := gamesyncDebugContext(ctx, proof, room)
	event["event"] = "gamesync_control_authorization"
	event["decision"] = decision

	if owner != "" {
		event["owner_digest"] = identityDigest(owner)
	}

	event["previous_fields"] = summarizeFields(old.GetFields(), 0)

	switch operation := op.GetOperationType().(type) {
	case *gs.WriteOperation_UpdateDocument:
		event["operation"] = "update"
		event["mask"] = operation.UpdateDocument.GetUpdateMask().GetPaths()
		event["fields"] = summarizeFields(operation.UpdateDocument.GetDocument().GetFields(), 0)
		event["precondition_exists"] = operation.UpdateDocument.GetCurrentDocument().GetExists()

	case *gs.WriteOperation_MergeDocument:
		event["operation"] = "merge"
		event["fields"] = summarizeFields(operation.MergeDocument.GetDocument().GetFields(), 0)

	case *gs.WriteOperation_DeleteDocument:
		event["operation"] = "delete"

	case *gs.WriteOperation_TransformDocument:
		event["operation"] = "transform"
		paths := []string{}
		for _, transform := range operation.TransformDocument.GetFieldTransforms() {
			paths = append(paths, transform.GetFieldPath())
		}
		event["mask"] = paths
	}

	s.p.event(event)
}
