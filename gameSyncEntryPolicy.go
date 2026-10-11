package main

import gs "github.com/alyeri/jamboree-npln-server/proto/gamesync/v1"

// A password or visibility write does not change the host's entry policy.
func writesParticipationField(operation *gs.WriteOperation) bool {
	if update := operation.GetUpdateDocument(); update != nil {
		mask := update.GetUpdateMask()

		if mask == nil {
			return true
		}

		for _, field := range mask.Paths {
			path, err := fieldPath(field)

			if field == "*" || err == nil && len(path) == 1 && path[0] == "cp" {
				return true
			}
		}
	}

	if merge := operation.GetMergeDocument(); merge != nil {
		return merge.GetDocument().GetFields().GetFields()["cp"] != nil
	}

	if transform := operation.GetTransformDocument(); transform != nil {
		for _, field := range transform.GetFieldTransforms() {
			path, err := fieldPath(field.GetFieldPath())

			if err == nil && len(path) == 1 && path[0] == "cp" {
				return true
			}
		}
	}

	return false
}
