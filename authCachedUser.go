package main

import "strings"

// A stale cache must trigger registration, never a token for the requested user.
func recoverableCachedUserID(id string) bool {
	if len(id) == 33 || (strings.HasPrefix(id, "u-") && len(id) == 34) {
		return true
	}

	if len(id) != 22 {
		return false
	}

	for _, character := range id {
		if character >= 'a' && character <= 'z' {
			continue
		}

		if character >= 'A' && character <= 'Z' {
			continue
		}

		if character >= '0' && character <= '9' {
			continue
		}

		if character != '-' && character != '_' {
			return false
		}
	}

	return true
}
