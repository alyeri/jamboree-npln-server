package main

import (
	"fmt"
	"strings"
)

// Keep built-in protocol names visible without exposing user document identifiers.
func redactedDocumentPath(name string) string {
	parts := strings.Split(name, "/")

	if len(parts) > 33 || len(name) > 512 {
		return "invalid"
	}

	for i, part := range parts {
		if i == 2 && parts[0] == "docs" && parts[1] == "__gs" && len(part) == 1 && part[0] >= 'a' && part[0] <= 'z' {
			continue
		}

		switch part {
		case "docs", "__gs", "__us", "__pgn", "__stu", "__stg", "__pus", "__upcs", "__ps", "__ug", "f", "v", "host", "current":
		default:
			parts[i] = fmt.Sprintf("<%d>", len(part))
		}
	}

	return strings.Join(parts, "/")
}
