package main

import "testing"

func TestDiagnosticDocumentPathRedactsIdentifiers(t *testing.T) {
	for input, expected := range map[string]string{
		"docs/__gs/f": "docs/__gs/f",
		"docs/__us/secret-user/private/password1234": "docs/__us/<11>/<7>/<12>",
		"docs/1234/value": "docs/<4>/<5>",
	} {
		if got := redactedDocumentPath(input); got != expected {
			t.Fatalf("got %q, want %q", got, expected)
		}
	}
}
