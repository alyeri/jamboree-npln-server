package main

import (
	"bytes"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/net/http2"
)

func TestGamesyncRoutesByTenantWithoutClientIP(t *testing.T) {
	routes := map[string]gamesyncRoute{}

	for _, tenant := range []string{"t-adf89f68-lp1", "t-ba973ec6-lp1"} {
		label := tenant
		routes[tenant] = gamesyncRoute{
			label: label,
			proxy: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer private-test" {
					t.Error("authorization not preserved")
				}

				w.Write([]byte(label))
			}),
		}
	}

	router := gamesyncTenantRouter(routes, &serverContext{
		writer: &bytes.Buffer{},
	})

	for _, tenant := range []string{
		"t-adf89f68-lp1",
		"t-ba973ec6-lp1",
		"unknown",
		"",
	} {
		request := httptest.NewRequest(http.MethodPost, "/nn.npln.gamesync.v1.Gamesync/GetDocument", nil)
		request.ProtoMajor = 2

		request.Header.Set("Content-Type", "application/grpc")

		request.Header.Set("Authorization", "Bearer private-test")

		request.RemoteAddr = "192.0.2.123:1000"

		if tenant != "" {
			request.Header.Set("npln-tenant-id", tenant)
		}

		response := httptest.NewRecorder()

		router.ServeHTTP(response, request)

		if _, found := routes[tenant]; found {
			if response.Body.String() != tenant {
				t.Fatal("tenant sent to wrong backend")
			}
		} else if response.Header().Get("Grpc-Status") == "" {
			t.Fatal("unknown or missing tenant accepted")
		}
	}
}

func TestGamesyncBackendPreservesStreamingAndTrailers(t *testing.T) {
	backend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Error("backend must use HTTP/2")
		}

		w.Header().Set("Content-Type", "application/grpc")

		w.Header().Set("Trailer", "Grpc-Status")

		w.WriteHeader(http.StatusOK)

		w.(http.Flusher).Flush()

		buffer := make([]byte, 5)

		for {
			n, err := r.Body.Read(buffer)

			if n > 0 {
				w.Write(buffer[:n])

				w.(http.Flusher).Flush()
			}

			if err != nil {
				break
			}
		}

		w.Header().Set("Grpc-Status", "0")
	}))
	backend.EnableHTTP2 = true

	backend.StartTLS()
	defer backend.Close()

	certificate := filepath.Join(t.TempDir(), "backend.pem")
	err := os.WriteFile(certificate, pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: backend.Certificate().Raw,
	}), 0600)

	if err != nil {
		t.Fatal(err)
	}

	proxy, err := gamesyncBackend(strings.TrimPrefix(backend.URL, "https://"), "example.com", certificate)

	if err != nil {
		t.Fatal(err)
	}

	gateway := httptest.NewUnstartedServer(proxy)
	gateway.EnableHTTP2 = true

	gateway.StartTLS()
	defer gateway.Close()

	transport := gateway.Client().Transport.(*http.Transport)

	if err := http2.ConfigureTransport(transport); err != nil {
		t.Fatal(err)
	}

	reader, writer := io.Pipe()
	request, _ := http.NewRequest(http.MethodPost, gateway.URL, reader)

	request.Header.Set("Content-Type", "application/grpc")

	response, err := gateway.Client().Do(request)

	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	// Read a reply while the request remains open: buffering the entire request
	// would deadlock a bidirectional KeepUserSession stream.
	for _, frame := range []string{"first", "again"} {
		if _, err := writer.Write([]byte(frame)); err != nil {
			t.Fatal(err)
		}

		got := make([]byte, len(frame))

		if _, err := io.ReadFull(response.Body, got); err != nil || string(got) != frame {
			t.Fatalf("streamed reply failed: %v", err)
		}
	}

	writer.Close()

	io.Copy(io.Discard, response.Body)

	if response.Trailer.Get("Grpc-Status") != "0" {
		t.Fatal("gRPC trailer lost")
	}
}
