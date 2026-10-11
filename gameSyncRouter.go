package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/net/http2"
)

type gamesyncRoute struct {
	label string
	proxy http.Handler
}

func gamesyncBackend(address, serverName, certificate string) (http.Handler, error) {
	pem, err := os.ReadFile(certificate)

	if err != nil {
		return nil, err
	}

	roots := x509.NewCertPool()

	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("invalid backend certificate for %s", serverName)
	}

	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: serverName,
			RootCAs:    roots,
		},
	}

	proxy := httputil.NewSingleHostReverseProxy(&url.URL{
		Scheme: "https",
		Host:   address,
	})
	proxy.Transport = transport
	proxy.FlushInterval = -1
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		// Do not include request headers, tokens or backend error payloads.
		gamesyncRoutingError(w, "14", "Gamesync backend unavailable")
	}

	return proxy, nil
}

func gamesyncRoutingError(w http.ResponseWriter, code, message string) {
	w.Header().Set("Content-Type", "application/grpc")

	w.Header().Set("Grpc-Status", code)

	w.Header().Set("Grpc-Message", message)

	w.WriteHeader(http.StatusOK)
}

func gamesyncTenantRouter(routes map[string]gamesyncRoute, p *serverContext) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 || r.Method != http.MethodPost ||
			!strings.HasPrefix(r.URL.Path, "/nn.npln.") ||
			!strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			http.Error(w, "gRPC over HTTP/2 required", http.StatusBadRequest)

			return
		}

		tenant, err := gamesyncRequestTenant(r)

		if err != nil || tenant == "" {
			p.event(map[string]any{
				"event":  "gamesync_route_rejected",
				"reason": "missing_or_invalid_routing_identity",
				"method": safeMethod(r.URL.Path),
			})

			gamesyncRoutingError(w, "3", "Gamesync tenant could not be determined")

			return
		}

		route, found := routes[tenant]

		if !found {
			gamesyncRoutingError(w, "5", "Unknown Gamesync tenant")

			return
		}

		p.event(map[string]any{
			"event":   "gamesync_tenant_routed",
			"backend": route.label,
			"method":  safeMethod(r.URL.Path),
		})

		route.proxy.ServeHTTP(w, r)
	})
}

func serveGamesyncRouter(address string, p *serverContext) error {
	jamboree, err := gamesyncBackend(
		os.Getenv("GAMESYNC_JAMBOREE_BACKEND"),
		"t-adf89f68-lp1.lp1.t.npln.srv.nintendo.net",
		os.Getenv("GAMESYNC_JAMBOREE_CERT"),
	)

	if err != nil {
		return err
	}

	wonder, err := gamesyncBackend(
		os.Getenv("GAMESYNC_WONDER_BACKEND"),
		"t-ba973ec6-lp1.lp1.t.npln.srv.nintendo.net",
		os.Getenv("GAMESYNC_WONDER_CERT"),
	)

	if err != nil {
		return err
	}

	routes := map[string]gamesyncRoute{
		"t-adf89f68": {
			label: "jamboree",
			proxy: jamboree,
		},
		"t-adf89f68-lp1": {
			label: "jamboree",
			proxy: jamboree,
		},
		"t-ba973ec6": {
			label: "wonder",
			proxy: wonder,
		},
		"t-ba973ec6-lp1": {
			label: "wonder",
			proxy: wonder,
		},
	}
	server := &http.Server{
		Addr:              address,
		Handler:           gamesyncTenantRouter(routes, p),
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    64 * 1024,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	if err := http2.ConfigureServer(server, &http2.Server{}); err != nil {
		return err
	}

	// Native NPLN clients may negotiate the historical grpc-exp ALPN token.
	server.TLSConfig.NextProtos = []string{"h2", "grpc-exp"}
	server.TLSNextProto["grpc-exp"] = server.TLSNextProto["h2"]

	p.event(map[string]any{
		"event":  "gamesync_router_start",
		"listen": address,
	})

	return server.ListenAndServeTLS(os.Getenv("CERT_FILE"), os.Getenv("KEY_FILE"))
}
