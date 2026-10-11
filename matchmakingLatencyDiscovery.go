package main

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func (g *gameSessionServer) ListLatencyMeasurementServers(ctx context.Context, req *mm.ListLatencyMeasurementServersRequest) (*mm.ListLatencyMeasurementServersResponse, error) {
	if _, err := callerIdentity(ctx, g.p.identityKey); err != nil {
		return nil, err
	}

	parent := req.GetParent()

	if parent != "tenants/current" && parent != "tenants/t-adf89f68" && parent != "tenants/t-adf89f68-lp1" {
		return nil, status.Error(codes.InvalidArgument, "Jamboree tenant required")
	}

	if req.GetPageToken() != "" || req.GetPageSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid latency server page")
	}

	host, err := publicServerIP()

	if err != nil {
		return nil, err
	}

	g.p.event(map[string]any{
		"event":    "latency_servers_listed",
		"count":    1,
		"protocol": "HTTP",
		"port":     17480,
	})

	return &mm.ListLatencyMeasurementServersResponse{
		LatencyMeasurementServers: []*mm.LatencyMeasurementServer{
			{
				Name:     "tenants/t-adf89f68-lp1/latencyMeasurementServers/local",
				Host:     host,
				Port:     17480,
				Protocol: mm.LatencyMeasurementServer_HTTP,
			},
		},
	}, nil
}

func latencyHandler(p *serverContext) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)

			return
		}

		w.Header().Set("Content-Type", "text/plain")

		w.Header().Set("Cache-Control", "no-store")

		w.Header().Set("Content-Length", "2")

		w.WriteHeader(http.StatusOK)

		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("OK"))
		}

		p.event(map[string]any{
			"event":  "latency_http_probe",
			"method": r.Method,
			"status": 200,
		})
	})
}

func serveLatency(addr string, p *serverContext) error {
	s := &http.Server{
		Addr:              addr,
		Handler:           latencyHandler(p),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       3 * time.Second,
		WriteTimeout:      3 * time.Second,
		IdleTimeout:       5 * time.Second,
		MaxHeaderBytes:    4096,
	}

	return s.ListenAndServe()
}
