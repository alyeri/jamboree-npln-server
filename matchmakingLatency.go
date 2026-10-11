package main

import (
	"encoding/json"
	"math/big"

	"google.golang.org/protobuf/proto"

	mm "github.com/alyeri/jamboree-npln-server/proto/matchmaking/v1"
)

func cloneLatencyData(definition *mm.UserDefinition) *mm.LatencyData {
	if definition.GetLatencyData() == nil {
		return nil
	}

	return proto.Clone(definition.LatencyData).(*mm.LatencyData)
}

// Gamesync represents these protobuf durations as a JSON string containing
// nanoseconds, as in the Pokemon and Wonder implementations.
func gamesyncLatencyJSON(data *mm.LatencyData) string {
	latencies := map[string]any{}

	for region, duration := range data.GetLatencies() {
		nanos := new(big.Int).Mul(big.NewInt(duration.GetSeconds()), big.NewInt(1_000_000_000))

		nanos.Add(nanos, big.NewInt(int64(duration.GetNanos())))

		latencies[region] = map[string]any{
			"nanos": nanos,
		}
	}

	encoded, _ := json.Marshal(map[string]any{
		"latencies": latencies,
	})

	return string(encoded)
}
