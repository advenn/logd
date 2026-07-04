package loki

import (
	"strconv"
	"time"

	"github.com/advenn/logd/core/query"
)

// matrixResponse is the Loki result for a metric query_range: one time series per label
// set, each a list of [unix_seconds, value_string] points.
type matrixResponse struct {
	Status string     `json:"status"`
	Data   matrixData `json:"data"`
}

type matrixData struct {
	ResultType string         `json:"resultType"`
	Result     []matrixSeries `json:"result"`
}

type matrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"` // [unix_seconds_number, value_string]
}

func newMatrixResponse(series []query.MatrixSeries) matrixResponse {
	res := make([]matrixSeries, 0, len(series))
	for _, s := range series {
		vals := make([][2]any, 0, len(s.Points))
		for _, p := range s.Points {
			vals = append(vals, [2]any{float64(p.TS) / 1e9, strconv.FormatFloat(p.Value, 'f', -1, 64)})
		}
		res = append(res, matrixSeries{Metric: s.Labels, Values: vals})
	}
	return matrixResponse{Status: "success", Data: matrixData{ResultType: "matrix", Result: res}}
}

// newVectorFromMatrix collapses a single-step matrix into an instant vector (each series'
// last point).
func newVectorFromMatrix(series []query.MatrixSeries) vectorResponse {
	res := make([]vectorSample, 0, len(series))
	for _, s := range series {
		if len(s.Points) == 0 {
			continue
		}
		p := s.Points[len(s.Points)-1]
		res = append(res, vectorSample{Metric: s.Labels, Value: [2]any{float64(p.TS) / 1e9, strconv.FormatFloat(p.Value, 'f', -1, 64)}})
	}
	return vectorResponse{Status: "success", Data: vectorData{ResultType: "vector", Result: res}}
}

// The JSON response shapes Grafana's Loki datasource expects. Ported from the prior
// implementation.

type queryRangeResponse struct {
	Status string         `json:"status"`
	Data   queryRangeData `json:"data"`
}

type queryRangeData struct {
	ResultType string         `json:"resultType"`
	Result     []streamResult `json:"result"`
}

// streamResult groups values under a shared label set. Values are [unix_nano_string, line].
type streamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

// tailResponse is one Loki /tail WebSocket message: newly-matched streams plus any
// dropped-entry markers (logd never reports drops here — a slow tailer's entries are
// silently dropped by the non-blocking fan-out).
type tailResponse struct {
	Streams        []streamResult `json:"streams"`
	DroppedEntries []any          `json:"dropped_entries"`
}

func newQueryRangeResponse(results []streamResult) queryRangeResponse {
	if results == nil {
		results = []streamResult{}
	}
	return queryRangeResponse{
		Status: "success",
		Data:   queryRangeData{ResultType: "streams", Result: results},
	}
}

// vectorResponse answers instant metric queries (one sample per series) and Grafana's
// datasource health check (vector(1)+vector(1) → a scalar stub).
type vectorResponse struct {
	Status string     `json:"status"`
	Data   vectorData `json:"data"`
}

type vectorData struct {
	ResultType string         `json:"resultType"`
	Result     []vectorSample `json:"result"`
}

type vectorSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"` // [unix_seconds_number, value_string] — Loki/Prometheus shape
}

func newScalarVector(ts time.Time, value string) vectorResponse {
	return vectorResponse{
		Status: "success",
		Data: vectorData{
			ResultType: "vector",
			Result:     []vectorSample{{Metric: map[string]string{}, Value: [2]any{float64(ts.UnixNano()) / 1e9, value}}},
		},
	}
}

type labelsResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

func labelResponse(values []string) labelsResponse {
	if values == nil {
		values = []string{}
	}
	return labelsResponse{Status: "success", Data: values}
}
