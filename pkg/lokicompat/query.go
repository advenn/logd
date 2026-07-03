package lokicompat

import (
	"encoding/json"
	"time"
)

// ---- Loki query_range response types ----
// These match the JSON Grafana expects from a Loki data source.

// QueryRangeResponse is the top-level response for /loki/api/v1/query_range.
type QueryRangeResponse struct {
	Status string         `json:"status"`
	Data   QueryRangeData `json:"data"`
}

// QueryRangeData wraps the result stream array.
type QueryRangeData struct {
	ResultType string         `json:"resultType"`
	Result     []StreamResult `json:"result"`
}

// StreamResult groups values under a shared label set.
type StreamResult struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"` // [unix_nano_string, log_line]
}

// ---- Loki vector (metric) response types ----
// Used for instant metric/scalar queries (e.g. Grafana's health-check query
// vector(1)+vector(1)). logd does not evaluate metric queries yet (Phase 8),
// but must return a valid vector shape so clients parse it correctly instead
// of choking on a streams response with the wrong field count.

// VectorResponse is the top-level response for a metric instant query.
type VectorResponse struct {
	Status string     `json:"status"`
	Data   VectorData `json:"data"`
}

// VectorData wraps the vector sample array.
type VectorData struct {
	ResultType string         `json:"resultType"`
	Result     []VectorSample `json:"result"`
}

// VectorSample is one sample: a label set and a [unixSeconds, value] pair.
// Value is encoded as [number, string] exactly like Loki/Prometheus.
type VectorSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]interface{}    `json:"value"`
}

// NewScalarVectorResponse builds a single-sample vector response at the given
// time with the given string value and empty labels.
func NewScalarVectorResponse(ts time.Time, value string) *VectorResponse {
	return &VectorResponse{
		Status: "success",
		Data: VectorData{
			ResultType: "vector",
			Result: []VectorSample{
				{
					Metric: map[string]string{},
					Value:  [2]interface{}{float64(ts.UnixNano()) / 1e9, value},
				},
			},
		},
	}
}

// ---- Loki labels response types ----

// LabelResponse is the response for /loki/api/v1/labels.
type LabelResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// LabelValuesResponse is the response for /loki/api/v1/label/{name}/values.
type LabelValuesResponse struct {
	Status string   `json:"status"`
	Data   []string `json:"data"`
}

// ---- Serialization helpers ----

// NewQueryRangeResponse creates a success response from stream results.
func NewQueryRangeResponse(results []StreamResult) *QueryRangeResponse {
	if results == nil {
		results = []StreamResult{}
	}
	return &QueryRangeResponse{
		Status: "success",
		Data: QueryRangeData{
			ResultType: "streams",
			Result:     results,
		},
	}
}

// NewLabelResponse creates a success response with label keys.
func NewLabelResponse(labels []string) *LabelResponse {
	if labels == nil {
		labels = []string{}
	}
	return &LabelResponse{
		Status: "success",
		Data:   labels,
	}
}

// NewLabelValuesResponse creates a success response with label values.
func NewLabelValuesResponse(values []string) *LabelValuesResponse {
	if values == nil {
		values = []string{}
	}
	return &LabelValuesResponse{
		Status: "success",
		Data:   values,
	}
}

// MarshalJSON is a convenience to marshal any value without returning an error
// the caller must handle — it panics on error, suitable for test helpers.
func MarshalJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
