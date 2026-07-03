package ingestion

import (
	"io"
	"log"
	"net/http"

	"github.com/advenn/logd/pkg/lokicompat"
)

// Handler serves ingestion endpoints: Loki push and native JSON.
type Handler struct {
	queue  *WriteQueue
	parser *Parser
}

// NewHandler creates an ingestion Handler.
func NewHandler(queue *WriteQueue) *Handler {
	return &Handler{
		queue:  queue,
		parser: NewParser(),
	}
}

// RegisterRoutes adds ingestion routes to the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /loki/api/v1/push", h.handleLokiPush)
	mux.HandleFunc("POST /v1/ingest", h.handleNativeIngest)
}

// handleLokiPush handles POST /loki/api/v1/push — the Loki push endpoint.
// Accepts snappy-compressed protobuf (Promtail) and plain JSON (for testing).
func (h *Handler) handleLokiPush(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeLokiError(w, http.StatusBadRequest, "reading body: "+err.Error())
		return
	}
	defer r.Body.Close()

	contentType := r.Header.Get("Content-Type")

	switch contentType {
	case "application/x-protobuf", "application/x-protobuf; charset=utf-8":
		req, err := lokicompat.DecodePushRequest(body)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "decoding protobuf: "+err.Error())
			return
		}
		logEntries, err := h.parser.ParseLokiPush(req)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "parsing push: "+err.Error())
			return
		}
		if err := h.queue.EnqueueBatch(logEntries); err != nil {
			log.Printf("error enqueueing batch: %v", err)
		}

	case "application/json", "application/json; charset=utf-8", "":
		// For JSON, accept a Loki-compatible push JSON object.
		// Actually, JSON push is not standard Loki — this is a convenience for testing.
		// Parse as native JSON entry.
		entry, err := h.parser.ParseNativeJSON(body)
		if err != nil {
			writeLokiError(w, http.StatusBadRequest, "parsing JSON: "+err.Error())
			return
		}
		if err := h.queue.Enqueue(entry); err != nil {
			log.Printf("error enqueueing entry: %v", err)
		}

	default:
		writeLokiError(w, http.StatusUnsupportedMediaType, "unsupported Content-Type: "+contentType)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleNativeIngest handles POST /v1/ingest — native structured JSON.
func (h *Handler) handleNativeIngest(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	entry, err := h.parser.ParseNativeJSON(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := h.queue.Enqueue(entry); err != nil {
		http.Error(w, "queue full", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// writeLokiError writes a Loki-format error response.
func writeLokiError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	resp := lokicompat.ErrorResponse(msg)
	w.Write(lokicompat.MarshalJSON(resp))
}
