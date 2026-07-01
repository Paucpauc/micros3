package s3api

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// S3Error represents the standard S3 XML error response
type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestId string   `xml:"RequestId"`
}

// WriteError writes an S3-compliant XML error to the response writer
func WriteError(w http.ResponseWriter, r *http.Request, code string, message string, statusCode int) {
	reqID := r.Header.Get("X-Amz-Request-Id")
	if reqID == "" {
		reqID = uuid.New().String()
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", reqID)
	w.Header().Set("x-amz-id-2", reqID)
	w.Header().Set("Server", "MicroS3")
	w.WriteHeader(statusCode)

	errResponse := S3Error{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestId: reqID,
	}

	// XML header is required
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	_ = enc.Encode(errResponse)
}

// MapErrorToS3 maps generic errors or storage errors to S3 specific XML errors
func MapErrorToS3(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		return
	}

	// Detect common error conditions
	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "ServiceUnavailable"):
		WriteError(w, r, "ServiceUnavailable", "The service is temporarily unavailable. Writes are blocked during synchronization.", http.StatusServiceUnavailable)
	case errStr == "bucket not empty":
		WriteError(w, r, "BucketNotEmpty", "The bucket you tried to delete is not empty.", http.StatusConflict)
	case errStr == "not found" || errStr == "file does not exist" || errStr == "no such file or directory" || strings.Contains(errStr, "does not exist"):
		WriteError(w, r, "NoSuchKey", "The specified key does not exist.", http.StatusNotFound)
	default:
		WriteError(w, r, "InternalError", err.Error(), http.StatusInternalServerError)
	}
}
