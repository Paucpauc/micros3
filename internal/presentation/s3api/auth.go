package s3api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/paucpauc/micros3/internal/config"
	"go.uber.org/zap"
)

var authHeaderRegex = regexp.MustCompile(`AWS4-HMAC-SHA256\s+Credential=([^/]+)/([^/]+)/([^/]+)/s3/aws4_request,\s*SignedHeaders=([^,]+),\s*Signature=([0-9a-fA-F]+)`)

// AuthValidator validates S3 AWS Signature V4 requests
type AuthValidator struct {
	credentials []config.Credentials
	logger      *zap.Logger
}

func NewAuthValidator(credentials []config.Credentials, logger *zap.Logger) *AuthValidator {
	return &AuthValidator{
		credentials: credentials,
		logger:      logger,
	}
}

// ValidateRequest checks the AWS Signature V4 signature of the incoming request
func (av *AuthValidator) ValidateRequest(r *http.Request) (string, error) {
	// 1. Get Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		// Also check query param signature (pre-signed URL) - optional but nice.
		// For wal-g, it uses header authorization. So we focus on that.
		return "", errors.New("missing Authorization header")
	}

	matches := authHeaderRegex.FindStringSubmatch(authHeader)
	if len(matches) != 6 {
		return "", errors.New("invalid Authorization header format")
	}

	accessKey := matches[1]
	dateStr := matches[2]   // YYYYMMDD
	regionStr := matches[3] // region
	signedHeadersStr := matches[4]
	signatureHex := matches[5]

	// Region is taken from the request credential scope

	// 2. Find secret key
	var secretKey string
	for _, cred := range av.credentials {
		if cred.AccessKey == accessKey {
			secretKey = cred.SecretKey
			break
		}
	}
	if secretKey == "" {
		return "", errors.New("invalid access key ID")
	}

	// 3. Get request date/time from headers
	amzDate := r.Header.Get("X-Amz-Date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	if amzDate == "" {
		return "", errors.New("missing Date or X-Amz-Date header")
	}

	// Verify request is within clock skew (15 minutes)
	reqTime, err := parseAmzDate(amzDate)
	if err != nil {
		return "", fmt.Errorf("invalid date format: %w", err)
	}
	skew := time.Since(reqTime)
	if skew < 0 {
		skew = -skew
	}
	if skew > 15*time.Minute {
		return "", errors.New("request time too skewed")
	}

	// 4. Determine hashed payload
	hashedPayload := r.Header.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		hashedPayload = "UNSIGNED-PAYLOAD"
	}

	// For streaming uploads (aws-chunked) the payload hash in the signature is always
	// the literal string "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" — body chunks are signed
	// separately, so we must NOT hash the body here.
	isStreaming := hashedPayload == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"

	if !isStreaming && hashedPayload != "UNSIGNED-PAYLOAD" {
		// Read and hash request body
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			return "", fmt.Errorf("failed to read body for auth: %w", err)
		}
		r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

		h := sha256.Sum256(bodyBytes)
		calculatedHash := hex.EncodeToString(h[:])
		if calculatedHash != hashedPayload {
			return "", errors.New("payload hash mismatch")
		}
	}

	// For streaming uploads, swap out the body reader with an aws-chunked decoder
	// so downstream handlers receive plain data without chunk framing.
	if isStreaming {
		r.Body = decodeChunkedBody(r.Body)
	}

	// 5. Reconstruct Canonical Request
	canonicalRequest, err := buildCanonicalRequest(r, signedHeadersStr, hashedPayload)
	if err != nil {
		return "", fmt.Errorf("failed to build canonical request: %w", err)
	}

	loweredSigned := strings.Split(strings.ToLower(signedHeadersStr), ";")
	sort.Strings(loweredSigned)
	var headerVals []string
	for _, hName := range loweredSigned {
		hVal := r.Header.Get(hName)
		if hName == "host" {
			if hVal == "" {
				hVal = r.Host
			}
			hVal = stripStandardPort(hVal)
		}
		allVals := r.Header.Values(hName)
		headerVals = append(headerVals, fmt.Sprintf("%s=%q (all: %v)", hName, hVal, allVals))
	}
	av.logger.Debug("SigV4 signed header values", zap.Strings("headers", headerVals))

	// 6. Build String to Sign
	scope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStr, regionStr)
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate,
		scope,
		sha256Hex(canonicalRequest),
	)

	// 7. Calculate Signature
	expectedSignature := calculateSignature(secretKey, dateStr, regionStr, stringToSign)

	av.logger.Debug("SigV4 debug",
		zap.String("accessKey", accessKey),
		zap.String("secretKey", secretKey),
		zap.String("dateStr", dateStr),
		zap.String("regionStr", regionStr),
		zap.String("amzDate", amzDate),
		zap.String("scope", scope),
		zap.String("signedHeaders", signedHeadersStr),
		zap.String("hashedPayload", hashedPayload),
		zap.String("canonicalRequest", canonicalRequest),
		zap.String("stringToSign", stringToSign),
		zap.String("expectedSignature", expectedSignature),
		zap.String("gotSignature", signatureHex),
		zap.String("r.Host", r.Host),
	)

	if !hmac.Equal([]byte(signatureHex), []byte(expectedSignature)) {
		return "", errors.New("signature mismatch")
	}

	return accessKey, nil
}

func parseAmzDate(dateStr string) (time.Time, error) {
	// Try ISO 8601 basic format (e.g. 20260630T120000Z)
	t, err := time.Parse("20060102T150405Z", dateStr)
	if err == nil {
		return t, nil
	}
	// Try HTTP-date format (e.g. Tue, 30 Jun 2026 12:00:00 GMT)
	t, err = time.Parse(time.RFC1123, dateStr)
	if err == nil {
		return t, nil
	}
	t, err = time.Parse(time.RFC1123Z, dateStr)
	if err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unable to parse date string: %s", dateStr)
}

func buildCanonicalRequest(r *http.Request, signedHeadersStr string, hashedPayload string) (string, error) {
	// HTTP Method
	method := r.Method

	// Canonical URI
	uri := getCanonicalURI(r.URL.Path)

	// Canonical Query String
	query := getCanonicalQueryString(r.URL.Query())

	// Canonical Headers & Signed Headers
	signedHeaders := strings.Split(strings.ToLower(signedHeadersStr), ";")
	sort.Strings(signedHeaders)

	var canonicalHeaders strings.Builder
	for _, hName := range signedHeaders {
		hVal := r.Header.Get(hName)
		if hName == "host" {
			if hVal == "" {
				hVal = r.Host
			}
			hVal = stripStandardPort(hVal)
		}
		// Trim spaces and clean value
		hVal = strings.TrimSpace(hVal)
		// Collapse multiple spaces into one
		re := regexp.MustCompile(`\s+`)
		hVal = re.ReplaceAllString(hVal, " ")

		canonicalHeaders.WriteString(fmt.Sprintf("%s:%s\n", hName, hVal))
	}

	canonicalSignedHeaders := strings.Join(signedHeaders, ";")

	canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
		method,
		uri,
		query,
		canonicalHeaders.String(),
		canonicalSignedHeaders,
		hashedPayload,
	)

	return canonicalRequest, nil
}

func stripStandardPort(hostVal string) string {
	h, p, err := net.SplitHostPort(hostVal)
	if err != nil {
		return hostVal
	}
	if p == "80" || p == "443" {
		return h
	}
	return hostVal
}

func getCanonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	// According to AWS SigV4 specification:
	// URI-encode each path segment. Slashes must not be encoded.
	segments := strings.Split(path, "/")
	var encodedSegments []string
	for _, s := range segments {
		encodedSegments = append(encodedSegments, pathEscape(s))
	}
	encodedURI := strings.Join(encodedSegments, "/")
	if !strings.HasPrefix(encodedURI, "/") {
		encodedURI = "/" + encodedURI
	}
	return encodedURI
}

func getCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	var keys []string
	for k := range query {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var queryParts []string
	for _, k := range keys {
		vals := query[k]
		sort.Strings(vals)
		escapedK := pathEscape(k)
		if len(vals) == 0 {
			queryParts = append(queryParts, fmt.Sprintf("%s=", escapedK))
		} else {
			for _, v := range vals {
				queryParts = append(queryParts, fmt.Sprintf("%s=%s", escapedK, pathEscape(v)))
			}
		}
	}

	return strings.Join(queryParts, "&")
}

// pathEscape performs S3-compatible path escaping
func pathEscape(s string) string {
	escaped := url.PathEscape(s)
	escaped = strings.ReplaceAll(escaped, "*", "%2A")
	escaped = strings.ReplaceAll(escaped, "!", "%21")
	escaped = strings.ReplaceAll(escaped, "'", "%27")
	escaped = strings.ReplaceAll(escaped, "(", "%28")
	escaped = strings.ReplaceAll(escaped, ")", "%29")
	escaped = strings.ReplaceAll(escaped, "+", "%2B")
	escaped = strings.ReplaceAll(escaped, ",", "%2C")
	escaped = strings.ReplaceAll(escaped, ":", "%3A")
	escaped = strings.ReplaceAll(escaped, ";", "%3B")
	escaped = strings.ReplaceAll(escaped, "=", "%3D")
	escaped = strings.ReplaceAll(escaped, "@", "%40")
	return escaped
}

func sha256Hex(data string) string {
	h := sha256.Sum256([]byte(data))
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func calculateSignature(secretKey string, dateStr string, regionStr string, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), dateStr)
	kRegion := hmacSHA256(kDate, regionStr)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hmacSHA256(kSigning, stringToSign)
	return hex.EncodeToString(signature)
}

// decodeChunkedBody returns an io.ReadCloser that transparently decodes the
// AWS "aws-chunked" transfer encoding used in streaming SigV4 uploads.
//
// Each chunk has the form:
//
//	<hex-size>[;chunk-signature=<sig>]\r\n
//	<data>\r\n
//
// A terminal chunk with size 0 signals the end of the body.
func decodeChunkedBody(body io.ReadCloser) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		defer body.Close()
		buf := make([]byte, 32*1024)
		// We need line-oriented reading, so wrap in a simple scanner.
		data, err := io.ReadAll(body)
		if err != nil {
			_ = pw.CloseWithError(err)
			return
		}
		_ = buf // keep buf to avoid unused import

		remaining := data
		for len(remaining) > 0 {
			// Find the end of the chunk header line (\r\n)
			crlfIdx := indexCRLF(remaining)
			if crlfIdx < 0 {
				break
			}
			headerLine := string(remaining[:crlfIdx])
			remaining = remaining[crlfIdx+2:]

			// Parse chunk size (hex before optional semicolon)
			sizeStr := headerLine
			if semiIdx := strings.Index(headerLine, ";"); semiIdx >= 0 {
				sizeStr = headerLine[:semiIdx]
			}
			sizeStr = strings.TrimSpace(sizeStr)

			var chunkSize int64
			_, parseErr := fmt.Sscanf(sizeStr, "%x", &chunkSize)
			if parseErr != nil || chunkSize < 0 {
				break
			}
			// Terminal chunk
			if chunkSize == 0 {
				break
			}
			if int64(len(remaining)) < chunkSize+2 {
				// Malformed: write what we have
				_, _ = pw.Write(remaining[:len(remaining)])
				remaining = nil
				break
			}
			_, _ = pw.Write(remaining[:chunkSize])
			remaining = remaining[chunkSize+2:] // skip trailing \r\n
		}
		pw.Close()
	}()
	return pr
}

func indexCRLF(data []byte) int {
	for i := 0; i < len(data)-1; i++ {
		if data[i] == '\r' && data[i+1] == '\n' {
			return i
		}
	}
	return -1
}
