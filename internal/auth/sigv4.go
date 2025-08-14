package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

var (
	ErrNoAuthHeader        = errors.New("missing Authorization header")
	ErrUnsuportedAlgorithm = errors.New("unsupported algorithm")
	ErrBadCredentialScope  = errors.New("bad credential scope")
	ErrSignatureMismatch   = errors.New("signature does not match")
	ErrSkewedDate          = errors.New("date skew too large")
)

type CredentialsProvider interface {
	LookupSecret(accessKeyID string) (secret string, err error)
}

// ----- Public entrypoint -----

type VerifyOptions struct {
	// Максимально допустимый сдвиг времени (обычно 5-15 минут)
	MaxSkew time.Duration
	// Разрешить x-amz-content-sha256: UNSIGNED-PAYLOAD
	AllowUnsignedPayload bool
	// Регион/сервис — для S3 это "s3", регион можно не проверять строго (aws-cli кладёт любой)
	ExpectedService string // "s3"
}

type Result struct {
	AccessKeyID   string
	SignedHeaders []string
	AmzDate       time.Time
	Region        string
	ScopeDate     string
}

func VerifySigV4(r *http.Request, cred CredentialsProvider, opts VerifyOptions) (*Result, error) {
	authz := r.Header.Get("Authorization")
	if authz == "" {
		return nil, ErrNoAuthHeader
	}
	if !strings.HasPrefix(authz, "AWS4-HMAC-SHA256 ") {
		return nil, ErrUnsuportedAlgorithm
	}
	// parse Authorization params
	params := parseAuthzParams(strings.TrimPrefix(authz, "AWS4-HMAC-SHA256 "))
	credential := params["Credential"]
	signedHeaderCSV := params["SignedHeaders"]
	signatureHex := params["Signature"]
	if credential == "" || signedHeaderCSV == "" || signatureHex == "" {
		return nil, fmt.Errorf("authorization header malformed")
	}

	// Credential=AKIA.../YYYYMMDD/region/service/aws4_request
	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 {
		return nil, ErrBadCredentialScope
	}
	accessKeyID := credParts[0]
	scopeDate := credParts[1] // YYYYMMDD
	region := credParts[2]    // e.g. us-east-1
	service := credParts[3]   // must be "s3"
	term := credParts[4]      // aws4_request
	if service != opts.ExpectedService || term != "aws4_request" {
		return nil, ErrBadCredentialScope
	}

	// Time
	amzDate := r.Header.Get("x-amz-date")
	if amzDate == "" {
		amzDate = r.Header.Get("Date")
	}
	if amzDate == "" {
		return nil, fmt.Errorf("missing x-amz-date")
	}
	t, err := time.Parse("20060102T150405Z", amzDate)
	if err != nil {
		return nil, fmt.Errorf("bad x-amz-date")
	}
	if opts.MaxSkew > 0 {
		skew := time.Since(t)
		if skew < 0 {
			skew = -skew
		}
		if skew > opts.MaxSkew {
			return nil, ErrSkewedDate
		}
	}

	signedHeaders := strings.Split(signedHeaderCSV, ";")
	for i := range signedHeaders {
		signedHeaders[i] = strings.TrimSpace(strings.ToLower(signedHeaders[i]))
	}

	// Payload Hash
	payloadHash := r.Header.Get("x-amz-content-sha256")
	if payloadHash == "" {
		payloadHash = hexSha256OfBytes(nil)
	}
	if strings.EqualFold(payloadHash, "UNSIGNED-PAYLOAD") && !opts.AllowUnsignedPayload {
		return nil, fmt.Errorf("unsigned payload not allowed")
	}

	// Canonical request
	canonicalRequest, err := buildCanonicalRequest(r, signedHeaders, payloadHash)
	if err != nil {
		return nil, err
	}
	canonHash := hexSha256OfBytes([]byte(canonicalRequest))

	// String to sign
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		fmt.Sprintf("%s/%s/%s/aws4_request", scopeDate, region, service),
		canonHash,
	}, "\n")

	// Derive signing key
	secret, err := cred.LookupSecret(accessKeyID)
	if err != nil {
		return nil, err
	}
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(scopeDate))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	// Signature
	expectedSig := hmacSHA256Hex(kSigning, []byte(stringToSign))

	// Compare constant-time
	if subtle.ConstantTimeCompare([]byte(expectedSig), []byte(strings.ToLower(signatureHex))) != 1 {
		return nil, err
	}

	return &Result{
		AccessKeyID:   accessKeyID,
		SignedHeaders: signedHeaders,
		AmzDate:       t.UTC(),
		Region:        region,
		ScopeDate:     scopeDate,
	}, nil
}

// ----- helpers -----

func parseAuthzParams(s string) map[string]string {
	out := make(map[string]string)
	parts := strings.Split(s, ",")
	for _, p := range parts {
		p := strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, "=", 2)
		if len(kv) != 2 {
			continue
		}
		out[kv[0]] = strings.Trim(kv[1], `"`)
	}
	return out
}

func buildCanonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) (string, error) {
	method := r.Method

	// Canonical URI: уже percent-encoded
	uri := r.URL.EscapedPath()
	if uri == "" {
		uri = "/"
	}

	// Canonical Query String: сортировка по ключу/значению, RFC3986 encoding
	var qpairs []string
	q := r.URL.Query()
	for key, vals := range q {
		ek := uriEncode(key, true)
		sort.Strings(vals)
		for _, v := range vals {
			qpairs = append(qpairs, fmt.Sprintf("%s=%s", ek, uriEncode(v, true)))
		}
	}
	sort.Strings(qpairs)
	canonicalQuery := strings.Join(qpairs, "&")

	// Canonical Headers: только из SignedHeaders (в нижнем регистре; сворачиваем  пробелы)
	lcHeaders := make(http.Header)
	for k, vv := range r.Header {
		lcHeaders[strings.ToLower(k)] = vv
	}
	// Host обязателен; берём из r.Host
	lcHeaders["host"] = []string{r.Host}

	var buf bytes.Buffer
	for _, h := range signedHeaders {
		vv := lcHeaders[h]
		if len(vv) == 0 {
			if h == "host" {
				vv = []string{r.Host}
			} else {
				vv = []string{""}
			}
		}
		// Склеиваем значения через ",", сжимаем пробелы
		joined := strings.Join(vv, ",")
		joined = compressSpaces(joined)
		fmt.Fprintf(&buf, "%s:%s\n", h, strings.TrimSpace(joined))
	}
	canonicalHeaders := buf.String()

	// SignedHeaders CSV
	sh := strings.Join(signedHeaders, ";")

	// Final canonical request
	return strings.Join([]string{
		method,
		uri,
		canonicalQuery,
		canonicalHeaders,
		sh,
		payloadHash,
	}, "\n"), nil
}

func compressSpaces(s string) string {
	var b strings.Builder
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' {
			if !prevSpace {
				b.WriteByte(' ')
				prevSpace = true
			}
		} else {
			b.WriteRune(r)
			prevSpace = false
		}
	}
	return b.String()
}

func uriEncode(s string, encodeSlash bool) string {
	// AWS style: RFC3986; пробел -> %20; тильда не кодируется
	escaped := url.QueryEscape(s)
	escaped = strings.ReplaceAll(escaped, "+", "%20")
	escaped = strings.ReplaceAll(escaped, "&7E", "~")
	if !encodeSlash {
		escaped = strings.ReplaceAll(escaped, "%2F", "/")
	}
	return escaped
}

func hexSha256OfBytes(p []byte) string {
	h := sha256.Sum256(p)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

func hmacSHA256Hex(key, data []byte) string {
	return hex.EncodeToString(hmacSHA256(key, data))
}

// Utility: если нужен хэш тела запроса (обычно полагаемся на x-amz-content-sha256)
func Sha256HexOfReader(r io.ReadSeeker) (string, error) {
	cur, _ := r.Seek(0, io.SeekCurrent)
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	_, _ = r.Seek(cur, io.SeekStart)
	return hex.EncodeToString(h.Sum(nil)), nil
}
