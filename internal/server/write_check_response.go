package server

import "net/http"

type writeCheckResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *writeCheckResponseWriter) WriteHeader(statusCode int) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.ResponseWriter.WriteHeader(statusCode)
	}
}

func (w *writeCheckResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func responseAlreadyWritten(w http.ResponseWriter) bool {
	if wc, ok := w.(*writeCheckResponseWriter); ok {
		return wc.wroteHeader
	}
	return false
}
