package http

import "net/http"

// ExportIdempotent exposes the middleware to the external test package.
func (s *Server) ExportIdempotent(next http.HandlerFunc) http.HandlerFunc { return s.idempotent(next) }
