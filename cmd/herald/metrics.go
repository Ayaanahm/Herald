package main

import (
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func promHandler() http.Handler { return promhttp.Handler() }

// withPprof adds /debug/pprof when HERALD_PPROF=1. Off by default: profiles
// expose internals and should not be reachable in production.
func withPprof(h http.Handler) http.Handler {
	if os.Getenv("HERALD_PPROF") != "1" {
		return h
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.Handle("/", h)
	return mux
}
