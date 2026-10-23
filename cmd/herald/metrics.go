package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func promHandler() http.Handler { return promhttp.Handler() }
