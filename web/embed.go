// Package web holds the live dashboard, served by the API at /.
package web

import "embed"

//go:embed index.html archivo.woff2
var FS embed.FS
