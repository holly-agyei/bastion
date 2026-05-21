// Package ui exposes the embedded HTML dashboard. It's a single file served
// at /ui that polls /api/metrics, /api/alerts, and /api/config every second.
package ui

import _ "embed"

//go:embed index.html
var IndexHTML []byte
