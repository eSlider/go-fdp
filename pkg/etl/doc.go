// Package etl routes bulk and live ingestion jobs by data source (exchange).
// Register [BulkLoader] and [LiveSeries] implementations per [Source], then
// dispatch via [Router].
//
// See https://pkg.go.dev/github.com/eslider/go-fdp/pkg/etl
package etl
