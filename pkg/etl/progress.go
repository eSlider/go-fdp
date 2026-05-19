package etl

// Progress reports ETL pipeline status for one asset path.
type Progress struct {
	Status string
	Path   string
	Info   string
	Err    error
}
