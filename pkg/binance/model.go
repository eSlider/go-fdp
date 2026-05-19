package binance

import (
	"fmt"
	"strings"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
)

// Event - event to store daily informations
type Event struct {
	Date   time.Time
	Klines []KlineParquet
	Info   string
}

// ETLStatus - represents asset-processing status
type ETLStatus int

func (s *ETLStatus) String() any {
	return []string{
		"error",
		"zip-downloading",
		"zip-reading",
		"zip-ready",
		"csv-reading",
		"parquet-ready",
	}[*s]
}

const (
	StatusError ETLStatus = iota
	StatusZipDownloading
	StatusZipReading
	StatusZipReady
	StatusCSVReading
	StatusParquetReady
)

// AssetETLInfo - asset ETL info
type AssetETLInfo struct {
	Status ETLStatus    // Status of the asset ETL
	Buffer *data.Buffer // Buffer which should be used used to cache from zip -> csv file to parquet file
	Path   string       // Path to the asset zip file
	Err    error        // Error if any
	Info   string       // Additional info
}

func (i *AssetETLInfo) Done() bool {
	return i.Status == StatusParquetReady
}

type Frequency string

const (
	Monthly Frequency = "monthly"
	Daily             = "daily"
)

type FutureType string

func (f FutureType) String() string {
	return string(f)
}

const (
	FutureTypeCm FutureType = "cm" // Case Management
	FutureTypeUm            = "um" // Utilization Management
)

type FutureData string

const (
	// FutureDataAggTrades
	// 	- https://data.binance.vision/data/futures/um/daily/aggTrades/ACHUSDT/ACHUSDT-aggTrades-2025-10-26.zip
	FutureDataAggTrades FutureData = "aggTrades"

	// FutureDataBookDepth
	//	- https://data.binance.vision/data/futures/um/daily/bookDepth/AGIXBUSD/AGIXBUSD-bookDepth-2023-07-20.zip
	FutureDataBookDepth = "bookDepth"

	// FutureDataBookTicker
	//	- https://data.binance.vision/data/futures/um/daily/bookTicker/AGIXBUSD/AGIXBUSD-bookTicker-2023-07-20.zip
	FutureDataBookTicker = "bookTicker"

	// FutureDataIndexPriceKline https://data.binance.vision/data/futures/um/daily/indexPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2025-10-26.zip
	FutureDataIndexPriceKline = "indexPriceKline"

	// FutureDataKlines https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-10-26.zip
	FutureDataKlines             = "klines"
	FutureDataMarkPriceKlines    = "markPriceKline"
	FutureDataMetrics            = "metrics"
	FutureDataPremiumIndexKlines = "premiumIndexKline"
	FutureDataTrades             = "trades"
)

type MarketType string

const (
	Spot    MarketType = "spot"    // Spot - trading with immediate settlement
	Futures            = "futures" // Futures - trading with delayed settlement
	Option             = "option"  // Option - trading with the right, but not the obligation to buy or sell
)

func (m MarketType) String() string {
	return string(m)
}

func NewMarketType(s string) MarketType {
	if s == "" {
		return Spot
	}
	return MarketType(s)
}

type Indicator string

const (
	Klines    Indicator = "klines"
	Trades    Indicator = "trades"
	AggTrades Indicator = "aggTrades"
)

func (i Indicator) String() string {
	return string(i)
}

type HistoryAsset struct {
	MarketType
	Frequency
	data.Frame
	Indicator
	Market string
	Date   time.Time
}

func (q *HistoryAsset) String() string {
	return q.SymbolDateAssetZipLink()
}

// SymbolLink - is a link to a specific asset directory of a symbol
func (q *HistoryAsset) SymbolLink() string {
	if q.MarketType == "" {
		q.MarketType = Spot
	}
	if q.Frequency == "" {
		q.Frequency = Monthly
	}
	if q.Frame.String() == "" {
		q.Frame = data.Minute
	}
	if q.Indicator == "" {
		q.Indicator = Klines
	}

	link := fmt.Sprintf("data/%s/%s/%s/%s",
		q.MarketType,
		q.Frequency,
		q.Indicator,
		q.Market,
	)
	return link
}

// SymbolFrameLink - is a link to a specific asset and frame directory of zip files
func (q *HistoryAsset) SymbolFrameLink() string {
	// Indicator having no frame directories
	if q.Indicator == Klines {
		return fmt.Sprintf("%s/%s",
			q.SymbolLink(),
			q.Frame)
	}
	return q.SymbolLink()
}

// SymbolDateAssetZipLink - is a link to a concrete asset zip file
func (q *HistoryAsset) SymbolDateAssetZipLink() string {
	var layout string
	switch q.Frequency {
	case Monthly:
		layout = "2006-01"
	case Daily:
		layout = "2006-01-02"

	}
	switch q.Indicator {
	case Klines:
		return fmt.Sprintf("%s/%s-%s-%s.zip",
			q.SymbolFrameLink(),
			q.Market,
			q.Frame,
			q.Date.Format(layout))
	default:
		return fmt.Sprintf("%s/%s-%s-%s.zip",
			q.SymbolLink(),
			q.Market,
			q.Indicator,
			q.Date.Format(layout))
	}
}

// NewHistoryAssetByPath parse path to asset
// Example: - data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2023-06.zip
func NewHistoryAssetByPath(path string) (a *HistoryAsset, err error) {
	chunks := strings.Split(path, "/")
	a = &HistoryAsset{
		MarketType: MarketType(chunks[1]),
		Frequency:  Frequency(chunks[2]),
		Indicator:  Indicator(chunks[3]),
		Market:     chunks[4],
	}

	// The latest chunk is the file name
	fileName := strings.TrimRight(chunks[len(chunks)-1], ".zip")
	fileNameChunks := strings.Split(fileName, "-")
	l := len(fileNameChunks)

	switch a.Frequency {
	case Monthly: // Example: BTCUSDT-aggTrades-2025-10.zip
		a.Date, err = time.Parse("2006-01", fmt.Sprintf("%s-%s",
			fileNameChunks[l-2],
			fileNameChunks[l-1]))
	case Daily: // Example: BTCUSDT-aggTrades-2025-10-26.zip
		a.Date, err = time.Parse("2006-01-02",
			fmt.Sprintf("%s-%s-%s", fileNameChunks[l-3], fileNameChunks[l-2], fileNameChunks[l-1]))
	}

	switch a.Indicator {
	case Klines:
		a.Frame = data.StringToFrame(chunks[5])
	}

	return
}

// IsHistoryLinkAvailable - is a link to a concrete asset zip file
func (q *HistoryAsset) IsHistoryLinkAvailable() (err error) {
	// Market and Date are always required
	if q.Date.IsZero() {
		return fmt.Errorf("date is required")
	}

	// If the date is today, then there is no data for today
	if q.IsToday() {
		return fmt.Errorf("no data for today")
	}

	if q.MarketType == "" {
		return fmt.Errorf("market type is required")
	}

	if q.Market == "" {
		return fmt.Errorf("market is required")
	}

	// Frame is required only for klines
	if q.Indicator == Klines {
		if q.Frame == data.NoFrame {
			return fmt.Errorf("frame is required for klines")
		}
	}

	return nil
}

// ParquetPath - returns parquet file path
//   - Create link to work with hive partitioning
func (q *HistoryAsset) ParquetPath() string {
	// For aggTrades, don't include frame in the path since they don't have frames
	if q.Indicator == AggTrades {
		dest := fmt.Sprintf(
			"data/mtype=%s/indicator=%s/market=%s/year=%d/month=%d/day=%d/data.parquet",
			q.MarketType,
			q.Indicator,
			q.Market,
			q.Date.Year(),
			int(q.Date.Month()),
			q.Date.Day(),
		)
		return dest
	}

	dest := fmt.Sprintf(
		"data/mtype=%s/indicator=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/data.parquet",
		q.MarketType,
		q.Indicator,
		q.Market,
		q.Frame,
		q.Date.Year(),
		int(q.Date.Month()),
		q.Date.Day(),
	)
	return dest
}

// TodayDuckDBPath - returns the path to the DuckDB file for today's cached data
func (q *HistoryAsset) TodayDuckDBPath() string {
	// For aggTrades, don't include frame in the path since they don't have frames
	if q.Indicator == AggTrades {
		return fmt.Sprintf("data/mtype=%s/indicator=%s/market=%s/today.duckdb",
			q.MarketType,
			q.Indicator,
			q.Market)
	}
	return fmt.Sprintf("data/mtype=%s/indicator=%s/market=%s/frame=%s/today.duckdb",
		q.MarketType,
		q.Indicator,
		q.Market,
		q.Frame)
}

// TodayParquetDir - returns the directory for hourly parquet files for current day caching
func (q *HistoryAsset) TodayParquetDir() string {
	now := time.Now().UTC()
	// For aggTrades, don't include frame in the path since they don't have frames
	if q.Indicator == AggTrades {
		return fmt.Sprintf(
			"data/mtype=%s/indicator=%s/market=%s/year=%d/month=%d/day=%d/current",
			q.MarketType,
			q.Indicator,
			q.Market,
			now.Year(),
			int(now.Month()),
			now.Day(),
		)
	}
	return fmt.Sprintf(
		"data/mtype=%s/indicator=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/current",
		q.MarketType,
		q.Indicator,
		q.Market,
		q.Frame,
		now.Year(),
		int(now.Month()),
		now.Day(),
	)
}

// HourlyParquetPath - returns the path to an hourly parquet file for the given hour
func (q *HistoryAsset) HourlyParquetPath(hour int) string {
	return fmt.Sprintf("%s/hour_%02d.parquet", q.TodayParquetDir(), hour)
}

// IsToday - Check  date,s before now until midnight, handle using other api
func (q *HistoryAsset) IsToday() bool {
	return data.IsToday(q.Date)
}

// Transformer - interface for transforming data to several formats
type Transformer interface {
	Parquet() (any, error) // Parquet - transforms the data to parquet format
}
