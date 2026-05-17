package binance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync-v3/pkg/binance/v3"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"
	"time"

	"github.com/google/uuid"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var ErrNotZipLink = fmt.Errorf("not found")

//type CandleParquetQuery struct {
//	Market     string `validate:"required"`
//	Frame      string `validate:"required"`
//	Indicator  string `validate:"required"`
//	MarketType string `validate:"required"`
//	From       int64  `validate:"required"`
//	To         int64  `validate:"required"`
//	DataPath   string // Path to data directory
//	HivePath   string // Path to parquet files. Example: "*/*/*.parquet"
//}

// HistoryConsumer of binance historical assets
type HistoryConsumer struct {
	db         *sql.DB             // DuckDB
	ctx        context.Context     // Context
	cfg        *aws.Config         // AWS Config
	client     *s3.Client          // S3 Client
	downloader *manager.Downloader // S3 Downloader
	bucket     string              // Bucket
	localDir   string              // Local directory for downloaded files

	// Options
	recreateParquet bool // Recreate parquet files if they already exist
	storeZIP        bool // Store zip files locally flag
	storeCSV        bool // Store CSV files locally
}

type ListResult struct {
	Key  *string
	Page *s3.ListObjectsV2Output
	Obj  *types.Object
	Err  error
}

// NewAwsConfig returns aws config for binance data
// Ultimately this is just S3 with anonymous access and ap-northeast-1 region
// Alternatives:
//   - https://data.binance.vision/?prefix=data/spot/monthly/trades/0GBNB/
//   - https://data.binance.vision.s3.amazonaws.com/
func NewAwsConfig(ctx context.Context) (*aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("ap-northeast-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           "https://s3-ap-northeast-1.amazonaws.com",
					SigningRegion: "ap-northeast-1",
				}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown service: %s", service)
		})),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// List objects by paths
func (s *HistoryConsumer) List(path string) (ch chan ListResult) {
	ch = make(chan ListResult)
	go func() {
		// Close channel
		defer close(ch)

		// Create paginator
		list := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
			Bucket:  aws.String(s.bucket),
			MaxKeys: aws.Int32(100), // By default, 1000, but we need to iterate over all pages using callbacks
			Prefix:  aws.String(path),
		})

		// Iterate over pages
		for list.HasMorePages() {
			page, err := list.NextPage(s.ctx)

			// Handle error
			if err != nil {
				ch <- ListResult{
					Page: page,
					Err:  err,
				}
				continue
			}

			// Handle objects as they come extract paths
			for _, obj := range page.Contents {
				key := *obj.Key
				ch <- ListResult{
					Key:  &key,
					Page: page,
					Obj:  &obj,
				}
			}
		}

	}()
	return ch
}

// DownloadAndTransform - download, extract, transform and load data from binance history assets
func (s *HistoryConsumer) DownloadAndTransform(
	asset *HistoryAsset,
) (
	infoCh chan *AssetETLInfo,
	errCh chan error,
) {
	infoCh = make(chan *AssetETLInfo)
	errCh = make(chan error)

	go func() {
		defer close(infoCh)
		defer close(errCh)

		// If it's today, use hourly parquet caching
		if asset.IsToday() {
			switch asset.Indicator {
			case AggTrades:
				trades, err := s.FetchAndCacheCurrentDayAggTrades(asset)
				if err != nil {
					errCh <- fmt.Errorf("failed to fetch current day aggTrades: %w", err)
					return
				}
				parquetDir := fs.GetModuleRelativePath(asset.TodayParquetDir())
				infoCh <- &AssetETLInfo{
					Path:   parquetDir,
					Status: StatusParquetReady,
					Info:   fmt.Sprintf("Cached %d aggTrades in hourly parquet files", len(trades)),
				}
			default:
				// Klines
				candles, err := s.FetchAndCacheCurrentDay(asset)
				if err != nil {
					errCh <- fmt.Errorf("failed to fetch current day data: %w", err)
					return
				}
				parquetDir := fs.GetModuleRelativePath(asset.TodayParquetDir())
				infoCh <- &AssetETLInfo{
					Path:   parquetDir,
					Status: StatusParquetReady,
					Info:   fmt.Sprintf("Cached %d candles in hourly parquet files", len(candles)),
				}
			}
			return
		}

		if err := asset.IsHistoryLinkAvailable(); err != nil {
			errCh <- errors.Join(ErrNotZipLink, fmt.Errorf("invalid asset: %s", asset))
			return
		}

		link := asset.SymbolDateAssetZipLink()
		parquetPath := fs.GetModuleRelativePath(asset.ParquetPath())

		// Check if a file already exists, then skip downloading, transforming and loading
		if fs.FileExists(parquetPath) {
			infoCh <- &AssetETLInfo{
				Path:   parquetPath,
				Status: StatusParquetReady,
				Info:   fmt.Sprintf("Parquet file already exists: %s", parquetPath),
			}
			return
		}

		// Download and cache zip file
		zipInfo := s.CacheZip(link)
		if zipInfo.Err != nil {
			errCh <- errors.Join(zipInfo.Err, fmt.Errorf("error caching zip file: %v", zipInfo.Err))
			return
		}

		if zipInfo.Status == StatusZipReady {
			// Decompress ZIP file
			csvBuffer, err := data.Decompress(zipInfo.Buffer.Bytes())
			if err != nil {
				errCh <- fmt.Errorf("error decompressing zip file: %v", err)
				return
			}

			if s.storeCSV {
				// Store CSV file parallel to ZIP
				csvPath := fs.GetModuleRelativePath(strings.TrimSuffix(link, ".zip") + ".csv")
				infoCh <- &AssetETLInfo{
					Path:   csvPath,
					Status: StatusCSVReading,
					Buffer: csvBuffer,
					Err:    csvBuffer.Persist(csvPath),
				}
			}

			// Handle different indicators
			switch asset.Indicator {
			case AggTrades:
				parquetWriteCh, prqErrCh := data.WriteParquet[v3.AggTradeParquet](parquetPath)
				wroteAggTrades := 0

				// Read CSV and write to parquet
			aggTradesLoop:
				for row := range data.ReadHeaderlessCSV[v3.AggTrade](csvBuffer) {
					if row.Error != nil {
						errCh <- fmt.Errorf("error reading csv: %v", row.Error)
						continue
					}
					parquet, err := row.Value.Parquet()
					if err != nil {
						errCh <- fmt.Errorf("error converting csv entry to parquet: %v", err)
						continue
					}

					select {
					case parquetWriteCh <- parquet:
						wroteAggTrades++
					case err := <-prqErrCh:
						errCh <- fmt.Errorf("error writing parquet: %v", err)
						break aggTradesLoop
					}
				}
				close(parquetWriteCh)

				for err := range prqErrCh {
					errCh <- fmt.Errorf("error writing parquet: %v", err)
				}

				infoCh <- &AssetETLInfo{
					Path:   parquetPath,
					Status: StatusParquetReady,
					Info:   fmt.Sprintf("Wrote %d aggTrades to parquet", wroteAggTrades),
				}
			case Klines:
				parquetWriteCh, prqErrCh := data.WriteParquet[v3.KlineParquet](parquetPath)
				wroteKlines := 0

				// Read CSV and write to parquet
			klinesLoop:
				for row := range data.ReadHeaderlessCSV[v3.Kline](csvBuffer) {
					if row.Error != nil {
						errCh <- fmt.Errorf("error reading csv: %v", row.Error)
						continue
					}
					parquet, err := row.Value.Parquet()
					if err != nil {
						errCh <- fmt.Errorf("error converting csv entry to parquet: %v", err)
						continue
					}

					select {
					case parquetWriteCh <- parquet:
						wroteKlines++
					case err := <-prqErrCh:
						errCh <- fmt.Errorf("error writing parquet: %v", err)
						break klinesLoop
					}
				}
				close(parquetWriteCh)

				for err := range prqErrCh {
					errCh <- fmt.Errorf("error writing parquet: %v", err)
				}

				infoCh <- &AssetETLInfo{
					Path:   parquetPath,
					Status: StatusParquetReady,
					Info:   fmt.Sprintf("Wrote %d klines to parquet", wroteKlines),
				}
			default:
				errCh <- fmt.Errorf("unsupported indicator: %s", asset.Indicator)
				return
			}
		}

	}()
	return
}

// Download file from S3
//
//	Example: - data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2023-06.zip
func (s *HistoryConsumer) Download(path string, w io.WriterAt) (n int64, err error) {
	download, err := s.downloader.Download(s.ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	return download, err
}

// CacheZip - download and cache zip file
func (s *HistoryConsumer) CacheZip(link string) (info *AssetETLInfo) {
	// Check if a file already locally exists
	// ReadCSV existing file
	var err error
	path := fs.GetModuleRelativePath(link)
	info = &AssetETLInfo{Path: path}

	// Do we cache the file?
	if fs.FileExists(path) {
		// Read zip
		info.Status = StatusZipReading
		if info.Buffer, err = data.ReadIntoBuffer(path); err != nil {
			info.Err = fmt.Errorf("error reading zip file: %v", err)
			return
		}
	} else {
		// Download the file if it doesn't exist
		info.Status = StatusZipDownloading
		info.Buffer = &data.Buffer{}
		if _, err1 := s.Download(link, info.Buffer); err1 != nil {
			info.Err = fmt.Errorf("error downloading zip file: %v", err1)
			return
		}

		go func() {
			if s.storeZIP {
				// Store a downloaded file into a local directory save it as a zip file
				if err := info.Buffer.Persist(path); err != nil {
					fmt.Printf("error storing zip file: %v\n", err)
				}
			}
		}()
	}

	info.Status = StatusZipReady
	return info
}

// GetAssets ETLBinanceHistoryAsset - download, extract, transform and load data from binance history assets
//   - If ZIP file already exists, load it from disk
//   - If CSV file already exists, load it from disk
//   - If a parquet file already exists, load it from disk
//   - Check if a parquet file is empty, if so, delete it and recreate it
func (s *HistoryConsumer) GetAssets(asset *HistoryAsset) (info chan *AssetETLInfo) {
	prefix := asset.SymbolFrameLink()
	info = make(chan *AssetETLInfo)

	go func() {
		for result := range s.List(prefix) {
			// Handle error
			if result.Err != nil {
				info <- &AssetETLInfo{
					Path:   *result.Key,
					Status: StatusError,
					Err:    fmt.Errorf("error listing files: %v", result.Err),
				}
				continue
			}
			path := *result.Key

			// Skip CHECKSUM and folders
			if strings.HasSuffix(path, "CHECKSUM") || strings.HasSuffix(path, "/") {
				continue
			}

			zipAsset, err := NewHistoryAssetByPath(path)
			if err != nil {
				info <- &AssetETLInfo{
					Path:   path,
					Status: StatusError,
					Err:    fmt.Errorf("error creating asset path: %v", err),
				}
			}

			assetInfoCh, assetErrCh := s.DownloadAndTransform(zipAsset)
			for inCh, erCh := assetInfoCh, assetErrCh; inCh != nil || erCh != nil; {
				select {
				case assetInfo, ok := <-inCh:
					if !ok {
						inCh = nil
						continue
					}
					info <- assetInfo
				case assetErr, ok := <-erCh:
					if !ok {
						erCh = nil
						continue
					}
					info <- &AssetETLInfo{
						Path:   path,
						Status: StatusError,
						Err:    fmt.Errorf("error downloading and transforming asset: %v", assetErr),
					}
				}
			}
		}
		close(info)
	}()

	return info
}

func NewHistoryConsumer(ctx context.Context) (*HistoryConsumer, error) {
	cfg, err := NewAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(*cfg)
	downloader := manager.NewDownloader(client)
	return &HistoryConsumer{
		recreateParquet: false,
		storeZIP:        false,
		bucket:          "data.binance.vision",
		client:          client,
		downloader:      downloader,
		cfg:             cfg,
		ctx:             ctx,
	}, nil
}

// FetchAndCacheCurrentDay fetches current day data from API and caches into hourly parquet files
func (s *HistoryConsumer) FetchAndCacheCurrentDay(asset *HistoryAsset) ([]*v3.Kline, error) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	var allCandles []*v3.Kline

	// Process each completed hour
	currentHour := now.Hour()
	for hour := 0; hour <= currentHour; hour++ {
		hourStart := midnight.Add(time.Duration(hour) * time.Hour)
		hourEnd := hourStart.Add(time.Hour)

		// For the current hour, use current time as end
		isCurrentHour := hour == currentHour
		if isCurrentHour {
			hourEnd = now
		}

		parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))

		// Check if we already have cached data for completed hours
		if !isCurrentHour && fs.FileExists(parquetPath) {
			// Read from cache for completed hours
			candles, err := s.readHourlyParquet(parquetPath, midnight)
			if err == nil && len(candles) > 0 {
				allCandles = append(allCandles, candles...)
				continue
			}
		}

		if isCurrentHour {
			// Refresh last hour logic
			// If file exists, read it to get last time, then fetch new data and merge
			if err := s.RefreshLastHour(asset); err != nil {
				return nil, fmt.Errorf("failed to refresh last hour: %w", err)
			}
			// Read updated data
			candles, err := s.readHourlyParquet(parquetPath, midnight)
			if err != nil {
				return nil, fmt.Errorf("failed to read updated hour %d parquet: %w", hour, err)
			}
			allCandles = append(allCandles, candles...)
			continue
		}

		// Fetch from API for completed hours (if not cached)
		candles, err := s.fetchHourData(asset, hourStart, hourEnd)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch hour %d data: %w", hour, err)
		}

		if len(candles) > 0 {
			// Cache to parquet file
			if err := s.writeHourlyParquet(parquetPath, candles); err != nil {
				return nil, fmt.Errorf("failed to write hourly parquet: %w", err)
			}
			allCandles = append(allCandles, candles...)
		}
	}

	return allCandles, nil
}

// FetchAndCacheCurrentDayAggTrades fetches current day aggTrades from API and caches into hourly parquet files
// Returns data even if caching fails (logs warnings for cache errors)
func (s *HistoryConsumer) FetchAndCacheCurrentDayAggTrades(asset *HistoryAsset) ([]*v3.AggTrade, error) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	var allTrades []*v3.AggTrade

	// Process each completed hour
	currentHour := now.Hour()
	for hour := 0; hour <= currentHour; hour++ {
		hourStart := midnight.Add(time.Duration(hour) * time.Hour)
		hourEnd := hourStart.Add(time.Hour)

		// For the current hour, use current time as end
		isCurrentHour := hour == currentHour
		if isCurrentHour {
			hourEnd = now
		}

		parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))

		// Check if we already have cached data for completed hours
		if !isCurrentHour && fs.FileExists(parquetPath) {
			// Read from cache for completed hours
			trades, err := s.readHourlyAggTradesParquet(parquetPath, midnight)
			if err == nil && len(trades) > 0 {
				allTrades = append(allTrades, (trades)...)
				continue
			}
		}

		if isCurrentHour {
			// Refresh last hour - fetch directly from API
			trades, err := s.fetchCurrentHourAggTrades(asset)
			if err != nil {
				fmt.Printf("Warning: failed to fetch current hour aggTrades: %v\n", err)
			} else {
				allTrades = append(allTrades, trades...)
				// Try to cache (best effort, don't fail on error)
				if err := s.writeHourlyAggTradesParquet(parquetPath, trades); err != nil {
					fmt.Printf("Warning: failed to cache current hour aggTrades: %v\n", err)
				}
			}
			continue
		}

		// Fetch from API for completed hours (if not cached)
		trades, err := s.fetchHourAggTradesData(asset, hourStart, hourEnd)
		if err != nil {
			fmt.Printf("Warning: failed to fetch hour %d aggTrades: %v\n", hour, err)
			continue // Continue with other hours
		}

		if len(trades) > 0 {
			allTrades = append(allTrades, trades...)
			// Try to cache (best effort, don't fail on error)
			if err := s.writeHourlyAggTradesParquet(parquetPath, trades); err != nil {
				fmt.Printf("Warning: failed to cache hour %d aggTrades: %v\n", hour, err)
			}
		}
	}

	return allTrades, nil
}

// fetchCurrentHourAggTrades fetches aggTrades for the current hour from API
func (s *HistoryConsumer) fetchCurrentHourAggTrades(asset *HistoryAsset) ([]*v3.AggTrade, error) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	currentHour := now.Hour()
	hourStart := midnight.Add(time.Duration(currentHour) * time.Hour)

	return s.fetchHourAggTradesData(asset, hourStart, now)
}

// RefreshLastHourAggTrades refreshes the last hour's aggTrades parquet file by fetching new data from API
func (s *HistoryConsumer) RefreshLastHourAggTrades(asset *HistoryAsset) error {
	now := time.Now().UTC()
	currentHour := now.Hour()
	parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(currentHour))

	// Read last trade ID from existing file to avoid re-fetching all data
	var lastTradeID int64 = 0
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	hourStart := midnight.Add(time.Duration(currentHour) * time.Hour)

	if fs.FileExists(parquetPath) {
		trades, err := s.readHourlyAggTradesParquet(parquetPath, midnight)
		if err == nil && len(trades) > 0 {
			// Get the last trade to continue from
			lastTradeID = trades[len(trades)-1].AggTradeID
		}
	}

	// Fetch new data
	var newTrades []*v3.AggTrade
	var err error

	if lastTradeID > 0 {
		// Fetch from last trade ID
		newTrades, err = s.fetchAggTradesFromID(asset, lastTradeID+1)
	} else {
		// Fetch from hour start
		newTrades, err = s.fetchHourAggTradesData(asset, hourStart, now)
	}

	if err != nil {
		return fmt.Errorf("failed to fetch new aggTrades: %w", err)
	}

	if len(newTrades) == 0 {
		return nil // No new data
	}

	// Read existing trades and merge
	var existingTrades []*v3.AggTrade
	if fs.FileExists(parquetPath) {
		existingTrades, _ = s.readHourlyAggTradesParquet(parquetPath, midnight)
	}

	// Merge: keep existing trades that are not in new data
	merged := make(map[int64]*v3.AggTrade)
	for _, t := range existingTrades {
		merged[t.AggTradeID] = t
	}
	for _, t := range newTrades {
		merged[t.AggTradeID] = t // New data overwrites existing
	}

	// Convert back to slice and sort
	var allTrades []*v3.AggTrade
	for _, t := range merged {
		allTrades = append(allTrades, t)
	}

	// Sort by AggTradeID
	sortAggTradesByID(allTrades)

	// Write merged data back
	return s.writeHourlyAggTradesParquet(parquetPath, allTrades)
}

// fetchHourAggTradesData fetches aggTrades data for a specific time range
func (s *HistoryConsumer) fetchHourAggTradesData(asset *HistoryAsset, start, end time.Time) ([]*v3.AggTrade, error) {
	return v3.AggTrades(&v3.AggTradeRequest{
		Base: v3.SymbolRequest{
			Symbol:    asset.Market,
			StartTime: new(start.UnixMilli()),
			EndTime:   new(end.UnixMilli()),
		},
		Limit: 1000,
	})
}

// fetchAggTradesFromID fetches aggTrades starting from a specific trade ID
func (s *HistoryConsumer) fetchAggTradesFromID(asset *HistoryAsset, fromID int64) ([]*v3.AggTrade, error) {
	return v3.AggTrades(&v3.AggTradeRequest{
		Base: v3.SymbolRequest{
			Symbol: asset.Market,
		},
		FromID: &fromID,
		Limit:  1000,
	})
}

// readHourlyAggTradesParquet reads aggTrades from an hourly parquet file
func (s *HistoryConsumer) readHourlyAggTradesParquet(path string, date time.Time) ([]*v3.AggTrade, error) {
	recordCh, errCh := data.ReadParquet[v3.AggTradeParquet](path)

	var trades []*v3.AggTrade
	for done := false; !done; {
		select {
		case record, ok := <-recordCh:
			if !ok {
				done = true
				continue
			}
			// Convert ParquetAggTrade back to AggTrade
			trades = append(trades, record.ToAggTrade(date))
		case err, ok := <-errCh:
			if !ok {
				done = true
				continue
			}
			return nil, err
		}
	}

	return trades, nil
}

// writeHourlyAggTradesParquet writes aggTrades to an hourly parquet file atomically
func (s *HistoryConsumer) writeHourlyAggTradesParquet(path string, trades []*v3.AggTrade) error {
	// Convert to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for %s: %w", path, err)
	}
	path = absPath

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to a temp file first to avoid race conditions
	tempPath := fmt.Sprintf("%s.%s.tmp", path, uuid.New().String()[:8])
	writeCh, errCh := data.WriteParquet[v3.AggTradeParquet](tempPath)

	// Write all trades
	for _, trade := range trades {
		parquet, err := trade.Parquet()
		if err != nil {
			close(writeCh)
			os.Remove(tempPath)
			return fmt.Errorf("failed to convert aggTrade to parquet: %w", err)
		}
		select {
		case writeCh <- parquet:
		case err := <-errCh:
			os.Remove(tempPath)
			return fmt.Errorf("failed to write parquet: %w", err)
		}
	}
	close(writeCh)

	// Wait for errors
	for err := range errCh {
		os.Remove(tempPath)
		return fmt.Errorf("failed to write parquet: %w", err)
	}

	// Verify temp file exists before renaming
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return fmt.Errorf("temp parquet file was not created: %s", tempPath)
	}

	// Atomic rename
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// sortAggTradesByID sorts aggTrades by AggTradeID in ascending order
func sortAggTradesByID(trades []*v3.AggTrade) {
	for i := 0; i < len(trades)-1; i++ {
		for j := i + 1; j < len(trades); j++ {
			if trades[i].AggTradeID > trades[j].AggTradeID {
				trades[i], trades[j] = trades[j], trades[i]
			}
		}
	}
}

// ReadCachedCurrentDay reads cached current day data from hourly parquet files
func (s *HistoryConsumer) ReadCachedCurrentDay(asset *HistoryAsset) ([]*v3.Kline, error) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	currentHour := now.Hour()

	var allCandles []*v3.Kline

	// If it's the current day, we might need to refresh the last hour data first
	if asset.IsToday() {
		if err := s.RefreshLastHour(asset); err != nil {
			// Log error but continue with what we have
			fmt.Printf("Failed to refresh last hour data: %v\n", err)
		}
	}

	for hour := 0; hour <= currentHour; hour++ {
		parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))

		if !fs.FileExists(parquetPath) {
			continue
		}

		candles, err := s.readHourlyParquet(parquetPath, midnight)
		if err != nil {
			return nil, fmt.Errorf("failed to read hour %d parquet: %w", hour, err)
		}

		allCandles = append(allCandles, candles...)
	}

	return allCandles, nil
}

// RefreshLastHour refreshes the last hour's parquet file by fetching new data from API
func (s *HistoryConsumer) RefreshLastHour(asset *HistoryAsset) error {
	now := time.Now().UTC()
	currentHour := now.Hour()
	parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(currentHour))

	// Read last candle time from existing file to avoid re-fetching all data
	var lastOpenTime int64 = 0
	// Calculate start time for API call
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	if fs.FileExists(parquetPath) {
		candles, err := s.readHourlyParquet(parquetPath, midnight)
		if err == nil && len(candles) > 0 {
			lastOpenTime = candles[len(candles)-1].OpenTime
		}
	}

	hourStart := midnight.Add(time.Duration(currentHour) * time.Hour)

	// If we have existing data, start from last candle time
	if lastOpenTime > 0 {
		lastTime := data.AnyTimestampToTime(lastOpenTime)
		if lastTime != nil && lastTime.After(hourStart) {
			hourStart = *lastTime
		}
	}

	// Fetch new data
	newCandles, err := s.fetchHourData(asset, hourStart, now)
	if err != nil {
		return fmt.Errorf("failed to fetch new data: %w", err)
	}

	if len(newCandles) == 0 {
		return nil // No new data
	}

	// Read existing candles and merge
	var existingCandles []*v3.Kline
	if fs.FileExists(parquetPath) {
		existingCandles, _ = s.readHourlyParquet(parquetPath, midnight)
	}

	// Merge: keep existing candles that are not in new data
	merged := make(map[int64]*v3.Kline)
	for _, c := range existingCandles {
		merged[c.OpenTime] = c
	}
	for _, c := range newCandles {
		merged[c.OpenTime] = c // New data overwrites existing
	}

	// Convert back to slice and sort
	var allCandles []*v3.Kline
	for _, c := range merged {
		allCandles = append(allCandles, c)
	}

	// Sort by OpenTime
	sortKlinesByOpenTime(allCandles)

	// Write merged data back
	return s.writeHourlyParquet(parquetPath, allCandles)
}

// fetchHourData fetches kline data for a specific time range
func (s *HistoryConsumer) fetchHourData(asset *HistoryAsset, start, end time.Time) ([]*v3.Kline, error) {
	// Convert to milliseconds for API
	return v3.Klines(&v3.KlineRequest{
		Base: v3.SymbolRequest{
			Symbol:    asset.Market,
			StartTime: new(start.UnixMilli()),
			EndTime:   new(end.UnixMilli()),
		},
		Interval: asset.Frame.String(),
		Limit:    1000,
	})
}

// readHourlyParquet reads klines from an hourly parquet file
func (s *HistoryConsumer) readHourlyParquet(path string, date time.Time) ([]*v3.Kline, error) {
	recordCh, errCh := data.ReadParquet[v3.KlineParquet](path)

	var klines []*v3.Kline
	for done := false; !done; {
		select {
		case record, ok := <-recordCh:
			if !ok {
				done = true
				continue
			}
			// Convert ParquetKline back to Kline
			klines = append(klines, record.ToKline(date))
		case err, ok := <-errCh:
			if !ok {
				done = true
				continue
			}
			return nil, err
		}
	}

	return klines, nil
}

// writeHourlyParquet writes klines to an hourly parquet file atomically
// Uses temp file + rename to prevent race conditions with readers
func (s *HistoryConsumer) writeHourlyParquet(path string, klines []*v3.Kline) error {
	// Convert to absolute path to avoid relative path resolution issues
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for %s: %w", path, err)
	}
	path = absPath

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to a temp file first to avoid race conditions
	// Use unique suffix to prevent concurrent requests from interfering
	tempPath := fmt.Sprintf("%s.%s.tmp", path, uuid.New().String()[:8])
	writeCh, errCh := data.WriteParquet[v3.KlineParquet](tempPath)

	// Write all klines
	for _, kline := range klines {
		parquet, err := kline.Parquet()
		if err != nil {
			close(writeCh)
			os.Remove(tempPath) // Clean up temp file
			return fmt.Errorf("failed to convert kline to parquet: %w", err)
		}
		select {
		case writeCh <- parquet:
		case err := <-errCh:
			os.Remove(tempPath)
			return fmt.Errorf("failed to write parquet: %w", err)
		}
	}
	close(writeCh)

	// Wait for errors
	for err := range errCh {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to write parquet: %w", err)
	}

	// Verify temp file exists before renaming
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return fmt.Errorf("temp parquet file was not created: %s", tempPath)
	}

	// Atomic rename: temp file -> final path
	// This ensures readers only see complete files
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// sortKlinesByOpenTime sorts klines by OpenTime in ascending order
func sortKlinesByOpenTime(klines []*v3.Kline) {
	for i := 0; i < len(klines)-1; i++ {
		for j := i + 1; j < len(klines); j++ {
			if klines[i].OpenTime > klines[j].OpenTime {
				klines[i], klines[j] = klines[j], klines[i]
			}
		}
	}
}
