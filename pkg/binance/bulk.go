package binance

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/fs"
)

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
				parquetWriteCh, prqErrCh := data.WriteParquet[AggTradeParquet](parquetPath)
				wroteAggTrades := 0

				// Read CSV and write to parquet
			aggTradesLoop:
				for row := range data.ReadHeaderlessCSV[AggTrade](csvBuffer) {
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
				parquetWriteCh, prqErrCh := data.WriteParquet[KlineParquet](parquetPath)
				wroteKlines := 0

				// Read CSV and write to parquet
			klinesLoop:
				for row := range data.ReadHeaderlessCSV[Kline](csvBuffer) {
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
