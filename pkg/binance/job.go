package binance

import "github.com/eslider/go-binance-fdp/pkg/etl"

// JobFromAsset maps a history asset to an ETL job.
func JobFromAsset(asset *HistoryAsset) etl.Job {
	return etl.Job{
		Source:     etl.SourceBinance,
		MarketType: string(asset.MarketType),
		Market:     asset.Market,
		Indicator:  string(asset.Indicator),
		Frame:      asset.Frame,
		Date:       asset.Date,
	}
}

// AssetFromJob maps an ETL job to a history asset.
func AssetFromJob(job etl.Job) *HistoryAsset {
	return &HistoryAsset{
		MarketType: MarketType(job.MarketType),
		Frequency:  Daily,
		Frame:      job.Frame,
		Indicator:  Indicator(job.Indicator),
		Market:     job.Market,
		Date:       job.Date,
	}
}
