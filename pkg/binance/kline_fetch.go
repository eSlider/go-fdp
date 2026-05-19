package binance

import (
	"context"
	"fmt"
)

const defaultKlineLimit int64 = 1000

// KlinesAll fetches all klines in the requested window, paginating while responses are full.
func FetchKlinesAll(ctx context.Context, req *KlineRequest) ([]*Kline, error) {
	if req == nil {
		return nil, fmt.Errorf("kline request is nil")
	}
	limit := req.Limit
	if limit <= 0 {
		limit = defaultKlineLimit
	}

	var all []*Kline
	start := req.Base.StartTime
	for page := 0; page < 10000; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		pageReq := *req
		pageReq.Limit = limit
		pageReq.Base = req.Base
		if start != nil {
			s := *start
			pageReq.Base.StartTime = &s
		}

		batch, err := FetchKlines(ctx, &pageReq)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		all = append(all, batch...)

		if int64(len(batch)) < limit {
			break
		}
		last := batch[len(batch)-1].OpenTime
		nextStart := last + 1
		if req.Base.EndTime != nil && nextStart > *req.Base.EndTime {
			break
		}
		start = &nextStart
	}
	return all, nil
}
