package binance

import "sync-v3/pkg/binance/v3"

func aggTradesFromV3(in []v3.AggTrade) []*AggTrade {
	out := make([]*AggTrade, len(in))
	for i, t := range in {
		out[i] = &AggTrade{
			AggTradeID:       t.AggTradeID,
			Price:            t.Price,
			Quantity:         t.Quantity,
			FirstTradeID:     t.FirstTradeID,
			LastTradeID:      t.LastTradeID,
			Timestamp:        t.Timestamp,
			IsBuyerMaker:     t.IsBuyerMaker,
			IsBestPriceMatch: t.IsBestPriceMatch,
		}
	}
	return out
}

func klinesFromV3(in []v3.Kline) []*Kline {
	out := make([]*Kline, len(in))
	for i, k := range in {
		out[i] = &Kline{
			OpenTime:       k.OpenTime,
			OpenPrice:      k.OpenPrice,
			HighPrice:      k.HighPrice,
			LowPrice:       k.LowPrice,
			ClosePrice:     k.ClosePrice,
			Volume:         k.Volume,
			CloseTime:      k.CloseTime,
			QuoteVolume:    k.QuoteVolume,
			NumberOfTrades: k.NumberOfTrades,
			TakerBuyVolume: k.TakerBuyVolume,
			TakerBuyQuote:  k.TakerBuyQuote,
			Ignore:         k.Ignore,
		}
	}
	return out
}
