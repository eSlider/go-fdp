package v3

//Data is returned in ascending order. Oldest first, newest last.
//All time and timestamp related fields are in milliseconds.

const defaultBaseURL = "https://api.binance.com"

// The base endpoint https://data-api.binance.vision can be used to access the following API endpoints that have NONE as security type:
// GET /api/v3/aggTrades
// GET /api/v3/avgPrice
// GET /api/v3/depth
// GET /api/v3/exchangeInfo
// GET /api/v3/klines
// GET /api/v3/ping
// GET /api/v3/ticker
// GET /api/v3/ticker/24hr
// GET /api/v3/ticker/bookTicker
// GET /api/v3/ticker/price
// GET /api/v3/time
// GET /api/v3/trades
// GET /api/v3/uiKlines
const dataApiBaseURL = "https://data-api.binance.vision"

// https://developers.binance.com/docs/derivatives/portfolio-margin-pro/general-info#general-api-information
var BaseUrls = []string{
	defaultBaseURL,

	// The last 4 endpoints in the point above (api1-api4) might give better performance but have less stability. Please use whichever works best for your setup.
	"https://api1.binance.com",
	"https://api2.binance.com",
	"https://api3.binance.com",
	"https://api4.binance.com",
}

// AggTrades fetches compressed aggregate trades.
func AggTrades(req *AggTradeRequest) ([]*AggTrade, error) {
	return GetCast[AggTrade]("api/v3/aggTrades", req)
}

// Klines fetches kline/candlestick data.
func Klines(req *KlineRequest) ([]*Kline, error) {
	return GetCast[Kline]("api/v3/klines", req)
}
