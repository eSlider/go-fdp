package binance

import (
	"fmt"
	"os"
	"strings"

	"github.com/eslider/go-fdp/pkg/fs"

	"github.com/jszwec/csvutil"
)

// JSON keys in symbols.json: symbol, type, description.
//
// Keep the field names and tags stable: other code may rely on them

// Symbol describes a tradable or reference asset symbol.
type Symbol struct {
	Name        string `csv:"Symbol"`      //  Name maps to the JSON field "symbol".
	Type        string `csv:"Type"`        // Type can be: crypto, fiat, stable, index, ...
	Description string `csv:"Description"` // Description is a human-friendly name.
}

type Market struct {
	Name    string
	Symbols []*Symbol
}

type ExchangeRegistry struct {
	Symbols []*Symbol
	Markets []*Market
}

// reg is should be used as a singleton
var reg *ExchangeRegistry

// NewExchangeRegistry creates a new exchange registry
func NewExchangeRegistry() (*ExchangeRegistry, error) {
	if reg != nil {
		return reg, nil
	}

	var symbols []*Symbol

	cryptoCSV, err := os.ReadFile(fs.GetModuleRelativePath("data/cryptos.csv"))
	if err != nil {
		return nil, fmt.Errorf("failed to read cryptos.csv: %w", err)
	}

	if err := csvutil.Unmarshal(cryptoCSV, &symbols); err != nil {
		return nil, err
	}

	marketsTXT, err := os.ReadFile(fs.GetModuleRelativePath("data/markets.txt"))
	if err != nil {
		return nil, fmt.Errorf("failed to read markets.txt: %w", err)
	}

	var markets []*Market
	for _, v := range strings.Split(string(marketsTXT), ",") {

		// Create market
		m := &Market{
			Name: v,
		}

		// Determine symbol names
		for _, s := range symbols {
			if strings.HasPrefix(v, s.Name) {
				m.Symbols = append(m.Symbols, s)
			} else if strings.HasSuffix(v, s.Name) {
				m.Symbols = append(m.Symbols, s)
			}
			if len(m.Symbols) > 1 {
				break
			}
		}

		markets = append(markets, m)
	}

	reg = &ExchangeRegistry{
		Symbols: symbols,
		Markets: markets,
	}
	return reg, nil
}
