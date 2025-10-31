package binance

import "testing"

func TestRegistry(t *testing.T) {

	registry, err := NewExchangeRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.Symbols) < 1 {
		t.Fatal("No symbols found")
	}
	if len(registry.Markets) < 1 {
		t.Fatal("No markets found")
	}
}
