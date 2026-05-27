package label

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabelAt_longTPFirst(t *testing.T) {
	high := []float64{100, 101, 105, 104}
	low := []float64{99, 100, 103, 102}
	close := []float64{100, 100.5, 104, 103}
	atr := []float64{1, 1, 1, 1}
	cfg := Config{TPMult: 2, SLMult: 1.5, MaxHold: 3}
	assert.Equal(t, OutcomeUp, LabelAt(high, low, close, atr, 0, cfg))
}

func TestLabelAt_longSLFirst(t *testing.T) {
	high := []float64{100, 100.5, 99, 98}
	low := []float64{100, 99, 97, 96}
	close := []float64{100, 99.5, 97.5, 97}
	atr := []float64{1, 1, 1, 1}
	cfg := Config{TPMult: 2, SLMult: 1.5, MaxHold: 3}
	assert.Equal(t, OutcomeDown, LabelAt(high, low, close, atr, 0, cfg))
}

func TestPurgeEmbargo(t *testing.T) {
	train, test := PurgeEmbargo(100, 0, 2, 5)
	assert.Len(t, test, 50)
	assert.True(t, len(train) < 100)
}
