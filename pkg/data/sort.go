package data

import (
	"cmp"
	"slices"
)

// SortBy sorts s in place by key ascending.
func SortBy[S ~[]E, E any, K cmp.Ordered](s S, key func(E) K) {
	slices.SortFunc(s, func(a, b E) int {
		return cmp.Compare(key(a), key(b))
	})
}
