package data

import "context"

// DrainParquet reads all records from parquet channels until both close.
func DrainParquet[T any](ctx context.Context, recordCh <-chan *T, errCh <-chan error) ([]*T, error) {
	var out []*T
	for recordCh != nil || errCh != nil {
		select {
		case record, ok := <-recordCh:
			if !ok {
				recordCh = nil
				continue
			}
			out = append(out, record)
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return out, nil
}
