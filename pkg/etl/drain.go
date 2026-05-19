package etl

import "context"

// DrainETL consumes progress and error channels until both are closed.
func DrainETL[T any](ctx context.Context, infoCh <-chan T, errCh <-chan error) error {
	for infoCh != nil || errCh != nil {
		select {
		case _, ok := <-infoCh:
			if !ok {
				infoCh = nil
			}
		case err, ok := <-errCh:
			if ok {
				return err
			}
			errCh = nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
