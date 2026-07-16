//go:build !darwin && !linux && !freebsd

package observability

func processStats() (float64, int64) { return 0, 0 }
