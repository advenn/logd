package query

// DisableCostGuard makes e fetch every candidate set by offset, however large, so a test can
// time the index path in the cases where the guard would have scanned.
func DisableCostGuard(e *Engine) { e.noCostGuard = true }
