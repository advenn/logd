// The testbed is a SEPARATE module on purpose.
//
// Go excludes nested modules from ./... , so the daemon's `go build ./...`, `go vet ./...`
// and its test suite are completely unaffected by anything in here. That matters twice
// over: the daemon's go.mod stays at its deliberate three dependencies, and the harness
// is free to take whatever dependencies a benchmark rig needs without touching the
// supply chain of the thing being benchmarked.
//
// The replace directive points at the parent so the harness can import the REAL
// core/extract compiler and core/config structs. Reimplementing template semantics here
// would let the linter and the daemon quietly disagree, which is the one bug this tool
// must never have.
module github.com/advenn/logd/testbed

go 1.26.2

require (
	github.com/advenn/logd v0.0.0-00010101000000-000000000000
	github.com/golang/snappy v1.0.0
	google.golang.org/protobuf v1.36.11
)

replace github.com/advenn/logd => ../
