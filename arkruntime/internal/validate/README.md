# Generated-model validation compatibility

This package mirrors the validator interfaces emitted by ogen v1.20.3. The
String, Int, Float, Array and constraint error implementations are adapted from
[ogen's Apache-2.0 sources](https://github.com/ogen-go/ogen/tree/v1.20.3/validate).
The original error aggregation and sentinel shim remains in `validate.go`.
Do not replace configured validators with no-op stubs: current schemas set real
bounds, string/array lengths, and patterns.

`../ogenregex` implements the emitted regex interface using the same regexp2
ECMAScript/Unicode fallback mode and 15-second timeout as ogen v1.20.3. It also preserves ogen's RE2 conversion fast path, including ECMAScript
Unicode whitespace and line-terminator semantics. The conversion code retains
its goja attribution; the upstream MIT notice is in `../ogenregex/GOJA_LICENSE`. The SDK depends on regexp2
v1.11.5 and x/text v0.14.0, not the full ogen module, and keeps Go 1.20.

Ark-apis `vendor-<api>` rewrites both ogen `validate` and `ogenregex` imports to
these internal packages. Their files are outside the generated `model/<api>`
directories and survive sync. Hand-written model codec tests end in
`_shim_test.go` and are also preserved by the vendor rule. Merge the SDK fix so
subsequent sync branches based on main inherit it; an unmerged branch is not the
source for later syncs.

After any schema/generator/vendor change, run `go build ./...`, `go test ./...`
and `go vet ./...` against the **vendored SDK**, including with Go 1.20.
The package tests cover validator bounds and regex behavior independently of
generated models. Add model-constraint and recursive JSON codec regressions
alongside the corresponding generated-model sync.
Generated `gen/go` tests using the upstream ogen library alone cannot detect
an incomplete shim. New emitted symbols still require a compatibility review.
