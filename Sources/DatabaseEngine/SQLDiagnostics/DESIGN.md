# SQL Diagnostics

## Purpose and Scope
Framework-owned typed SQL failure classification. Parent: [DatabaseEngine](../DESIGN.md). Children: none.

## Responsibilities and Boundaries
Kit owns validated portable SQLDiagnostic values. This component maps existing typed runtime cases into SQLSTATE and retains the original native error. Database owns original-input byte-to-character location adaptation; no semantic input receives an invented location.

## Related Designs
| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [DatabaseEngine](../DESIGN.md) | parent | execution errors | Classifies real failures. | No effect or cleanup ownership. |
| [SQLConstraints](../SQLConstraints/DESIGN.md) | depends on | typed violation and identifiers | Captures metadata before rollback. | Names never classify violations. |
| [Kit SQL](../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | SQLDiagnostic | Portable diagnostic value. | Native causes remain here. |

## Architecture
```text
native typed failure -> SQLDiagnostics -> Kit SQLDiagnostic
        |                                  |
        +-------- SQLExecutionFailure -----+
primary + mandatory cleanup composite remains the retained native error
```

## Contracts and Invariants
Classification inspects typed cases only. Unknown errors produce explicit XX000 internal-error diagnostics. SQLExecutionFailure retains the exact underlying error, including composite primary and cleanup errors; classification follows the primary operation without discarding cleanup. All values are immutable and Sendable across Native, WASM and Embedded. No reflection or dynamic type metadata is required.

### Request parameter binding failures
The closed Kit-owned `QueryParameterBindingError` algebra is classified at the
Framework diagnostic boundary while `SQLExecutionFailure` retains its exact
native cause. Missing positional or named bindings map to `42P02`
(`undefined_parameter`, name category). Invalid or duplicate positions/names map
to `22023` (`invalid_parameter_value`, data category). Unsupported values map to
`0A000` (unsupported category). Invalid structure delegates to the existing typed
structural classification, including resource admission. An invalid traversal
state remains `XX000` (internal-error category). These SQLSTATE meanings follow
the [PostgreSQL 18 error-code appendix](https://www.postgresql.org/docs/18/errcodes-appendix.html).

Request parameter validation precedes statement execution and does not own an
expression occurrence. It does not fabricate a source position or identifier;
text adapters retain a position only when their actual source owner supplies it.

## Verification and Change Impact
S16SQLDiagnosticTests checks each supported typed category, invalid state/location rejection, ambiguous constraint names, retained cancellation/cleanup and unknown errors. Actual constraint integration tests verify captured metadata survives transaction rollback. Text location adaptation is verified by Database tests. Changing error cases requires updating exhaustive case classifiers and their behavioral tests.
