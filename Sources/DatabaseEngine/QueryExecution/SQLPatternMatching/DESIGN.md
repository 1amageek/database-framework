# SQLPatternMatching

## Purpose and Scope

This component owns SQL LIKE/ILIKE, SIMILAR and the admitted POSIX ERE pattern
compiler, immutable programs, matching/capture selection, replacement and split
iteration. Its parent is [QueryExecution](../DESIGN.md); it has no children.
This is the frozen S05 implementation contract. Declared interfaces below are
internal implementation entries; behavior is complete only with the evidence
specified in Verification and Change Impact.

The SQL F05 inventory includes both scalar and row-producing functions. S05
owns their bounded pattern kernels and scalar/text-array adapters. Generic SQL
relation invocation for REGEXP_MATCHES and REGEXP_SPLIT_TO_TABLE remains an
explicit S10 dependency; those public relation paths are not complete merely
because their iterators work.

## Responsibilities and Boundaries

- Own grammar admission, flags, escape interpretation, POSIX match/capture
  selection, codepoint positions and zero-width progress.
- Own compilation, matcher scratch, capture ranges, replacement tokens and
  their request-scoped memory/work lifetimes.
- Return immutable input-backed ranges or admitted scalar/text-array output;
  retain the input and every claim needed by a returned range.
- Consume the parent's already-bound non-NULL text operands and request meter.
  Pattern operators preserve a CHAR subject's declared padding; their pattern
  and escape operands use ordinary text coercion. Text-declared regex functions,
  including regex SUBSTRING, use ordinary CHAR-to-TEXT trimming on every text
  argument. The parent retains the operator subject declaration and owns type
  selection, argument evaluation, strict NULL propagation, occurrence mapping
  and canonical result placement.
- Leave QueryAST grammar, Kit/Wire representations, generic arrays, relation
  composition, storage and authorization with their existing owners.
- Leave GraphIndex's XPath/SPARQL regex grammar, replacement syntax and runtime
  intact. There is no Engine-to-Graph import or SPARQL implementation migration.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [QueryExecution](../DESIGN.md) | parent | Selected values, request meter, retained output | Supplies admitted operands and consumes results. | NULL and occurrence handling stay with the caller. |
| [DatabaseKit SQL](../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | F05, C/POSIX profile, typed values | Defines the accepted SQL result semantics. | S05 array results use existing QueryArrayValue; S10 owns generic relation invocation. |
| [DatabaseEngine](../../DESIGN.md) | ancestor | ExecutionBudget, DatabaseWorkMeter and retained owners | Supplies shared resource and lifecycle contracts. | Meter identity is required, not budget equality. |
| [GraphIndex](../../../GraphIndex/DESIGN.md) | coordinates with | Existing programmatic LIKE consumer | Identifies an affected caller without sharing XPath policy. | Any cross-module adapter requires a separately reviewed narrow interface; none is proposed here. |

The retained [oracle findings](../../../../../.codex-tools/sql-s05-evidence/postgresql-oracle/contract-findings.json)
and [resolved user decision](../../../../../.codex-tools/sql-s05-evidence/contract-input-handoff.json)
are reference evidence. The user chose the official `e = ERE` meaning; the
pinned PostgreSQL 18.6 runtime's differing `e` behavior is an accepted product
divergence, not the execution oracle for that flag.

## Architecture

```text
QueryExecution caller: bind -> evaluate arguments -> strict NULL decision
    -> input owners + one request meter
        -> SQLPatternMatching compiler
            -> immutable program + admitted instruction/class/capture storage
                -> task-local match or split iterator + admitted scratch
                    -> input-backed match/capture/split slices
                        -> admitted text or text[] destination
                        -> S10 relation adapter: admitted rows
```

No native Regex, C regex, Foundation or platform-specific fallback is selected.
The implementation is pure Swift over Unicode scalars and uses the existing
retained collection layouts and reservations. A program is request-owned;
there is no global mutable regex cache or registration.

## Contracts and Invariants

### Internal Interface

The component exposes the following internal owner surface:

```swift
enum SQLPatternSyntax: Sendable {
    case like(caseInsensitive: Bool, escape: Unicode.Scalar?)
    case similar(escape: Unicode.Scalar?)
    case ere(caseInsensitive: Bool)
}

enum SQLPatternOperation: Sendable {
    case predicate, match, matches, replace, split
}

struct SQLPatternInput: Sendable {
    static func retaining(_ value: String, leases: [DatabaseQueryPayloadLease],
                          workMeter: DatabaseWorkMeter) throws -> Self
    static func admitting(_ value: String,
                          workMeter: DatabaseWorkMeter) throws -> Self
}

struct SQLPatternProgram: Sendable {
    static func compile(pattern: SQLPatternInput, syntax: SQLPatternSyntax,
                        flags: SQLPatternInput?, operation: SQLPatternOperation) throws -> Self
    static func escape(_ text: String, workMeter: DatabaseWorkMeter) throws -> Unicode.Scalar?
    var isGlobal: Bool { get }
    func matches(_ input: SQLPatternInput) throws -> Bool
    func makeMatchIterator(in input: SQLPatternInput,
                           start: Int32 = 1) throws -> SQLPatternMatchIterator
    func makeSplitIterator(in input: SQLPatternInput) throws -> SQLPatternSplitIterator
    func splittingToArray(_ input: SQLPatternInput, limits: QueryValueLimits) throws
        -> (value: QueryArrayValue, lease: DatabaseQueryPayloadLease)
    func replacing(in input: SQLPatternInput, with replacement: SQLPatternInput,
                   start: Int32 = 1, occurrence: Int32? = nil) throws -> DatabaseQueryScopedFieldValue
}

struct SQLPatternMatchIterator: ~Copyable {
    mutating func next() throws -> SQLPatternMatch?
}

struct SQLPatternSplitIterator: ~Copyable {
    mutating func next() throws -> SQLPatternSlice?
}

struct SQLPatternMatch: Sendable {
    var wholeMatch: SQLPatternSlice { get }
    var captureCount: Int { get }
    func capture(at index: Int) throws -> SQLPatternSlice?
    func materializeTextArray(limits: QueryValueLimits) throws
        -> (value: QueryArrayValue, lease: DatabaseQueryPayloadLease)
}

struct SQLPatternSlice: Sendable {
    var range: Range<String.Index> { get }
    func materialize() throws -> DatabaseQueryScopedFieldValue
}
```

All types and members above are internal. `SQLPatternInput.retaining` requires
nonempty producer leases from the identical meter and admits its own owner/lease
collection capacity. It retains the source String plus those leases without
recharging their already-owned content. `admitting` explicitly admits an input
without producer claims; it is not a silent fallback from invalid retention.
Program/input/flags/replacement meters must be identical.

`CanonicalSQLPatternEvaluator.evaluate(_:operands:workMeter:limits:)` consumes
CanonicalBoundExpression and evaluated QueryValues and returns
`(value: QueryValue, lease: DatabaseQueryPayloadLease?)`. It owns pattern-kind,
selected function/overload dispatch, ordinary CHAR-to-text adaptation through
CanonicalSQLScalarText, and conversion of SQLPatternError into the parent's
public expression failures. Only patternMatch subject adaptation sets
preservingCharacterPadding to true. All function arguments and the operator's
pattern/escape use false, matching the distinct bpchar operator and text-function
signatures; see [character types](https://www.postgresql.org/docs/18/datatype-character.html)
and [REL_18_6 catalog](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_proc.dat). The raw mutation entry is
`predicate(subject:pattern:escape:kind:workMeter:) -> Bool`; nil escape is
disabled and non-nil ESCAPE on an ERE operator is rejected. Work/cancellation
errors pass unchanged through the common `mapFailure(_:function:)` entry.

A slice stores its immutable input owner and scalar-aligned range together;
its raw String/Substring and reservation are not exposed. Match/capture
storage is an immutable admitted owner, so a match can outlive the iterator.
`capture(at:)` uses one-based groups, returns nil for an unmatched group and
rejects an invalid group index. An empty participating capture is a non-nil
empty slice. Neither a range nor a capture borrows mutable scratch.

The match iterator yields all nonoverlapping matches in input order. The
function adapter consumes one result or the complete iterator according to the
selected operation and `isGlobal`. Boolean matching may omit capture-result
materialization, but has the same grammar and successful-match semantics.

### Grammar and Flags

| Mode | Admitted meaning |
|---|---|
| LIKE/ILIKE | Whole-string match; `%` is zero or more codepoints and `_` is one codepoint. Only ASCII letters fold for ILIKE. The parser supplies omitted ESCAPE as backslash; nil disables escaping. `escape(_:workMeter:)` converts empty text to nil and requires exactly one codepoint otherwise. |
| SIMILAR | Whole-string match; SQL `%`/`_`, groups, alternation, bracket expressions and ERE repetition; dot is literal. ESCAPE uses the same explicit scalar-or-nil contract as LIKE. |
| ERE | Literal Unicode codepoints, concatenation, alternation, capturing groups, `.`, `^`/`$`, bracket ranges/negation/POSIX classes and `*`, `+`, `?`, `{m}`, `{m,}`, `{m,n}`. Match selection is earliest start, longest whole match, then POSIX subexpression priority and longest capture. |
| Excluded pattern constructs | ARE pattern backreferences and lookaround fail explicitly as unsupported patterns. Other non-ERE extensions, including inline mode/noncapturing groups, lazy/possessive quantifiers and property/word-class escape syntax, cannot enter a wider native engine. |

Within an ERE bracket expression, `[` is literal unless immediately followed
by `:`, `.` or `=`, which start a character class, collating element or
equivalence class. A literal `[` is also a valid range endpoint. Malformed
special bracket syntax remains an invalid pattern; it is never retried as
literal text. Outside brackets, ordinary escaped characters, including
alphanumeric `\q`, match the character itself. The explicitly excluded
pattern-backreference, property (`\p{...}`/`\P{...}`) and class-shorthand
(`\d`, `\D`, `\s`, `\S`, `\w`, `\W`) forms remain typed unsupported
constructs, rather than admitting ARE interpretation. ERE repetition bounds
are intrinsically 0...255 inclusive; 256 is an invalid pattern before expanded
state allocation. This syntax bound does not cap total NFA state count or
capture count, which remain governed by request resource admission. These rules
follow [PostgreSQL 18 ERE atoms, bounds and brackets](https://www.postgresql.org/docs/18/functions-matching.html#POSIX-SYNTAX)
with the listed explicit unsupported extensions retained by the product profile.

C/POSIX named classes and case matching are ASCII-only; literal non-ASCII
codepoints and codepoint ranges remain valid. No Unicode normalization or
Unicode case-fold mapping is applied. Bracket collation/equivalence for the
C/POSIX profile is single-codepoint identity, not a locale service.

Flags are processed left-to-right. `c`/`i` select sensitive/ASCII-insensitive;
`e` explicitly selects ERE; `q` selects literal pattern text; `t`/`x` select
tight/expanded syntax. `s` is non-newline-sensitive, `n` (and its alias `m`)
is newline-sensitive, `p` affects dot/negative classes but not anchors and `w`
affects anchors but not dot/negative classes. The default is tight ERE and
non-newline-sensitive matching. `b` requests excluded BRE and is a typed
unsupported flag. An unknown flag is an invalid flag. A later mode-setting
flag replaces the earlier setting in its category; `e` and `q` select the
pattern interpretation without admitting ARE syntax.

`g` is admitted only for matches/replacement operations. Predicate, first-match
and split operations reject it. Regex operators convey case sensitivity through `ere(caseInsensitive:)`;
they do not acquire a user flags argument. Function flags override that initial
case setting in their documented order. LIKE/SIMILAR receive no regex flags.

LIKE's trailing escape is represented as a deferred failure instruction: it
fails only if matching reaches it. An earlier decisive mismatch remains false.
ERE/SIMILAR syntax errors are compile failures. Strict SQL NULL is resolved
before any pattern/flag/escape compilation by the parent.

### Replacement, Splitting and Output

- Replacement defaults to the first match; `g` replaces all. `start` is a
  positive one-based codepoint position; positions past the input preserve it.
- Explicit occurrence zero replaces all; positive occurrence replaces only
  that numbered match from start and overrides `g`. Negative occurrence and
  nonpositive start are typed invalid arguments. Nil occurrence selects the
  flags/default behavior of the four-argument overload.
- Replacement references `\\1` through `\\9` select captures and `\\&` selects
  the whole match; `\\\\` emits a backslash. These output references do not
  admit pattern backreferences. Unmatched referenced captures emit empty text.
  Unknown escapes are copied with their backslash, and a trailing backslash is
  copied literally. References to nonparticipating or nonexistent groups emit
  empty text. Replacement literals and references retain their source ranges
  until output materialization. This follows REL_18_6
  [appendStringInfoRegexpSubstr](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/varlena.c#L4117-L4196);
  replacement grammar is independent of ERE pattern admission.
- After a zero-width match, the next search advances one codepoint; a final
  end-position zero-width match is emitted once and then iteration ends.
  Replacement still emits every intervening source slice exactly once.
- Splitting suppresses zero-width matches at the input edges and immediately
  after the previous delimiter. Consuming delimiters preserve interior empty
  fields. It always yields the final unsplit suffix, including the empty-input
  result, with no synthetic no-match success value.
- First-match array output is all captures, or the whole match when there are
  no groups. Unmatched captures are SQL NULL array elements. No match returns
  no `SQLPatternMatch`; the parent maps that to nullable text[] or zero rows.
- Array/scalar output capacity and UTF-8 content are admitted before String or
  QueryArrayValue construction. The returned lease owns the final array and
  element content; the caller retains it with the QueryValue. No source view
  survives without its source owner.

### F04 Regex and SIMILAR SUBSTRING

Regex `substring(text,text)` returns the first capture when any groups exist,
otherwise the whole first match. No match or an unmatched first capture gives
SQL NULL. The admitted three-text-argument form divides its SIMILAR pattern at
up to two escape-double-quote separators into independent prefix, extraction
and suffix patterns. Zero separators makes both outer patterns empty; one
separator makes only the suffix empty. Prefix and suffix have shortest-match
priority, independently of the extraction's longest matching span. More than
two separators fails as an invalid pattern. Internal bounded input views retain
the complete original input lease; no candidate substring is materialized.
The boundary search charges every candidate and uses the same admitted match
kernel, rather than native backtracking. Generic ordinary positional substring
and its SUBSTR alias remain with the F04 string owner.

## Runtime Flows

```text
compile: validate meter -> parse flags/escape -> parse bounded grammar
    -> count/admit expansion -> compile -> immutable program
match: validate meter -> admit scratch -> scan scalar positions
    -> tagged frontier/capture selection -> admitted immutable match ranges
    -> next search or finished
split: next delimiter -> discard degenerate zero-width boundary
    -> admitted source slice -> final suffix -> finished
replace: parse/admit template -> iterate selected matches
    -> measure/admit literals/captures/source slices -> append -> retained text
```

An iterator transitions from ready to active, then exhausted or failed. A
failure ends that iterator; it never publishes partial output as success. Its
scratch is released on exhaustion/failure or destruction. Earlier returned
immutable matches remain valid and retain their own claims/input owner.

## State, Ownership, and Lifecycle

| State | Owner | Lifetime |
|---|---|---|
| Source/pattern/flags text | Immutable input owner and producer leases, or explicit input admission | Last input/program/slice/match reference |
| AST, patches and compile expansion scratch | Compilation scope | Successful publication or any failure/cancel |
| Instructions, classes, group metadata | Immutable program owner | Last program/iterator reference |
| Frontier, visited/tag/capture scratch | Noncopyable task-local iterator or Boolean call | Exhaustion, failure, cancellation or destruction |
| Match/capture ranges | Immutable returned match owner | Last match/slice reference |
| Replacement tokens | Replacement operation scope | Final output transfer or failure |
| Materialized text/array | Existing query payload lease | Last returned payload owner |

All mutation is task-local and synchronous. Immutable owners are Sendable;
mutable iterators are noncopyable and do not expose their scratch or run an
external callback while changing it. There is no shared mutable state or
platform-conditional weakening of ownership/synchronization.

## Failure, Concurrency, and Constraints

Use one internal `SQLPatternError` with distinct invalid-pattern(offset/reason),
unsupported-pattern(offset/construct), invalid/unsupported-flag(offset/flag),
invalid-escape, invalid-replacement(offset/reason), invalid-start,
invalid-occurrence, invalid-capture-index and meter-mismatch cases. Pattern and
flag offsets are Unicode codepoint offsets in their own text; the parent owns
mapping to SQL occurrences/source diagnostics. Throws remain untyped so
DatabaseWorkLimitError and cancellation propagate unchanged. No resource or
unsupported error becomes false, NULL, empty array, zero rows or numeric failure.

Capacity arithmetic is checked before allocation. Existing validated request
memory/work/deadline limits govern AST nodes, expanded states, classes, capture
slots, frontier/tag histories, match count, output rows and bytes. There is no
new guessed numeric limit or fixed nine-group capture cap. Replacement's
reference spelling does not limit the number of stored/captured groups.

No backtracking search is admitted. The implementation must bound work by
actually charged parser/compiler steps, NFA frontier transitions, class tests,
capture/tag comparisons and output scans. Captures add state/work proportional
to the admitted group/tag representation; they are not treated as constant-cost
copies. Repetition expansion is charged and admitted before construction.
Boolean, first/global match, split and replace share one request work meter;
zero-width iteration has a provable scalar progress boundary. Parse, compile,
scan, normalization and output loops check cancellation/deadline at bounded
iteration boundaries. Budget exhaustion releases unfinished scratch/output.

## Verification and Change Impact

The owning tests are proposed `Tests/DatabaseEngineTests/SQLPatternMatchingTests.swift`
and the parent's public SQL/statement/retained-output regressions. Required
behavioral evidence covers grammar/flags/escape, earliest-longest captures,
unmatched versus empty captures, ASCII class/case versus literal Unicode,
codepoint positions, replacement first/global/start/N, zero-width match/split,
NULL suppression by the caller, exact/one-past work/memory, cancellation,
deadline, retained alias lifetime and later failure release.

The user-resolved `e=ERE` cases are specification fixtures; other compatible
cases compare the retained PostgreSQL observations. Pinned runtime results
using its conflicting `e` path are not expectations for product ERE captures.
Native, ordinary WASM and Embedded WASM must compile/link and execute the same
kernel cases; host-only or Foundation-backed results do not prove portability.

A grammar, flag, capture, output or lifecycle change requires rechecking the
parent's Binder/CanonicalRows/raw mutation consumers and the explicit
GraphIndex LIKE adapter when established. S10 relation integration consumes
these same iterators and admission contracts; it must verify no-match/NULL zero
rows, global rows, declared columns, cancellation and retained output before
REGEXP_MATCHES/SPLIT_TO_TABLE public SQL completion is claimed.
