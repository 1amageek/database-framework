# SQL Temporal Formatting

## Purpose and Scope

This component owns the deterministic C/POSIX temporal template language,
template-directed input conversion, and temporal scalar text output. Parent:
[SQLTemporal](../DESIGN.md). It has no children. This is an S06 contract proposal;
source implementation starts only after the parent freezes the shared kernel
and evaluation-environment interfaces.

The represented behavior is SQL formatting and parsing policy. Its semantic
owner is DatabaseEngine, because changes follow executable SQL meaning,
session-zone interpretation, failure behavior, and query resource admission.
It does not belong to DatabaseTypes or the wire/model adapters.

## Responsibilities and Boundaries

| Owner | Responsibility |
|---|---|
| Formatting | TO_CHAR temporal/interval templates, TO_DATE and two-text-argument TO_TIMESTAMP templates, C/POSIX names and modifiers, ISO/Postgres-style scalar text rendering, bounded parsing/output and format failures. |
| SQLTemporal kernel | Canonical finite/infinite carriers, strict lexical casts, validated calendar assembly/breakdown, checked Julian/ISO arithmetic, precision adjustment, local-to-instant interpretation and temporal ranges. |
| TimeZoneRules | Immutable supplied rules and instant-specific offset/abbreviation designation. |
| SQLTemporal environment | Explicit session zone and input-abbreviation authority; no host defaults. |
| Selected integration | Bind exact overloads and result metadata, supply environment, route scalar CAST/CONCAT through this formatter, retain published output leases. |

TO_TIMESTAMP(double precision) belongs to the temporal numeric constructor,
not this template parser. There is no TO_INTERVAL template function. Ordinary
temporal lexical CAST and template parsing remain distinct entry points.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [SQLTemporal](../DESIGN.md) | parent / depends on | Canonical calendar, zone interpretation and temporal environment | Supplies checked semantic operations | Kernel and environment interfaces must be frozen before implementation. |
| [TimeZoneRules](../TimeZoneRules/DESIGN.md) | depends on | designation(at:workMeter:) | Supplies output zone abbreviation and seconds east of UTC | Output designations do not define an input abbreviation registry. |
| [QueryExecution](../../DESIGN.md) | used by | Selected scalar calls, output reservations and leases | Integrates SELECT, Mutation, CAST and CONCAT | Non-NULL TO_CHAR operands can still produce NULL. |
| [SQL profile](../../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | depends on | T07/T08/F07 | Owns admitted SQL meaning | Infinity is admitted for DATE/TIMESTAMP/TIMESTAMPTZ/INTERVAL; numeric nonfinite remains excluded. |

## Architecture

```text
selected TO_* or scalar-text operation + explicit environment
    -> Formatting entry point
        -> metered template scanner / template-directed field parser
        -> checked SQLTemporal calendar and zone operations
        -> measured output length
            -> admitted final owner + retained result lease
```

The existing CanonicalSQLScalarText owner remains responsible for non-temporal
output. Formatting supplies its temporal borrow; no additional generic string
formatting framework or process-global format cache is introduced.

| Implementation | Owned behavior |
|---|---|
| [SQLTemporalFormatting.swift](SQLTemporalFormatting.swift) | Selected entry points, ordinary scalar text, measured final ownership and leases. |
| [SQLTemporalFormat.swift](SQLTemporalFormat.swift) | Retained-template scanner and replay renderer, token/modifier policy and interval pseudo-calendar fields. |
| [SQLTemporalFormatParser.swift](SQLTemporalFormatParser.swift) | Template-directed fields, literal/separator handling, checked input assembly and explicit abbreviation lookup. |

### Current facts and required delta

| Category | Evidence or requirement |
|---|---|
| Current implementation | [CanonicalSQLScalarText](../../CanonicalSQLScalarText.swift) formats numeric, Boolean, UUID and byte values but rejects temporal values. [CanonicalSQLScalarCast](../../CanonicalSQLScalarCast.swift) and [CanonicalSQLStringEvaluator](../../CanonicalSQLStringEvaluator.swift) consume that borrow. |
| PostgreSQL authority | Captured REL_18_6 formatting.c owns scanner, output and input field behavior; captured datetime.c owns ISO date/time and postgres interval scalar text. The source-backed S06 oracle records actual representative values and failures. |
| Required change | Add one portable temporal formatting path and route selected calls and scalar temporal output through it. No runtime correctness is claimed by this design. |

## Contracts and Invariants

### Proposed integration interface

The selected adapter returns the same `(value: QueryValue,
lease: DatabaseQueryPayloadLease?)` shape already used by selected pattern
execution. `SQLTemporalFormatting.evaluate` has the following proposed internal
seam. It neither reselects an overload nor reads a clock.

```swift
static func evaluate(
    _ bound: CanonicalBoundExpression,
    operands: [QueryValue],
    environment: SQLTemporalEnvironment,
    workMeter: DatabaseWorkMeter,
    limits: QueryValueLimits
) throws -> (value: QueryValue, lease: DatabaseQueryPayloadLease?)
```

`SQLTemporalFormatting.withText` consumes a non-NULL QueryValue, its selected
QueryColumnType, explicit temporal environment and work meter, and invokes a
synchronous generic borrowing-Substring closure. The temporary owner and claim
remain live through that closure. The borrow never enters an existential
protocol-extension dispatch and never escapes or crosses suspension.

```swift
static func withText<Result>(
    of value: QueryValue,
    type: QueryColumnType,
    environment: SQLTemporalEnvironment,
    workMeter: DatabaseWorkMeter,
    _ body: (borrowing Substring) throws -> Result
) throws -> Result
```

The proposed kernel seam is `SQLGregorianCalendar.Fields` with Int32
astronomical year (year 0 is 1 BC), month/day/hour/minute/second/microsecond,
and Int64 julianDay. `fields(julianDay:microsecondsOfDay:)` supports local
rendering immediately outside the UTC timestamp range; `julianDay(year:month:day:)`
validates a real civil date without applying the SQL type range.
`isoFields(julianDay:)` returns ISO year/week/weekday; `dayOfYear(julianDay:)`
and `dayOfWeek(julianDay:)` supply the remaining derived fields. A separate
`syntheticJulianDay(year:month:day:)` owns PostgreSQL interval pseudo-calendar
arithmetic. Canonical finite/infinity classification remains in Kit values,
and temporal assembly/range/rounding remains in the parent kernel.
Interval decomposition retains signed years/month remainder, days,
hours beyond 23, minutes, seconds and fractional microseconds; it does not
normalize a day into 24 hours or a month into 30 days for ordinary output.

The frozen environment is `SQLTemporalEnvironment`, containing
transactionInstant, statementInstant, wallClock and configuration. Formatting
uses only configuration's explicit zone/rules and abbreviation bindings. Shared
kernel operations are proposed pending their owner's final finite/synthetic
contract; the above does not add a public API layer.

### Overloads, NULL and infinity

| Operation | Selected behavior |
|---|---|
| TO_CHAR(timestamp,text), TO_CHAR(timestamptz,text), TO_CHAR(interval,text) | TEXT result; strict argument NULL propagation. Infinity of either sign or an empty template returns NULL before calendar/zone/template evaluation. |
| DATE supplied to TO_CHAR | Binder selects the preferred datetime TIMESTAMPTZ overload through implicit DATE conversion in the session zone; zone directives consume that designation. Finite DATE values outside the resulting instant range fail conversion. Infinity remains infinity and returns NULL. |
| TIME supplied to TO_CHAR | Binder selects INTERVAL through its implicit TIME-to-INTERVAL cast; interval token applicability therefore applies. |
| TIMETZ supplied to TO_CHAR | No implicit viable overload: TIMETZ-to-TIME is assignment-only. An explicit user cast may establish a legal selected overload. |
| TO_DATE(text,text) | DATE result. Parse and validate shared date/time/zone fields, then return the civil date. Parsed zone does not rotate its calendar date. |
| TO_TIMESTAMP(text,text) | TIMESTAMPTZ result. An explicit parsed offset/abbreviation takes precedence; otherwise use the explicit session zone and parent's gap/fold rule. |
| Scalar CAST/CONCAT text | Infinity renders exactly `infinity` or `-infinity`; it is not NULL. |

Template-directed input does not call the ordinary temporal lexical parser as
a fallback. The template grammar does not introduce a generic infinity keyword
or produce excluded nonfinite NUMERIC/float values.

These coercion choices follow the pinned primary catalogs:
[pg_type.dat](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_type.dat#L290)
marks TIMESTAMPTZ preferred in datetime category D;
[pg_cast.dat](https://github.com/postgres/postgres/blob/REL_18_6/src/include/catalog/pg_cast.dat#L353)
marks DATE-to-TIMESTAMP/TIMESTAMPTZ and TIME-to-INTERVAL implicit, while
TIMETZ-to-TIME is assignment-only;
[func_select_candidate](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/parse_func.c#L1101)
chooses a same-category preferred candidate after exact-match ties. The binder
owns this selection; the formatter consumes already-selected child types.

### Complete admitted token table

`T` means finite timestamp/timestamptz output; `I` means finite interval output;
`P` means the shared TO_DATE/TO_TIMESTAMP input parser. Every uppercase token
below also admits its lowercase spelling. Named month/day tokens additionally
admit the shown title case. Era and meridiem spellings preserve the source's
upper/lower and dotted variants. Arbitrary mixed-case spellings are ordinary
literal text unless the scanner finds an exact admitted token prefix.

| Token family | T | I | P and meaning |
|---|---|---|---|
| HH, HH12 | 12-hour wall clock | signed remainder on a 12-hour clock, zero remainder displays 12 | 12-hour field; 1...12 required with meridiem interpretation. |
| HH24 | 24-hour wall clock | full signed hours, can exceed 23 | Hour field; final parser range 0...23. |
| MI, SS | Minute, second | Signed minute, second components | Final fields 0...59. |
| MS, US, FF1...FF6 | Fixed-width fraction; truncate displayed lower digits | Signed fractional component with corresponding width | Right-pad short fractions to microseconds; FF selects output typmod adjustment through kernel. MS and US contributions add. |
| SSSS, SSSSS | Seconds since midnight | Signed time-component seconds | Both aliases parse seconds-of-day before explicit HH/MI/SS precedence. |
| AM/PM, A.M./P.M. | Actual meridiem, requested case/dots | Based on interval hour remainder | Case-insensitive accepted indicator; establishes 12-hour interpretation. |
| YYYY, YYY, YY, Y, Y,YYY | Civil year; short forms use last digits | Signed years component, no BC adjustment | Year and partial-year rules below; comma form uses checked thousands plus remainder. |
| IYYY, IYY, IY, I | ISO week year | Synthetic interval-calendar ISO year | ISO convention and partial-year rules below. |
| BC/AD, B.C./A.D. | Actual era, requested case/dots | Invalid-format failure | Era transformation below. |
| MONTH/Month/month, MON/Mon/mon | C English full/short month names | Invalid-format failure | Case-insensitive English month names; MONTH padding is output-only. |
| MM | Two-digit month | Signed month remainder | Gregorian month field. |
| DAY/Day/day, DY/Dy/dy | C English full/short weekday names | Invalid-format failure | Parse weekday; ignored for ordinary Gregorian date construction, consumed by ISO week-date construction. |
| D | Sunday=1 through Saturday=7 | Invalid-format failure | Gregorian weekday; does not override Gregorian date fields. |
| ID | Monday=1 through Sunday=7 | Invalid-format failure | ISO weekday used with ISO week date. |
| DDD | Gregorian day-of-year | months*30 + days | Gregorian ordinal input used when month/day are not already fully supplied. |
| IDDD | ISO day-of-year | Synthetic interval-calendar ISO day-of-year | ISO ordinal input. |
| DD | Day-of-month | Signed retained days | Gregorian day-of-month. |
| W | `(day-1)/7+1` | Same signed day-component expression | Sets first day of the requested seven-day block in the month. |
| WW | `(dayOfYear-1)/7+1` | Based on interval approximate day-of-year | Sets Gregorian ordinal `(week-1)*7+1`. |
| IW | ISO week | Synthetic interval-calendar ISO week | ISO week-date construction; omitted weekday defaults Monday. |
| CC | Century; 2001 starts century 21 | Truncated years/100 | Century/year combination rules below. |
| J | Local civil Julian day | Synthetic interval-calendar Julian day | Checked Julian-day date construction. |
| Q | Quarter | Quarter of retained month, empty if month is zero | Parse integer and discard; does not override month. |
| RM/rm | Roman month, blank-padded to four | Source-compatible signed month/year remainder interpretation | Case-insensitive Roman month. |
| TZ/tz | Selected zone abbreviation; absent for timestamp without zone | Invalid-format failure | Explicit input abbreviation, or numeric OF grammar. Full IANA names are not accepted by this directive. |
| TZH, TZM | Signed offset hours and absolute minutes | Invalid-format failure | Numeric east-positive offset: hour 0...15, minute 0...59; signed-hour separator rule below. |
| OF | Signed HH or HH:MM; FM changes hour padding | Invalid-format failure | Equivalent to TZH with optional colon TZM. |
| FX | Emits no text | Emits no text | Enable exact one-character separator consumption from this point onward. |

Synthetic interval-calendar directives use the source's signed interval
breakdown as input to PostgreSQL-compatible Julian/ISO arithmetic; they do not
validate the components as an actual civil date or silently use the current
date. The frozen boundary rejects a synthetic Julian/ISO operation with a typed
range failure when any original PostgreSQL date2j signed Int32 arithmetic stage
or result leaves its defined domain. The calendar owner checks each original
stage using wider arithmetic and preserves source truncating division/order.
No wraparound or substitute calendar is admitted. Plain interval output and
ordinary non-calendar component directives retain the full interval domain.
DDD/WW apply the same defined-domain rule to every original signed Int32 stage
of synthetic `tm_yday = (year * 12 + month) * 30 + day`. This check runs only
when the requested directive consumes that approximate ordinal; it does not
reject unrelated component formatting eagerly.

TIMESTAMP without zone has no abbreviation and zero output offset, matching
the zero-initialized PostgreSQL formatting fields. Historical seconds offsets
remain exact in plain scalar text; TO_CHAR TZH/TZM/OF deliberately display only
the hour/minute components defined above.

### Modifiers and literal scanner

| Form | Output behavior | Input behavior |
|---|---|---|
| FM/fm prefix | Applies to next field only; remove its supported zero/blank padding. Fraction directives retain their source-defined width. | Next numeric field accepts variable width; no global toggle. |
| TM/tm prefix | C/POSIX English names with no trailing name padding. | Same deterministic ASCII case-insensitive C/POSIX names. |
| TH/th suffix | Source-compatible numeric ordinal suffix, requested case. | Skip at most two following Unicode characters; does not validate their ordinal spelling. |
| SP suffix | Recognized source-compatible no-op; never invent spelled numbers. | Recognized no-op. Lowercase sp is literal text. |
| Double-quoted text | Literal characters; backslash escapes next character inside quote. | Consume corresponding input characters without testing equality. |
| Ordinary text | Literal bytes except exact token matches; backslash only escapes a following double quote outside quotes. | Template separators/whitespace use separator rules; other literal characters consume input characters. |

The scanner consumes at most one prefix and one postfix for each token, as
REL_18_6 parse_format does. A trailing unmatched quote consumes the remaining
template as literal text. Unknown letters are literal text, not an unsupported
token failure. A known directive invalid for the selected interval is a typed
format failure.

### Calendar and input field rules

- Output can mix Gregorian and ISO fields. Input cannot mix directives marked
  Gregorian and ISO by the captured DCH token table. Neutral era, century,
  Julian, quarter, name-of-weekday and clock fields do not themselves select
  a convention.
- YYY/YY/Y and IYY/IY/I adjust a supplied year shorter than four digits toward
  2020: values below 70 add 2000; below 100 add 1900; below 520 add 2000; below
  1000 add 1000. Four or more supplied digits are not adjusted.
- Numeric fields adjacent to another numeric field consume their declared
  width; a following nonnumeric token, separator, end, TH/th or FM enables
  variable width. YYYY with an adjacent MM therefore consumes four digits,
  while YYYY followed by a separator can consume a larger finite year.
- Negative input years mean BC; explicit BC inverts that sign again. Year zero
  means astronomical zero/1 BC. Formatting input is not MAKE_DATE admission.
- CC combines with YY/Y as a year within the specified century; a low-year
  `00` denotes the century's final year. CC alone means its first year.
  YYYY/YYY/Y,YYY take precedence over CC. BC centuries use the same source
  astronomical-year correction.
- Repeated field assignments that contradict an earlier nonzero value fail
  invalid format. Preserve source zero-field and precedence behavior rather
  than introducing a second generic consistency policy.
- Initial defaults are astronomical year zero, January 1, midnight. Julian,
  week, ordinal and explicit month/day construction follow captured
  do_to_timestamp precedence; every result then passes the kernel's checked
  range and calendar validation.
- TO_DATE and TO_TIMESTAMP use the SQL-function parser mode, not PostgreSQL's
  separate strict internal mode. Without FX, ASCII C whitespace is skipped
  around fields and one template separator consumes one input separator or
  nothing. FX separators consume exactly one Unicode character without
  equality. Numeric conversion still has its source whitespace rules.
- Parser exhaustion is source-compatible: input exhaustion stops processing
  remaining template nodes, and trailing input after the template is ignored.
  This is a precise template-parser rule, not permission to try another format.
- TZH preserves a preceding minus sign only when the count of skipped input
  separators exceeds the template separators, matching the source's
  extra-skip rule. Offset bounds are independent of ordinary temporal lexical
  and AT TIME ZONE textual-offset bounds.
- Input abbreviations use built-in UTC/GMT/Z plus explicitly supplied immutable
  fixed abbreviation-to-east-offset bindings in SQLTemporalConfiguration.
  Matching is ASCII case-insensitive. Conflicting names fail configuration
  admission. Unknown abbreviations fail explicitly; dynamic abbreviation
  bindings are outside this admitted capability. TZif designations cannot be
  reverse-mapped into an abbreviation registry.

### Scalar text policy

Use deterministic DateStyle ISO/YMD and IntervalStyle postgres. Dates print a
minimum-four-digit positive civil year, `-MM-DD`, and trailing ` BC` when
needed. Timestamp adds a space and `HH:MM:SS`; TIME retains `24:00:00`.
Fractional microseconds omit zero fractions and trailing fractional zeros.
TIMESTAMPTZ renders the session-zone local date/time with east-positive numeric
offset. TIMETZ preserves its stored offset; offsets with nonzero seconds print
those seconds. The BC marker follows the complete date/time/offset.

Intervals split months into signed years and month remainder, retain days and
full elapsed hours, and omit zero year/month/day components. Singular applies
only to positive one; use `year(s)`, `mon(s)` and `day(s)`. A positive component
following a negative component gains `+`; each nonzero component resets that
sign context. Print time when nonzero or when all components are zero, with
at least two hour digits and `:MM:SS` plus trimmed fraction. Do not normalize
mixed signs, convert months/days, append `ago`, or select a host IntervalStyle.

## Runtime Flows

```text
TO_CHAR: validate selected carriers -> strict NULL / infinity / empty check
    -> break down finite value and explicit zone -> scan template
        -> measure exact bytes -> reserve -> emit -> publish value + lease

TO_DATE / TO_TIMESTAMP: strict NULL -> admit template and input scanning
    -> parse template-directed fields -> validate through checked kernel
        -> resolve explicit/session zone when an instant is required
            -> publish canonical date/instant

CAST / CONCAT: resolve temporal scalar -> measure -> reserve temporary
    -> synchronous borrowed text -> release temporary
        -> caller retains its separately admitted final output
```

## State, Ownership, and Lifecycle

All parser/renderer state is invocation-local. C/POSIX name tables and token
definitions are immutable. No Foundation, DateFormatter, Calendar, libc locale,
process timezone, mutable global cache, or platform-specific isolation variant
is used. Every shared value has the same Sendable contract on Native, WASM and
Embedded.

Scan existing retained input without materializing an intermediate byte array.
Template nodes, if retained for replay, store bounded ranges into the retained
template owner and hold an admitted scratch reservation. Output uses an exact
count pass followed by one admitted final owner; the required final String
materialization occurs only at the SQL text boundary. Both passes are metered.
An output lease transfers with QueryValue through selected Rows and Mutation.
Failure or cancellation releases scratch and unpublished output immediately.

## Failure, Concurrency, and Constraints

Typed causes distinguish invalid format, conflicting fields, invalid calendar
or clock fields, unsupported selected signature, temporal range overflow,
invalid offset and unknown/unavailable abbreviation or zone capability. Map
these once into the existing selected-expression error contract. Work-meter
cancellation, deadline and resource failures propagate without relabeling.

Every input/template traversal, abbreviation lookup and output pass charges
the existing meter and checkpoints long loops. Checked count arithmetic and
admission precede node allocation and text growth. No separate guessed format
length ceiling is required when existing query input and intermediate-byte
budgets bound the complete operation and retained owners.

## Verification and Change Impact

Owning target: DatabaseEngineTests, with public SQL composition in
DatabaseTests and Native/ordinary WASM/Embedded runtime parity under the parent
matrix. Dedicated tests live in
[SQLTemporalFormattingTests.swift](../../../../../Tests/DatabaseEngineTests/SQLTemporalFormattingTests.swift).
Cases below are required falsifiable evidence, not executed passes.

| Stable case family | Counterexamples |
|---|---|
| S06-FMT-TOKENS | Every token family and case spelling; Gregorian and ISO year crossover, J zero, BC, C English names, RM, interval signed and >24-hour fields; every interval-invalid family rejects. |
| S06-FMT-MODIFIERS | FM affects one field, fractions keep width, TM C names omit padding, TH/th numeric suffix versus input skip, SP no-op, quoted Unicode and escapes, token prefixes and literal unknown letters. |
| S06-FMT-PARSE-YEAR | 69/70, 99/100, 519/520, 999/1000, four-digit short-token input, negative/zero/BC inversion, CC+YY 00, CC ignored by YYYY, >4-digit year with and without separator. |
| S06-FMT-PARSE-DATE | Gregorian/ISO mixing failure, ISO weekday omission, ordinal/Julian paths, invalid leap/day, repeated nonzero conflicts, zero/omitted defaults and field precedence. |
| S06-FMT-PARSE-CLOCK | 12 AM/PM, invalid HH12 and HH24=24, fractional short digits/MS+US sum/FF typmod, seven-digit US overflow, timezone sign ambiguity and 15:59/16:00 bound. |
| S06-FMT-PARSE-TEXT | FX slash with dash template succeeds, extra separator fails, differing quoted text is consumed, optional separators, leading/trailing whitespace, partial input exhaustion and ignored trailing input. |
| S06-FMT-ZONE | UTC and supplied abbreviation, unknown abbreviation and full IANA name in TZ failure, explicit offset overrides session zone, absent template zone uses session gap/fold, output NY standard/DST and historical-second offset. |
| S06-FMT-INFINITY | Every admitted temporal infinity renders lexical text; TO_CHAR infinities and empty template return NULL without a zone lookup; NULL metadata preserved; excluded numeric nonfinite never synthesized. |
| S06-FMT-SCALAR | DATE/BC/TIME24/TIMETZ/TIMESTAMP/TIMESTAMPTZ/INTERVAL CAST and CONCAT through selected SQL; mixed interval signs, zero and trimmed fraction; extreme DATE legal scalar output versus timestamp coercion overflow. |
| S06-FMT-RESOURCE | Exact and one-past work/retained-output/scratch budgets; long literal template cancellation and deadline; success/failure borrow cleanup and final Rows/Mutation lease release. |

Template/kernel changes revalidate their direct parsing/calendar consumers.
Environment changes revalidate selected scalar CAST/CONCAT call paths and
zone authority. Output ownership changes revalidate retained Rows and Mutation
results. No storage, wire, clock or runtime-composition ownership migrates here.
