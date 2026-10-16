# SQL Time Zone Rules

## Purpose and Scope

This component owns the Foundation-independent interpretation of explicitly
supplied IANA TZif rules for SQL temporal execution. Its parent is
[SQLTemporal](../DESIGN.md); it has no children. It belongs to DatabaseEngine,
not DatabaseTypes: named-zone rules, capability availability, and query work
are execution semantics rather than primitive timestamp representation.

The established product profile supports UTC and fixed offsets without IANA
data. Named zones require an explicitly injected rules snapshot. Missing or
unknown data never selects a host zone, UTC, a fixed approximation, or a POSIX
string interpretation. A complete bundled tzdb is not a default requirement.

## Responsibilities and Boundaries

| Owner | Responsibility |
|---|---|
| This component | Validate supplied TZif, retain immutable rules, resolve admitted names/aliases, obtain offsets and adjacent transitions, interpret the future footer, enforce bootstrap and query limits. |
| SQLTemporal parent | Calendar arithmetic, supported SQL temporal ranges, lexical temporal input, SQL gap/fold choice, precision/rounding, temporal operators/functions and output. |
| Runtime composition | Supply the snapshot and explicit session context; own long-lived input limits, provenance validation, and replacement between operations. |
| DatabaseTypes | Timestamp/civil primitives and immutable ByteString ownership/borrowing. |
| Application data preparation | Select a pinned IANA release, produce TZif with pinned zic options, preserve its manifest, and deliver bytes without ambient runtime discovery. |

This component does not read files, environment variables, clocks, operating
system timezone APIs, network resources, Foundation, ICU, or process-global
registration. UTC/fixed-offset parsing remains a separate parent operation.
It does not accept a named-zone lookup failure as permission to parse that name
as a POSIX TZ expression.

## Related Designs

| Design | Relationship | Contract Used | Summary | Cautions |
|---|---|---|---|---|
| [SQLTemporal](../DESIGN.md) | Parent / used by | Temporal context and SQL interpretation | Consumes offsets and transition facts | The parent owns SQL gap/fold choice and configuration injection. |
| [QueryExecution](../../DESIGN.md) | Ancestor | Selected execution, explicit capabilities, scoped work | Rules are an injected execution capability | No overload re-resolution or host default. |
| [SQL semantic profile](../../../../../../database-kit/Sources/DatabaseKit/Query/SQL/DESIGN.md) | Depends on | Explicit IANA supply; unknown/unavailable failure | Defines the product boundary | Full global tzdb availability is not promised. |
| [Primitive bytes](../../../../../../database-types/Sources/DatabaseTypes/Bytes/DESIGN.md) | Depends on | Immutable retained owner and synchronous bounded borrow | Holds raw TZif without repeated materialization | A slice retains its full owner; visible byte count is not retained extent. |
| [DatabaseEngine](../../../DESIGN.md) | Depends on | Work, deadline, cancellation and intermediate reservations | Enforces per-query access cost | Reservations follow retained lifetime and failures propagate. |

## Architecture

```text
Pinned IANA source + pinned zic configuration + artifact manifest
    -> application-supplied names, aliases and immutable TZif bytes
        -> SQLTimeZoneDatabase bootstrap validation
            -> immutable validated ranges and parsed footer rules
                -> SQLTimeZoneRulesProvider.rules(named:workMeter:)
                    -> query-scoped SQLTimeZoneRules
                        -> offsets / adjacent transition facts
                            -> SQLTemporal chooses SQL interpretation
```

The provider is an execution dependency, not an extension of QueryValue or a
new wire/persistence value. The concrete database and a rules handle are the
only top-level value owners needed. Transition facts and bootstrap limits are
nested domain values. No generic byte-reader framework, hot mutable cache,
platform-specific state variant, or additional package is required.

### Verified facts, intended guarantee, and delta

| Category | Fact or decision |
|---|---|
| Current implementation | DatabaseEngine has no IANA rules provider. Its existing dependencies provide Timestamp, CivilDateTime, ByteString, and DatabaseWorkMeter. The Foundation temporal adapter consumes Foundation TimeZone/Calendar outside this core. |
| Fixed platform | Swift 6.4.0 WASI SDK `time.h` explicitly states that WASI has no timezone tables. A declared `localtime_r` is not evidence of named-zone support. |
| Intended guarantee | One supplied snapshot produces the same offset/transition facts on Native, ordinary WASM, and Embedded WASM without host configuration. |
| Required change | Add this reader/provider, inject it through the parent's explicit temporal context, and execute identical pinned fixtures on all three targets. No existing runtime success is claimed by this document. |
| Data estimate | Local macOS 2026e-rearguard/posix_only data contains 599 names, 700,766 logical TZif bytes, and 345 distinct payload hashes totaling 405,905 bytes. This is a measured sizing reference, not an official artifact provenance proof or an allocation limit. |

## Contracts and Invariants

### Frozen rules interface

The parent selects the following rules interface for S06. Its implementation and runtime evidence are tracked by the workspace progress record.
The provider operation is a non-generic protocol requirement so an Embedded
existential calls a witness, not a protocol-extension-only default.

```swift
public protocol SQLTimeZoneRulesProvider: Sendable {
    func rules(
        named name: String,
        workMeter: DatabaseWorkMeter
    ) throws -> SQLTimeZoneRules
}
```

`SQLTimeZoneDatabase` is a concrete immutable `Sendable` implementation. The
initializer is `init(definitions: [Definition], aliases: [Alias] = [],
provenance: ByteString, limits: Limits) throws`. Nested `Definition` takes
`name: String, tzif: ByteString`; nested `Alias` takes `name: String,
target: String`. `Limits` takes `maximumInputBytes`, `maximumRetainedBytes`,
and `maximumTransitions` as UInt64. The provenance is the caller-verified
manifest bytes including release identity, retained through known exact byte
ownership. Read-only `inputByteCount`, `retainedByteCount`, `transitionCount`
and `provenance` expose admission facts. Its construction consumes canonical `(name: String,
TZif: ByteString)` entries, explicit alias-to-canonical-name entries, release
and artifact provenance metadata, and explicit `Limits`. Required limits are
maximum aggregate input bytes, maximum retained bytes, and maximum aggregate
transition count; they have no guessed global default. Metadata and name/alias
storage count toward the input/retained limits.

`SQLTimeZoneRules` is an immutable `Sendable` handle retaining the admitted
configuration snapshot and validated raw storage. Its public operations are:

```swift
func offset(at instant: Timestamp, workMeter: DatabaseWorkMeter) throws -> Int32
func transition(beforeOrAt instant: Timestamp, workMeter: DatabaseWorkMeter)
    throws -> SQLTimeZoneRules.Transition?
func transition(after instant: Timestamp, workMeter: DatabaseWorkMeter)
    throws -> SQLTimeZoneRules.Transition?
```

`Transition` contains `instant: Timestamp`, `offsetBefore: Int32`, and
`offsetAfter: Int32`; offsets are seconds east of UTC. The two searches use UTC
ordering and include computed footer transitions. They provide adjacent facts
for the parent's local-time inversion without selecting the SQL gap/fold
answer. No-transition fixed rules return nil transitions; unavailable coverage
throws rather than using nil as a successful substitute. The accepted F07 TO_CHAR TZ directive also consumes the zone abbreviation.
`designation(at:workMeter:)` therefore returns the nested immutable
`Designation(offsetSeconds: Int32, abbreviation: ByteString)` for the selected
instant. The abbreviation is an admitted bounded view of configuration-owned
ASCII bytes; materializing formatted text and retaining its output claim belong
to the parent. No additional public DST flag is required by the selected SQL
operations.


`SQLTimeZoneError` distinguishes `unknownZone`, `rulesUnavailable`,
`unsupportedData`, `malformedData`, `rangeUnavailable`, and
`admissionExceeded`. Work-meter cancellation, deadline and resource failures
propagate unchanged rather than being translated into unavailable data.
The three transition/offset operations and designation lookup are sufficient
for local inversion: the parent scans metered adjacent intervals over the
checked UTC candidate range and selects the SQL gap/fold result. It must not
assume every transition is one hour or that a local day has only one boundary.

Public constructor labels are selected by the implementation owner from these
semantic inputs; labels do not change the frozen meaning. Registration accepts
canonical definitions, explicit aliases, provenance and explicit limits. Names
and provenance retained after construction use exact admitted byte ownership;
input String UTF-8 length alone is not a physical retained-capacity proof.
Container injection and SQL evaluation context remain parent/runtime-owned.

### Name and provenance identity

- Named-zone values use ASCII-only case-insensitive lookup: fold `A...Z` to
  `a...z` for comparison, preserving canonical spelling in the snapshot. This
  policy concerns zone values, not QueryIR identifier normalization. No locale
  or Unicode case normalization is used.
- Registration rejects conflicting folded names and aliases. Aliases are
  explicit references to canonical entries in the same snapshot; they share
  one validated payload owner. Cycles and unresolved targets fail bootstrap.
- Lookup never interprets names as paths. It admits only supplied keys and
  aliases; no filesystem traversal or ambient alias lookup exists.
- A snapshot has one declared IANA release and immutable artifact identity.
  Provenance records source archive digest, zic version/options, canonical
  name/alias manifest, and each TZif digest. TZif itself does not encode the
  tzdb release. The supplying composition verifies provenance before injection;
  this component validates the binary format and does not invent a crypto or
  download dependency.
- The snapshot is fixed for an operation. Updating data creates a replacement
  snapshot for later operations; existing handles retain the old identity.

### Admitted TZif and coverage

| Input or instant | Required behavior |
|---|---|
| TZif v2/v3/v4, zero leap records | Admitted after complete checked validation. Read the second, 64-bit block; never narrow timestamps to 32 bits. |
| TZif v1 or leap-bearing input | Explicit unsupported-data failure. Never ignore leap records or use the legacy block as a fallback. |
| Before first explicit transition | Use local time type 0. |
| Between explicit transitions | Use the transition-selected local time type; a transition instant begins its new interval. |
| Nonempty footer at/after the final explicit transition | Evaluate the footer consistently with the final transition; no last-offset continuation approximation. |
| Empty footer, at/after the final explicit transition | Typed range-unavailable failure. The final transition does not authorize an indefinite continuation. |
| No explicit transitions and empty footer | Use type 0, per RFC 9636. |
| No explicit transitions and nonempty footer | Use the footer for all supported instants. |

Validate both block lengths with checked arithmetic before accessing ranges;
validate transition ordering, type indices, nonempty type table, offsets,
Boolean flags, abbreviation indices/termination, indicator counts, supported
version, and footer framing/grammar. Unsupported versions, truncated bytes,
malformed metadata, and integer overflow are failures before publication.
All calendar/epoch arithmetic remains checked within the parent's admitted
temporal range.

### Future POSIX footer

The reader supports the TZif footer grammar used by the admitted versions:
standard/DST abbreviations including angle-bracket forms, signed offsets with
minutes/seconds, optional DST offset, `Jn`, zero-based `n`, and `Mm.w.d` rules,
default transition time, and v3 extended signed transition hours. It preserves
non-hour DST, southern-hemisphere ordering, leap-year distinctions, and
all-year DST semantics. Missing explicit DST transition rules do not invoke an
installation-dependent default-rules file.

Generate only the transitions needed around the requested instant, using
checked Gregorian arithmetic. Do not expand all years from the last explicit
transition or build an unbounded future table. A finite footer is reusable
immutable metadata, not a cache keyed by each requested year.

## Runtime Flows

```text
Bootstrap:
check input limits -> measure retained extent and metadata -> admit capacity
    -> validate borrowed TZif -> parse bounded footer -> publish snapshot

Query:
checkpoint -> metered name lookup -> retain admitted configuration handle
    -> admit any query scratch -> metered binary search / footer calculation
        -> return scalar facts -> release handle/reservation

Failure/cancellation:
propagate typed cause -> release unpublished capacity or query scratch
```

The provider never performs runtime I/O. Binary search work, examined name
bytes, footer computation and repeated calls are charged to the passed meter;
bootstrap validation is not payment for later query searches.

## State, Ownership, and Lifecycle

- A snapshot owns immutable canonical names, alias indices, raw TZif owners,
  validated byte ranges, and bounded parsed footer metadata. There is no shared
  mutable search state and no Native/Embedded synchronization difference.
- Retain a known self-contained ByteString owner without copying. When a
  supplied view would retain a larger owner, either account the full known
  extent or detach once after bootstrap admission. Unknown retained extent
  requires an admitted exact copy or explicit rejection; visible bytes never
  substitute for unknown retained memory.
- Alias entries share an explicit validated rules record. No generic owner
  identity API or speculative deduplication is needed. Repeated arbitrary
  payload inputs may be conservatively accounted separately.
- Long-lived snapshot admission belongs to bootstrap/runtime composition and
  accounts for the complete retained owner extent. Query lookup retains this
  already-admitted configuration; it does not allocate or re-charge a duplicate
  rules database. Each query separately admits any new handle metadata, scratch,
  copies, and output, retaining reservations for their actual lifetime.
- A packed archive slice still retains the complete pack, which bootstrap must
  account. If an operation creates a new retained slice/copy, the operation
  accounts for its full owner extent. Per-zone self-contained owners allow
  smaller supplied snapshots without an owner-identity abstraction.
- Rules handles may be held by admitted configuration and its active operations.
  They never enter persisted values or cursors. Each operation fixes its
  snapshot; replacement leaves already-admitted configuration/handles valid. New query allocations release on success, early return, failure, and
  cancellation through their existing reservation owners.
- The database record table uses one private exact-count typed allocation.
  Bootstrap validates all supplied definitions and transition limits before
  allocating that table, then replays immutable input to populate it without
  an intermediate parsed-record array. Names/provenance use exact byte owners.
  Canonical owner metadata uses the existing 32-byte shared-owner budget unit
  plus stored field strides; this is not malloc implementation introspection.
  Array capacity cannot prove its retained extent before allocation, so this
  owner admits `count * MemoryLayout<Record>.stride` plus canonical owner
  metadata before allocating. It initializes each slot once, tracks the
  initialized prefix on failure, and deinitializes that prefix exactly once
  before deallocation. The pointer never escapes the private owner; only
  immutable record values are read after publication. All payload owners and
  records are Sendable and no mutation occurs after initialization.
- Bytes are accessed only through synchronous bounded borrowing. Pointers do
  not escape, cross an await, or become the ownership representation.

## Failure, Concurrency, and Constraints

| Cause | Failure contract |
|---|---|
| No provider / unknown admitted name | Typed zone-unavailable / unknown-zone failure at the parent's semantic boundary. |
| Unsupported version or leap data | Typed unsupported-rules-data failure. |
| Invalid binary, footer, alias or provenance input | Typed bootstrap validation failure; no partially published snapshot. |
| Undefined instant coverage | Typed range-unavailable failure. |
| Bootstrap input, retained extent or transition limit | Typed admission failure before the affected allocation. |
| Query work, memory, cancellation or deadline | Preserve existing meter/cancellation errors without relabeling them as invalid zone data. |

The frozen SQLTimeZoneError cases above preserve structural invalidity,
unsupported data, missing capability and bootstrap resource exhaustion.
The parent maps them without swallowing the original work-meter failure.
Immutable sharing is identical on Native/WASM/Embedded. This component has no
mutable cache requiring locks and does not weaken `Sendable` by platform.

## Verification and Change Impact

The owning test target is `Tests/DatabaseEngineTests`:
`SQLTimeZoneRulesTests.swift` owns ten behavioral methods;
`Fixtures/SQLTimeZoneFixtures.swift` contains the same exact IANA bytes for
Native and portable verification. The official 2026e archives and private zic
2026e generation (`-b slim -L /dev/null`, without history truncation) are
recorded in `.codex-tools/sql-s06-evidence/time-zone-fixtures/manifest.json`.
Detached signatures are retained; signature signer validation has not been
claimed. Controlled synthetic/mutated RFC inputs are identified separately
from the four official generated zones. The parent's public SQL tests must
also prove the following composition contracts.

| Proof subject | Required counterexamples |
|---|---|
| Real supplied IANA data | Pinned New York standard/DST transitions; Lord Howe half-hour DST; a historical seconds-based non-hour offset; explicit aliases and ASCII case behavior. |
| SQL composition | Named-zone input and UTC conversion through the actual selected evaluator; gap/fold interpretation remains parent-owned; missing provider/name fails. |
| Future coverage | An instant beyond the last explicit table uses the footer; northern/southern rules, Jn/n leap distinction and extended transition time; empty-footer exact-last failure and no-transition type0 success. |
| Parser admission | Every admitted TZif version; unsupported v1/leap inputs; truncated headers/blocks, invalid indices/ordering/footer, checked length overflow, and exact/one-past input/retained/transition limits. |
| Ownership and resource | Retained slices/aliases, unknown extent handling, exact admission before copies, repeated metered lookups, work/memory one-past rejection, cancellation/deadline, release on success/failure and snapshot replacement. |
| Platform | Exact Swift 6.4.0 release + matching SDK ordinary/Embedded WASM compile/link/runtime, plus Native; identical pinned payloads and expected offset/transition facts. No host Foundation/timezone substitute. |

IANA upgrades change input identity and expected rules, requiring the affected
rules fixtures and parent temporal composition tests to run again. Parser or
owner changes require malformed/admission/lifetime and portable evidence.
This document records the design contract only; implementation and actual
runtime proofs are not yet complete.

### Authoritative format references

- [RFC 9636, TZif](https://www.rfc-editor.org/info/rfc9636/) owns file-format and undefined-coverage semantics.
- [IANA fixed 2026e tzfile manual](https://raw.githubusercontent.com/eggert/tz/2026e/tzfile.5) and [POSIX footer manual](https://raw.githubusercontent.com/eggert/tz/2026e/newtzset.3) clarify the admitted data and footer grammar.
- [IANA release archives](https://data.iana.org/time-zones/releases/) provide versioned artifacts; use pinned releases rather than latest links or an unrecorded host database.
