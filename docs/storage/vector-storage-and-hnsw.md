# Vector Storage And HNSW

## Goal

Vector indexes expose database-types `Vector` values while storing dense
Float32 payloads as compact binary data. Flat, HNSW, IVF, and PQ each own an
explicit persisted layout.

```mermaid
flowchart LR
    A["Persistable model: Vector"] --> B["Borrow Float32 elements"]
    B --> C["Float32 little-endian payload"]
    C --> D["StorageKit transaction"]
    B --> E["SwiftHNSW borrowed buffer API"]
    D --> F["Retained bounded payload view"]
    F --> G["Flat / IVF / PQ distance calculation"]
    D --> H["Training-only mutable materialization"]
```

## Storage Contract

| Layer | User-Facing Shape | Physical Shape |
| --- | --- | --- |
| Model field | `Vector` | Borrowed Float32 elements at the index boundary |
| Debug / print | `[Float]` after explicit decode | Raw bytes only when inspecting storage directly |
| Flat index | `Vector` execution with `[Float]` convenience input | Borrowed `Float32` little-endian payload by primary key |
| HNSW index | `Vector` execution with `[Float]` convenience input | Borrowed vector owner plus label mappings and SwiftHNSW graph snapshot |
| IVF index | `Vector` query/result semantics | Trained centroids, inverted lists, and assignments |
| PQ index | `Vector` query/result semantics | Trained codebooks, byte codes, and retraining vectors |

## Current Physical Layout

| Algorithm | Keys | Values |
| --- | --- | --- |
| Flat | `[indexSubspace][primaryKey]` | Float32 little-endian vector payload |
| HNSW vectors | `[indexSubspace]/vectors/[label]` | Float32 little-endian vector payload |
| HNSW labels | `[indexSubspace]/labels/[primaryKey]` | Tuple-encoded label |
| HNSW primary keys | `[indexSubspace]/pks/[label]` | Tuple-encoded primary key |
| HNSW graph metadata | `[indexSubspace]/_graphMetadata` | Tuple(version, byteCount, chunkSize, chunkCount, revision) |
| HNSW graph chunks | `[indexSubspace]/_graphChunks/[chunk]` | SwiftHNSW versioned binary graph snapshot chunk |
| IVF centroids | `[subspace]/centroids/[clusterId]` | Float32 little-endian vector payload |
| IVF metadata | `[subspace]/metadata` | Structured training metadata |
| IVF lists | `[subspace]/lists/[clusterId]/[primaryKey]` | Float32 little-endian vector payload |
| IVF assignments | `[subspace]/assignments/[primaryKey]` | Tuple-encoded cluster id |
| PQ codebooks | `[subspace]/codebooks/[m]` | Flattened Float32 little-endian payload |
| PQ metadata | `[subspace]/metadata` | Structured training metadata |
| PQ codes | `[subspace]/codes/[primaryKey]` | Byte codes |
| PQ vectors | `[subspace]/vectors/[primaryKey]` | Float32 little-endian vector payload |

PQ search evaluates Euclidean, cosine, and dot-product distances from
query-specific lookup tables. The compressed vector is not reconstructed for
each candidate. Invalid codebook shapes and code ranges fail with
`ProductQuantizationError`.

Flat, IVF, PQ, HNSW, and Fusion search retain one owner for each query or
persisted payload and borrow its bounded storage synchronously. Persisted
Float32 values are read as unaligned little-endian elements so slicing does not
require alignment copies. PQ codebooks and compressed codes are each borrowed
once per evaluation. Only offline IVF/PQ training materializes mutable arrays,
because those algorithms revisit and mutate indexed elements across iterations.

## Copy Boundary Contract

| Path | Owner / View | Production Copy Budget |
| --- | --- | --- |
| `Vector` query builder | Retained canonical `Vector` owner | 0 copies at the builder boundary |
| Persisted Flat / IVF candidate | Retained `ByteString` plus `PersistedVectorView` | 0 copies and one synchronous borrow per distance evaluation |
| Persisted PQ codebook / code | Retained codebook owners plus flattened lookup table, bounded code borrow | 0 candidate reconstruction copies and one borrow per persisted payload |
| HNSW query / insert | Retained `Vector` owner borrowed by SwiftHNSW | 0 copies for finite Float32 values within backend magnitude limits |
| HNSW extreme cosine input | Explicit normalized `Vector` owner | 1 required copy because Float32 backend accumulation would otherwise become non-finite |
| Index persistence | Final little-endian `ByteString` owner | 1 required materialization at the persistence ownership boundary |
| IVF / PQ training | Independent mutable Float arrays | Intentional training-only materialization; never used by the production search path |
| Canonical wire / async driver boundary | Independently owned encoded buffer | Materialize only where the receiver may outlive the synchronous borrow |

The borrow-counting tests cover the production Flat, IVF, and PQ readers rather
than only conversion helpers. HNSW rejects extreme dot-product and Euclidean
inputs when rescaling would change their metric semantics; cosine may normalize
into a new owner because normalization preserves cosine distance.

## Cloudflare Hosting Contract

The framework keeps Flat, HNSW, IVF, and PQ in the single `VectorIndexes`
feature. `database-framework-cloudflare` narrows the execution capability at
its hosting boundary:

```mermaid
flowchart LR
    A["Application container definition"] --> B["Resolve every vector algorithm"]
    B --> C{"Cloudflare capability"}
    C -->|"Flat / IVF / PQ"| D["Open DBContainer"]
    C -->|"HNSW"| E["Typed bootstrap failure"]
    E --> F["No fallback and no index allocation"]
```

Cloudflare Workers limits an isolate to 128 MB, including JavaScript heap and
WebAssembly allocations. HNSW graph restore, live graph ownership, and
snapshot replacement cannot be given a safe guarantee inside that shared
budget. The adapter therefore treats HNSW as unsupported even when a small
graph happens to fit. The bootstrap validator must run before
`DBContainer.open`, migration, graph restoration, or index initialization.

The authoritative platform limit is documented by Cloudflare at
<https://developers.cloudflare.com/workers/platform/limits/#memory>.

## Shared-State Contract

There is no `hasFeature(Embedded)` or `canImport(Synchronization)` branch in the
reviewed DatabaseEngine and VectorIndex state paths. The same storage owner and
isolation primitive are compiled for every target.

| Logical state | Native | WASM | Embedded WASM | Read / mutation / release invariant |
| --- | --- | --- | --- | --- |
| HNSW graph cache entries and revision | `Mutex<State>` | `Mutex<State>` | `Mutex<State>` | Snapshot and replacement use `withLock`; graph search and external callbacks occur outside the state lock |
| HNSW non-thread-safe graph search | `Mutex<Void>` | `Mutex<Void>` | `Mutex<Void>` | The same serialized search entry point is used on every target |
| Context pending mutations and save state | `Mutex<ContextState>` | `Mutex<ContextState>` | `Mutex<ContextState>` | Staging and save transitions use the same mutation entry points; no lock crosses `await` |
| Storage lifecycle phase, operation count, waiters | `Mutex<State>` | `Mutex<State>` | `Mutex<State>` | Admission and shutdown transitions are identical; backend shutdown starts outside the lock |
| Operation lease exactly-once completion | `Mutex<Bool>` | `Mutex<Bool>` | `Mutex<Bool>` | Namespace borrows retain the concrete lease across `await`; release is idempotent and owner-scoped |

## Current Validation Evidence

Current validation follows the repository-wide Xcode test and Swift 6.4 WASI
build gates documented in [Production Readiness](../production-readiness.md).

| Date | Scope | Result |
| --- | --- | --- |
| 2026-08-03 | Strict FoundationDB Xcode harness with database-kit 26.0803.0 and storage-kit 26.0803.0, Swift 6.4 snapshot `2026-07-23-a` | Passed: 3,918 tests; 0 failures, skips, expected failures, runtime warnings, compiler internal errors, macro-plugin internal errors, or coverage-profiler errors |
| 2026-08-03 | Strict SQLite Xcode harness with the same published dependency graph | Passed: 101 tests; 0 failures, skips, expected failures, runtime warnings, or internal tool errors |
| 2026-08-02 | Strict PostgreSQL Xcode harness against PostgreSQL 16.14 in an isolated Apple Container | Passed: 71 tests; 0 failures, skips, expected failures, runtime warnings, startup-order warnings, or internal tool errors |
| 2026-08-03 | `swift build --swift-sdk ..._wasm --product Database --disable-default-traits --traits AllRuntimeFeatures -c release -debug-info-format none` | Passed: normal WASM release build with swift-hnsw 1.1.4 |
| 2026-08-03 | `swift build --swift-sdk ..._wasm-embedded --product Database --disable-default-traits --traits AllRuntimeFeatures -c release -debug-info-format none` | Passed: Embedded WASM release build with the same runtime feature set and swift-hnsw 1.1.4 |
| 2026-08-03 | `swift build --swift-sdk ..._static-linux-0.1.0 --triple aarch64-swift-linux-musl --product DatabaseCLICore --disable-default-traits --traits PostgreSQL -c release -debug-info-format none` | Passed: static Linux release compile and link, including DatabaseMath, DatabaseEngine, and PostgreSQL dependencies |
| 2026-08-02 | `database-framework-cloudflare`: `swift build --swift-sdk ..._wasm --disable-default-traits --traits AllRuntimeFeatures --target CloudflareDatabaseRuntimeVerification` | Passed: normal WASM reactor compile and link with swift-hnsw 1.1.4 |
| 2026-08-02 | Cloudflare AllRuntimeFeatures feasibility gate with `..._wasm-embedded` | Passed: Flat, IVF, and PQ execute through startup validation; effective HNSW fails deterministically before container opening; `VectorIndex.o`, `SwiftHNSW.o`, and `CTurboQuantKernels.o` remain linked |
| 2026-08-02 | Embedded runtime budgets | Passed: 9,084,920 optimized bytes, 3,150,215 compressed bytes, 64 MiB address space, 60.512625 ms startup; workerd RPC and SQLite restart persistence passed |

## Design Rules

- Use database-types `Vector` as the portable user-facing and schema value.
- Borrow contiguous vector elements when computing distance or writing binary
  payloads; materialize an array only at an explicit API ownership boundary.
- Store persisted vector payloads as Float32 little-endian bytes.
- Use structured values only for metadata and mappings, not for dense vector payloads.
- Decode to arrays for debug output, print-oriented inspection, and algorithms
  that explicitly require independent mutable training storage. Search paths
  retain the owner and borrow bounded views instead.
- Treat malformed vector payloads as index corruption and throw typed errors.
- Keep algorithm-specific runtime snapshots versioned and separate from model storage.
