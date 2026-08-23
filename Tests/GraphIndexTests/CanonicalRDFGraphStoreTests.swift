import DatabaseEngine
import DatabaseTypes
import DatabaseWire
import DatabaseKit
import StorageKit
import TestHeartbeat
import Testing
import TestSupport
@testable import GraphIndex

@Suite("Canonical RDF graph store", .heartbeat)
struct CanonicalRDFGraphStoreTests {
    private enum ExpectedFailure: Error {
        case rollback
    }

    @Test("Empty named graphs survive create, clear, and delete")
    func emptyNamedGraphLifecycle() async throws {
        let engine = InMemoryEngine()
        let store = makeStore()
        let graph = try RDFGraphName(iri: "https://example.com/graph/empty")
        let quad = try makeQuad(graph: graph)

        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let meter = makeMeter()
            try await store.createGraph(
                graph,
                transaction: transaction,
                workMeter: meter
            )
            let graphsAfterCreate = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            #expect(graphsAfterCreate.map(\.graph) == [graph])
            let inserted = try await store.insert(
                quad,
                transaction: transaction,
                workMeter: meter
            )
            #expect(inserted == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: false
            ))
            let insertedAgain = try await store.insert(
                quad,
                transaction: transaction,
                workMeter: meter
            )
            #expect(insertedAgain == RDFGraphInsertResult(
                quadInserted: false,
                graphCreated: false
            ))
            let cleared = try await store.clear(
                .named(graph),
                transaction: transaction,
                workMeter: meter
            )
            #expect(cleared == 1)
            let graphsAfterClear = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            #expect(graphsAfterClear.map(\.graph) == [graph])
            let scan = try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: .named(graph),
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            #expect(scan.isEmpty)
        }
    }

    @Test("GRAPH evaluation distinguishes empty and missing named graphs")
    func graphEvaluationUsesCatalogExistence() async throws {
        let engine = InMemoryEngine()
        let store = makeStore()
        let empty = try RDFGraphName(
            iri: "https://example.com/graph/empty-evaluation"
        )
        let missing = try RDFGraphName(
            iri: "https://example.com/graph/missing-evaluation"
        )

        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try await store.createGraph(
                empty,
                transaction: transaction,
                workMeter: makeMeter()
            )
        }

        let executor = SPARQLQueryExecutor(
            monotonicClock: TestProcessMonotonicClock(),
            wallClock: FixedTestWallClock(),
            datasetScanner: store
        )
        let (emptyBindings, _) = try await executeSPARQLTest(
            executor: executor,
            pattern: .graph(.named(empty), .basic([])),
            limit: nil,
            offset: 0,
            workMeter: makeMeter(),
            database: engine
        )
        let (missingBindings, _) = try await executeSPARQLTest(
            executor: executor,
            pattern: .graph(.named(missing), .basic([])),
            limit: nil,
            offset: 0,
            workMeter: makeMeter(),
            database: engine
        )
        let (variableBindings, _) = try await executeSPARQLTest(
            executor: executor,
            pattern: .graph(.variable("?graph"), .basic([])),
            limit: nil,
            offset: 0,
            workMeter: makeMeter(),
            database: engine
        )

        #expect(emptyBindings.count == 1)
        #expect(missingBindings.isEmpty)
        #expect(variableBindings.count == 1)
        #expect(variableBindings[0]["?graph"] == .rdfTerm(empty.term))
    }

    @Test("Insert and delete keep all six physical indexes consistent")
    func sixWayIndexConsistency() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(iri: "https://example.com/graph/indexes")
        let quad = try makeQuad(graph: graph)

        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let inserted = try await store.insert(
                quad,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(inserted == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
        }
        let insertedPhysicalKeyCount = try await physicalKeyCount(
            root: root,
            engine: engine
        )
        #expect(insertedPhysicalKeyCount == 6)

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let deleted = try await store.delete(
                quad,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(deleted)
            let deletedAgain = try await store.delete(
                quad,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(!deletedAgain)
        }
        let deletedPhysicalKeyCount = try await physicalKeyCount(
            root: root,
            engine: engine
        )
        #expect(deletedPhysicalKeyCount == 0)

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let graphs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(graphs.map(\.graph) == [graph])
        }
    }

    @Test("Graph-scoped clear and drop do not affect other graphs")
    func graphScopesAreIsolated() async throws {
        let engine = InMemoryEngine()
        let store = makeStore()
        let first = try RDFGraphName(iri: "https://example.com/graph/first")
        let second = try RDFGraphName(iri: "https://example.com/graph/second")
        let firstQuad = try makeQuad(graph: first, suffix: "first")
        let secondQuad = try makeQuad(graph: second, suffix: "second")
        let defaultQuad = try makeQuad(graph: nil, suffix: "default")

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let meter = makeMeter()
            let insertedFirst = try await store.insert(
                firstQuad,
                transaction: transaction,
                workMeter: meter
            )
            let insertedSecond = try await store.insert(
                secondQuad,
                transaction: transaction,
                workMeter: meter
            )
            let insertedDefault = try await store.insert(
                defaultQuad,
                transaction: transaction,
                workMeter: meter
            )
            #expect(insertedFirst == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
            #expect(insertedSecond == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
            #expect(insertedDefault == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: false
            ))
            let clearedFirst = try await store.clear(
                .named(first),
                transaction: transaction,
                workMeter: meter
            )
            let droppedSecond = try await store.drop(
                .named(second),
                transaction: transaction,
                workMeter: meter
            )
            #expect(clearedFirst == 1)
            #expect(droppedSecond == 1)
        }

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let meter = makeMeter()
            let graphs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            let defaultScan = try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: .defaultGraph,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            let namedScan = try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: .allNamedGraphs,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            #expect(graphs.map(\.graph) == [first])
            #expect(defaultScan.map(\.quad) == [defaultQuad])
            #expect(namedScan.isEmpty)
        }
    }

    @Test("All-graph operations preserve or remove catalogs by contract")
    func allGraphCatalogSemantics() async throws {
        let engine = InMemoryEngine()
        let store = makeStore()
        let first = try RDFGraphName(iri: "https://example.com/graph/a")
        let second = try RDFGraphName(iri: "https://example.com/graph/b")

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let meter = makeMeter()
            let insertedFirst = try await store.insert(
                try makeQuad(graph: first, suffix: "a"),
                transaction: transaction,
                workMeter: meter
            )
            let insertedSecond = try await store.insert(
                try makeQuad(graph: second, suffix: "b"),
                transaction: transaction,
                workMeter: meter
            )
            let insertedDefault = try await store.insert(
                try makeQuad(graph: nil, suffix: "default"),
                transaction: transaction,
                workMeter: meter
            )
            #expect(insertedFirst == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
            #expect(insertedSecond == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
            #expect(insertedDefault == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: false
            ))
            let cleared = try await store.clear(
                .allGraphs,
                transaction: transaction,
                workMeter: meter
            )
            let graphsAfterClear = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            let dropped = try await store.drop(
                .allNamedGraphs,
                transaction: transaction,
                workMeter: meter
            )
            let graphsAfterDrop = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            #expect(cleared == 3)
            #expect(graphsAfterClear.map(\.graph) == [first, second])
            #expect(dropped == 0)
            #expect(graphsAfterDrop.isEmpty)
        }
    }

    @Test("All-named range stops before the default graph marker")
    func allNamedRangeExcludesDefaultGraph() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let iriGraph = try RDFGraphName(
            iri: "https://example.com/graph/named-range"
        )
        let blankGraph = try RDFGraphName(
            blankNodeIdentifier: "named-range"
        )
        let defaultQuad = try makeQuad(graph: nil, suffix: "default-range")

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let meter = makeMeter()
            let insertedIRI = try await store.insert(
                try makeQuad(graph: iriGraph, suffix: "iri-range"),
                transaction: transaction,
                workMeter: meter
            )
            let insertedBlank = try await store.insert(
                try makeQuad(graph: blankGraph, suffix: "blank-range"),
                transaction: transaction,
                workMeter: meter
            )
            let insertedDefault = try await store.insert(
                defaultQuad,
                transaction: transaction,
                workMeter: meter
            )
            #expect(insertedIRI == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
            #expect(insertedBlank == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
            #expect(insertedDefault == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: false
            ))

            let removed = try await store.clear(
                .allNamedGraphs,
                transaction: transaction,
                workMeter: meter
            )
            #expect(removed == 2)
        }

        let remainingPhysicalKeyCount = try await physicalKeyCount(
            root: root,
            engine: engine
        )
        #expect(remainingPhysicalKeyCount == 6)
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let meter = makeMeter()
            let defaultResult = try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: .defaultGraph,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            let namedResult = try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: .allNamedGraphs,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            let catalogs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: meter
            )
            #expect(defaultResult.count == 1)
            #expect(defaultResult.first?.quad == defaultQuad)
            #expect(namedResult.isEmpty)
            #expect(Set(catalogs.map(\.graph)) == Set([iriGraph, blankGraph]))
        }
    }

    @Test("Oversized keys fail before any transaction mutation")
    func oversizedKeyDoesNotPartiallyMutate() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(iri: "https://example.com/graph/large")
        let oversizedQuad = RDFQuad(
            subject: .iri(
                try RDFIRI(
                    "https://example.com/subject/"
                        + String(
                            repeating: "x",
                            count: databaseMaximumKeySize
                        )
                )
            ),
            predicate: try RDFPredicateIRI(
                "https://example.com/predicate"
            ),
            object: try .iri(
                validating: "https://example.com/object"
            ),
            graph: graph
        )

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            do {
                _ = try await store.insert(
                    oversizedQuad,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
                Issue.record("Expected an oversized physical key to fail")
            } catch let error as RDFGraphStoreError {
                guard case .keyTooLarge(let actual, let maximum) = error else {
                    Issue.record("Unexpected graph store error: \(error)")
                    return
                }
                #expect(actual > maximum)
                #expect(maximum == databaseMaximumKeySize)
            }
        }

        let physicalKeysAfterFailure = try await physicalKeyCount(
            root: root,
            engine: engine
        )
        #expect(physicalKeysAfterFailure == 0)
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let catalogs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(catalogs.isEmpty)
        }
    }

    @Test("Fixed six-index writes reserve work before mutating")
    func writeBudgetFailureDoesNotPartiallyMutate() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(iri: "https://example.com/graph/budget")

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let meter = DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 10,
                    maximumWorkUnits: 4,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
            do {
                _ = try await store.insert(
                    try makeQuad(graph: graph, suffix: "budget"),
                    transaction: transaction,
                    workMeter: meter
                )
                Issue.record("Expected the fixed write reservation to fail")
            } catch let error as DatabaseWorkLimitError {
                #expect(error == .maximumWorkUnits(
                    stage: .storageWrite,
                    consumed: 4,
                    requested: 7,
                    maximum: 4
                ))
            }
        }

        let physicalKeysAfterFailure = try await physicalKeyCount(
            root: root,
            engine: engine
        )
        #expect(physicalKeysAfterFailure == 0)
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let catalogs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(catalogs.isEmpty)
        }
    }

    @Test("Fixed six-index deletes reserve work before mutating")
    func deleteBudgetFailureDoesNotPartiallyMutate() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/delete-budget"
        )
        let quad = try makeQuad(graph: graph, suffix: "delete-budget")

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let inserted = try await store.insert(
                quad,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(inserted == RDFGraphInsertResult(
                quadInserted: true,
                graphCreated: true
            ))
        }
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            let meter = DatabaseWorkMeter(
                budget: ExecutionBudget(
                    maximumRows: 10,
                    maximumWorkUnits: 4,
                    timeoutMilliseconds: 30_000
                ),
                monotonicClock: TestProcessMonotonicClock()
            )
            do {
                _ = try await store.delete(
                    quad,
                    transaction: transaction,
                    workMeter: meter
                )
                Issue.record("Expected the fixed delete reservation to fail")
            } catch let error as DatabaseWorkLimitError {
                #expect(error == .maximumWorkUnits(
                    stage: .storageWrite,
                    consumed: 3,
                    requested: 6,
                    maximum: 4
                ))
            }
        }

        let physicalKeysAfterFailure = try await physicalKeyCount(
            root: root,
            engine: engine
        )
        #expect(physicalKeysAfterFailure == 6)
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let result = try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: .named(graph),
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(result.count == 1)
            #expect(result.first?.quad == quad)
        }
    }

    @Test("Duplicate insertion reports a missing graph catalog as corruption")
    func duplicateInsertionDoesNotRepairMissingCatalog() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/orphaned"
        )
        let quad = try makeQuad(graph: graph, suffix: "orphaned")
        let physicalCodec = RDFQuadIndexPhysicalCodec(
            baseSubspace: root.subspace(Int64(1))
        )

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try RDFQuadIndexWritePlan(quad: quad).forEachEntry { entry in
                try transaction.setValue([], for: physicalCodec.encode(entry))
            }
        }
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            do {
                _ = try await store.insert(
                    quad,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
                Issue.record("Expected the missing catalog to fail")
            } catch let error as RDFGraphStoreError {
                #expect(error == .missingCatalogForStoredQuad(graph))
            }
        }

        let physicalKeys = try await physicalKeyCount(
            root: root,
            engine: engine
        )
        #expect(physicalKeys == 6)
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let catalogs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(catalogs.isEmpty)
        }
    }

    @Test("Insertion detects an orphaned sibling quad before graph creation")
    func insertionDoesNotRepairSiblingOrphan() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/orphaned-sibling"
        )
        let storedQuad = try makeQuad(graph: graph, suffix: "stored")
        let candidateQuad = try makeQuad(graph: graph, suffix: "candidate")

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try writePhysicalQuad(
                storedQuad,
                root: root,
                transaction: transaction
            )
        }
        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            await #expect(
                throws: RDFGraphStoreError.missingCatalogForStoredQuad(graph)
            ) {
                _ = try await store.insert(
                    candidateQuad,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
            }
        }

        #expect(try await physicalKeyCount(root: root, engine: engine) == 6)
    }

    @Test("Deleting an absent quad detects an orphaned sibling quad")
    func absentDeletionDetectsSiblingOrphan() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/orphaned-delete"
        )
        let storedQuad = try makeQuad(graph: graph, suffix: "stored")
        let absentQuad = try makeQuad(graph: graph, suffix: "absent")

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try writePhysicalQuad(
                storedQuad,
                root: root,
                transaction: transaction
            )
        }
        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            await #expect(
                throws: RDFGraphStoreError.missingCatalogForStoredQuad(graph)
            ) {
                _ = try await store.delete(
                    absentQuad,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
            }
        }

        #expect(try await physicalKeyCount(root: root, engine: engine) == 6)
    }

    @Test("Explicit graph creation refuses to repair orphaned physical state")
    func createGraphDoesNotRepairOrphan() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(
            iri: "https://example.com/graph/orphaned-create"
        )

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try writePhysicalQuad(
                try makeQuad(graph: graph, suffix: "stored"),
                root: root,
                transaction: transaction
            )
        }
        _ = try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            await #expect(
                throws: RDFGraphStoreError.missingCatalogForStoredQuad(graph)
            ) {
                try await store.createGraph(
                    graph,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
            }
        }

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let graphs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(graphs.isEmpty)
        }
    }

    @Test("Transaction failure rolls back catalog and quad mutations")
    func transactionRollbackIsAtomic() async throws {
        let engine = InMemoryEngine()
        let store = makeStore()
        let graph = try RDFGraphName(iri: "https://example.com/graph/rollback")

        await #expect(throws: ExpectedFailure.self) {
            try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
                _ = try await store.insert(
                    try makeQuad(graph: graph),
                    transaction: transaction,
                    workMeter: makeMeter()
                )
                throw ExpectedFailure.rollback
            }
        }

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let graphs = try await store.namedGraphs(
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            let scan = try await store.scan(
                subject: nil,
                predicate: nil,
                object: nil,
                graphTarget: .allNamedGraphs,
                limit: nil,
                readMode: .snapshot,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(graphs.isEmpty)
            #expect(scan.isEmpty)
        }
    }

    @Test("Corrupted graph catalog markers fail instead of disappearing")
    func corruptedCatalogMarkerFails() async throws {
        let engine = InMemoryEngine()
        let root = makeRoot()
        let store = CanonicalRDFGraphStore(rootSubspace: root)
        let graph = try RDFGraphName(iri: "https://example.com/graph/corrupt")
        let codec = RDFGraphCatalogCodec(
            subspace: root.subspace(Int64(2))
        )

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
            try transaction.setValue([2], for: codec.key(for: graph))
        }

        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            await #expect(throws: RDFGraphStoreError.invalidCatalogMarker) {
                _ = try await store.namedGraphs(
                    limit: nil,
                    readMode: .snapshot,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
            }
            return ()
        }
    }

    private func makeStore() -> CanonicalRDFGraphStore {
        CanonicalRDFGraphStore(rootSubspace: makeRoot())
    }

    private func makeRoot() -> Subspace {
        Subspace(prefix: Tuple("canonical-rdf-store-tests").pack())
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func makeQuad(
        graph: RDFGraphName?,
        suffix: String = "value"
    ) throws -> RDFQuad {
        RDFQuad(
            subject: .iri(
                try RDFIRI(
                    "https://example.com/subject/\(suffix)"
                )
            ),
            predicate: try RDFPredicateIRI(
                "https://example.com/predicate"
            ),
            object: .literal(RDFLiteral(
                lexicalForm: suffix,
                datatype: .xsdString
            )),
            graph: graph
        )
    }

    private func physicalKeyCount(
        root: Subspace,
        engine: InMemoryEngine
    ) async throws -> Int {
        try await StorageTransactionExecutor(engine: engine).withTransaction(configuration: .default, clock: TestProcessMonotonicClock()) { transaction in
            let range = root.subspace(Int64(1)).range()
            var count = 0
            try await transaction.forEachInRange(
                from: .firstGreaterOrEqual(range.begin),
                to: .firstGreaterOrEqual(range.end),
                snapshot: true,
                streamingMode: .iterator
            ) { _, _ in
                count += 1
            }
            return count
        }
    }

    private func writePhysicalQuad(
        _ quad: RDFQuad,
        root: Subspace,
        transaction: any TransactionAccess
    ) throws {
        let codec = RDFQuadIndexPhysicalCodec(
            baseSubspace: root.subspace(Int64(1))
        )
        try RDFQuadIndexWritePlan(quad: quad).forEachEntry { entry in
            try transaction.setValue([], for: codec.encode(entry))
        }
    }
}
