@testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
@testable import DatabaseServer
import DatabaseServerFoundation
import DatabaseTypes
import DatabaseWire
#if DATABASE_SERVER_TEST_GRAPH_INDEXES
@testable import GraphIndex
#endif
import StorageKit
import Synchronization
import TestSupport
import Testing

@Suite("Composition query execution", .serialized)
struct DatabaseCompositionQueryTests {
    @Test("global ordering, provenance, and durable paging are fixed")
    func globalOrderAndDurablePaging() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await seed(fixture)

        let first = try await successfulPage(
            request(pageLimit: 2),
            requestID: 1,
            fixture: fixture
        )
        #expect(try priorities(first) == [1, 2])
        let continuation = try #require(first.continuation)
        #expect(continuation.count == 32)
        let provenance = try #require(first.provenance)
        var origins = provenance.makeOriginIterator()
        #expect(try origins.next() == .source(fixture.primaryBaseID))
        #expect(try origins.next() == .source(fixture.secondaryBaseID))
        #expect(try origins.next() == nil)

        var later = DatabaseEndpointEntity()
        later.id = "later"
        later.title = "Inserted after snapshot publication"
        later.priority = 0
        let context = fixture.container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(fixture.primaryBaseID).newContext()
        try context.insert(later)
        try await context.save()

        let second = try await successfulPage(
            request(pageLimit: 2, continuation: continuation),
            requestID: 2,
            fixture: fixture
        )
        #expect(try priorities(second) == [3, 4])
        #expect(second.continuation == nil)
    }

    @Test("joins execute independently inside every member Base")
    func baseLocalJoinIsSupported() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await insert(
            [("shared", 1)],
            into: fixture.primaryBaseID,
            container: fixture.container
        )
        try await insert(
            [("shared", 2)],
            into: fixture.secondaryBaseID,
            container: fixture.container
        )
        let leftAliased: DataSource = .table(
            TableRef(
                schema: nil,
                table: DatabaseEndpointEntity.persistableType,
                alias: "lhs"
            )
        )
        let rightAliased: DataSource = .table(
            TableRef(
                schema: nil,
                table: DatabaseEndpointEntity.persistableType,
                alias: "rhs"
            )
        )
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .column(ColumnRef(table: "lhs", column: "id")),
                    alias: "id"
                ),
                ProjectionItem(
                    .column(ColumnRef(table: "lhs", column: "priority")),
                    alias: "priority"
                ),
            ]),
            source: .join(
                JoinClause(
                    type: .inner,
                    left: leftAliased,
                    right: rightAliased,
                    condition: .on(
                        .equal(
                            .column(ColumnRef(table: "lhs", column: "id")),
                            .column(ColumnRef(table: "rhs", column: "id"))
                        )
                    )
                )
            ),
            orderBy: [
                SortKey(
                    .column(
                        ColumnRef(table: "lhs", column: "priority")
                    )
                ),
            ]
        )

        let page = try await successfulPage(
            QueryExecuteOperation.Request(
                input: .ir(.select(query)),
                page: QueryExecuteOperation.Page(limit: 10)
            ),
            requestID: 10,
            fixture: fixture
        )
        #expect(try priorities(page) == [1, 2])
        let provenance = try #require(page.provenance)
        var origins = provenance.makeOriginIterator()
        #expect(try origins.next() == .source(fixture.primaryBaseID))
        #expect(try origins.next() == .source(fixture.secondaryBaseID))
        #expect(try origins.next() == nil)
    }

    @Test("a Composition generation change makes a continuation stale")
    func generationChangeInvalidatesContinuation() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await seed(fixture)
        let first = try await successfulPage(
            request(pageLimit: 1),
            requestID: 20,
            fixture: fixture
        )
        let continuation = try #require(first.continuation)

        _ = try await fixture.container.withControlMetadataTransaction {
            transaction in
            try await fixture.container.compositionCatalog.replace(
                id: fixture.compositionID,
                bases: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ],
                expectedRevision: 1,
                transaction: transaction.storageAccess
            )
        }

        let error = try await remoteFailure(
            request(pageLimit: 1, continuation: continuation),
            requestID: 21,
            fixture: fixture
        )
        #expect(error.category == .conflict)
        #expect(error.code == "COMPOSITION_SNAPSHOT_STALE")
    }

    @Test("decomposable aggregates reduce across every member Base")
    func decomposableAggregates() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await seed(fixture)
        let priority: Expression = .column(ColumnRef(column: "priority"))
        let query = SelectQuery(
            projection: .items([
                ProjectionItem(
                    .aggregate(.count(nil, distinct: false)),
                    alias: "count"
                ),
                ProjectionItem(
                    .aggregate(.sum(priority, distinct: false)),
                    alias: "sum"
                ),
                ProjectionItem(
                    .aggregate(.avg(priority, distinct: false)),
                    alias: "average"
                ),
                ProjectionItem(.aggregate(.min(priority)), alias: "minimum"),
                ProjectionItem(.aggregate(.max(priority)), alias: "maximum"),
            ]),
            source: .table(
                TableRef(DatabaseEndpointEntity.persistableType)
            )
        )
        let page = try await successfulPage(
            QueryExecuteOperation.Request(
                input: .ir(.select(query)),
                page: QueryExecuteOperation.Page(limit: 10)
            ),
            requestID: 25,
            fixture: fixture
        )
        let row = try #require(
            page.materializedRows(maximumCount: 1).first
        )
        #expect(try value("count", row: row, page: page) == .int64(4))
        #expect(try value("sum", row: row, page: page) == .int64(10))
        #expect(
            try value("average", row: row, page: page) == .float64(2.5)
        )
        #expect(try value("minimum", row: row, page: page) == .int64(1))
        #expect(try value("maximum", row: row, page: page) == .int64(4))
        let provenance = try #require(page.provenance)
        var origins = provenance.makeOriginIterator()
        #expect(
            try origins.next() == .derived(
                contributors: [
                    fixture.secondaryBaseID,
                    fixture.primaryBaseID,
                ].sorted()
            )
        )
        #expect(try origins.next() == nil)
    }

    @Test("durable paging stays within a bounded intermediate row budget")
    func durablePagingUsesBoundedMemory() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await insert(
            stride(from: 0, to: 48, by: 2).map {
                ("primary-\($0)", Int64($0))
            },
            into: fixture.primaryBaseID,
            container: fixture.container
        )
        try await insert(
            stride(from: 1, to: 48, by: 2).map {
                ("secondary-\($0)", Int64($0))
            },
            into: fixture.secondaryBaseID,
            container: fixture.container
        )
        let budget = ExecutionBudget(
            maximumRows: 100,
            maximumWorkUnits: 1_000_000,
            maximumIntermediateRows: 8,
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            timeoutMilliseconds: 30_000
        )
        var continuation: ByteString?
        var values: [Int64] = []
        var requestID: UInt64 = 26
        repeat {
            let page = try await successfulPage(
                request(
                    pageLimit: 2,
                    continuation: continuation,
                    budget: budget
                ),
                requestID: requestID,
                fixture: fixture
            )
            values.append(contentsOf: try priorities(page))
            continuation = page.continuation
            requestID += 1
        } while continuation != nil
        #expect(values == (0..<48).map(Int64.init))
    }

    @Test("DISTINCT merges exact rows and retains every contributor")
    func distinctRowsRetainContributors() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await insert(
            [("primary-1", 1), ("primary-3", 3)],
            into: fixture.primaryBaseID,
            container: fixture.container
        )
        try await insert(
            [("secondary-1", 1), ("secondary-2", 2)],
            into: fixture.secondaryBaseID,
            container: fixture.container
        )

        let first = try await successfulPage(
            distinctPriorityRequest(pageLimit: 1),
            requestID: 27,
            fixture: fixture
        )
        #expect(try priorities(first) == [1])
        var firstOrigins = try #require(first.provenance)
            .makeOriginIterator()
        #expect(
            try firstOrigins.next() == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
        #expect(try firstOrigins.next() == nil)

        let second = try await successfulPage(
            distinctPriorityRequest(
                pageLimit: 1,
                continuation: try #require(first.continuation)
            ),
            requestID: 28,
            fixture: fixture
        )
        #expect(try priorities(second) == [2])
        let third = try await successfulPage(
            distinctPriorityRequest(
                pageLimit: 1,
                continuation: try #require(second.continuation)
            ),
            requestID: 29,
            fixture: fixture
        )
        #expect(try priorities(third) == [3])
        #expect(third.continuation == nil)
    }

    @Test("single-page DISTINCT releases its unpublished snapshot slot")
    func singlePageDistinctReleasesSnapshotSlot() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await insert(
            [("primary", 1)],
            into: fixture.primaryBaseID,
            container: fixture.container
        )
        try await insert(
            [("secondary", 1)],
            into: fixture.secondaryBaseID,
            container: fixture.container
        )

        for requestID in 100..<109 {
            let page = try await successfulPage(
                distinctPriorityRequest(pageLimit: 10),
                requestID: UInt64(requestID),
                fixture: fixture
            )
            #expect(try priorities(page) == [1])
            #expect(page.continuation == nil)
        }
    }

    @Test("DISTINCT compares exact identity after a digest collision")
    func distinctDigestCollisionUsesExactIdentity() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let reservation = try await fixture.snapshotStore.beginWrite(
            compositionID: fixture.compositionID,
            compositionGeneration: 1,
            schemaGeneration: 1,
            queryFingerprint: ByteString(repeating: 0x71, count: 32),
            authorization: TestBaseEnvironment.authorization
        )
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 100,
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: 16,
                maximumIntermediateBytes: 1 * 1_024 * 1_024,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
        let spill = DatabaseCompositionDistinctSpill(
            snapshotStore: fixture.snapshotStore,
            reservation: reservation,
            maximumIntermediateBytes: 1 * 1_024 * 1_024,
            workMeter: workMeter,
            identityFingerprint: { _ in
                ByteString(repeating: 0x42, count: 32)
            }
        )
        let output = Mutex<[DatabaseCompositionDistinctSpill.Result]>([])

        do {
            try await spill.insert(
                DatabaseEngine.QueryRow(
                    fields: ["priority": .int64(1)],
                    annotations: ["representative": .string("first")],
                    version: PersistableVersionToken("version-1")
                ),
                origin: .source(fixture.primaryBaseID),
                sequence: 0
            )
            try await spill.insert(
                DatabaseEngine.QueryRow(
                    fields: ["priority": .int64(2)]
                ),
                origin: .source(fixture.secondaryBaseID),
                sequence: 1
            )
            try await spill.insert(
                DatabaseEngine.QueryRow(
                    fields: ["priority": .int64(1)],
                    annotations: ["representative": .string("later")],
                    version: PersistableVersionToken("version-2")
                ),
                origin: .source(fixture.secondaryBaseID),
                sequence: 2
            )
            try await spill.forEachResult(batchSize: 1) { result in
                output.withLock { $0.append(result) }
                return true
            }
        } catch {
            do {
                try await fixture.snapshotStore.abortWrite(reservation)
            } catch let cleanupError {
                Issue.record("DISTINCT workspace cleanup failed: \(cleanupError)")
            }
            throw error
        }
        try await fixture.snapshotStore.abortWrite(reservation)

        let results = output.withLock { $0 }
        #expect(results.count == 2)
        #expect(results[0].row.fields["priority"] == .int64(1))
        #expect(
            results[0].origin == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
        #expect(
            results[0].row.annotations["representative"]
                == .string("first")
        )
        #expect(results[0].row.version?.value == "version-1")
        #expect(results[1].row.fields["priority"] == .int64(2))
    }

    #if DATABASE_SERVER_TEST_VECTOR_INDEXES
    @Test("vector search merges one comparable score contract globally")
    func vectorSearchMergesGlobally() async throws {
        let fixture = try await makeFixture(
            schema: try Schema(
                entities: [try DatabaseCompositionVectorEntity.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        DatabaseCompositionVectorEntity.self
                    ),
                ]
            )
        )
        defer { await fixture.container.shutdown() }
        try await insertVectors(
            [
                ("a-0", [0, 0]),
                ("a-4", [4, 0]),
            ],
            into: fixture.primaryBaseID,
            container: fixture.container
        )
        try await insertVectors(
            [
                ("b-1", [1, 0]),
                ("b-2", [2, 0]),
            ],
            into: fixture.secondaryBaseID,
            container: fixture.container
        )

        let first = try await successfulPage(
            vectorRequest(pageLimit: 2),
            requestID: 110,
            fixture: fixture
        )
        #expect(try strings("id", page: first) == ["a-0", "b-1"])
        #expect(try distances(first) == [0, 1])
        var firstOrigins = try #require(first.provenance)
            .makeOriginIterator()
        #expect(try firstOrigins.next() == .source(fixture.primaryBaseID))
        #expect(try firstOrigins.next() == .source(fixture.secondaryBaseID))
        #expect(try firstOrigins.next() == nil)

        let second = try await successfulPage(
            vectorRequest(
                pageLimit: 2,
                continuation: try #require(first.continuation)
            ),
            requestID: 111,
            fixture: fixture
        )
        #expect(try strings("id", page: second) == ["b-2"])
        #expect(try distances(second) == [2])
        #expect(second.continuation == nil)
    }
    #endif

    #if DATABASE_SERVER_TEST_GRAPH_INDEXES
    @Test("SPARQL DISTINCT preserves Base-qualified blank-node identity")
    func sparqlDistinctQualifiesBlankNodes() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let predicate = try RDFTerm.iri(validating: "urn:composition:predicate")
        let object = try RDFTerm.iri(validating: "urn:composition:object")
        let sharedSubject = try RDFTerm.iri(
            validating: "urn:composition:shared-subject"
        )
        let blankSubject = RDFTerm.blankNode(
            try RDFBlankNodeIdentifier("same-local-label")
        )
        for baseID in [fixture.primaryBaseID, fixture.secondaryBaseID] {
            try await insertRDF(
                [
                    try RDFQuad(
                        validatingSubject: blankSubject,
                        predicate: predicate,
                        object: object
                    ),
                    try RDFQuad(
                        validatingSubject: sharedSubject,
                        predicate: predicate,
                        object: object
                    ),
                ],
                into: baseID,
                container: fixture.container
            )
        }
        let query = SelectQuery(
            projection: .distinctItems([
                ProjectionItem(.variable(Variable("subject"))),
                ProjectionItem(.variable(Variable("object"))),
            ]),
            source: .graphPattern(
                .bgp(
                    TriplePattern(
                        subject: .variable("subject"),
                        predicate: .iri("urn:composition:predicate"),
                        object: .variable("object")
                    )
                )
            ),
            orderBy: [
                SortKey(.variable(Variable("subject"))),
            ]
        )

        let page = try await successfulPage(
            QueryExecuteOperation.Request(
                input: .ir(.select(query)),
                page: QueryExecuteOperation.Page(limit: 10)
            ),
            requestID: 112,
            fixture: fixture
        )
        let subjects = try rdfTerms("subject", page: page)
        #expect(subjects.count == 3)
        let blankIdentifiers = subjects.compactMap { term -> String? in
            guard case .blankNode(let identifier) = term else { return nil }
            return identifier.rawValue
        }
        #expect(blankIdentifiers.count == 2)
        #expect(Set(blankIdentifiers).count == 2)
        #expect(
            subjects.contains(
                try RDFTerm.iri(
                    validating: "urn:composition:shared-subject"
                )
            )
        )

        var origins = try #require(page.provenance).makeOriginIterator()
        var values: [CompositionOrigin] = []
        while let origin = try origins.next() { values.append(origin) }
        #expect(values.filter { origin in
            if case .source = origin { return true }
            return false
        }.count == 2)
        #expect(
            values.contains(
                .derived(
                    contributors: [
                        fixture.primaryBaseID,
                        fixture.secondaryBaseID,
                    ].sorted()
                )
            )
        )
    }

    @Test("SPARQL SERVICE is rejected before Composition execution")
    func sparqlServiceIsUnsupported() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let query = SelectQuery(
            projection: .all,
            source: .service(
                endpoint: "https://example.invalid/sparql",
                pattern: .basic([]),
                silent: false
            )
        )

        let error = try await remoteFailure(
            QueryExecuteOperation.Request(
                input: .ir(.select(query)),
                page: QueryExecuteOperation.Page(limit: 10)
            ),
            requestID: 113,
            fixture: fixture
        )
        #expect(error.category == .invalidRequest)
        #expect(error.code == "COMPOSITION_PLAN_UNSUPPORTED")
    }

    @Test("SPARQL REDUCED is rejected instead of weakening semantics")
    func sparqlReducedIsUnsupported() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let query = SelectQuery(
            projection: .all,
            source: .graphPattern(.basic([])),
            reduced: true
        )

        let error = try await remoteFailure(
            QueryExecuteOperation.Request(
                input: .ir(.select(query)),
                page: QueryExecuteOperation.Page(limit: 10)
            ),
            requestID: 114,
            fixture: fixture
        )
        #expect(error.category == .invalidRequest)
        #expect(error.code == "COMPOSITION_PLAN_UNSUPPORTED")
    }

    @Test("SPARQL ASK evaluates every Base in one federated snapshot")
    func sparqlAskUsesFederatedSnapshotAndProvenance() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await insertRDF(
            [
                try RDFQuad(
                    validatingSubject: .iri(
                        validating: "urn:composition:ask-subject"
                    ),
                    predicate: .iri(
                        validating: "urn:composition:ask-predicate"
                    ),
                    object: .iri(
                        validating: "urn:composition:ask-object"
                    )
                ),
            ],
            into: fixture.secondaryBaseID,
            container: fixture.container
        )
        let matching = AskQuery(
            pattern: .basic([
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:composition:ask-predicate"),
                    object: .variable("object")
                ),
            ])
        )
        let matchingResponse = try await invoke(
            QueryExecuteOperation.Request(
                input: .ir(.ask(matching)),
                page: QueryExecuteOperation.Page(limit: 1)
            ),
            requestID: 115,
            fixture: fixture
        )
        let matchingResult: QueryBooleanResult
        switch matchingResponse {
        case .success(.boolean(let result)):
            matchingResult = result
        default:
            Issue.record("Expected a successful Composition ASK result")
            throw TestFailure.unexpectedResponse
        }
        #expect(matchingResult.value)
        var matchingOrigins = try #require(matchingResult.provenance)
            .makeOriginIterator()
        #expect(
            try matchingOrigins.next()
                == .derived(contributors: [fixture.secondaryBaseID])
        )
        #expect(try matchingOrigins.next() == nil)
        if case .federated(let points) = matchingResult.consistency {
            #expect(points.count == 2)
        } else {
            Issue.record("Expected federated ASK consistency")
        }

        let missing = AskQuery(
            pattern: .basic([
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:composition:missing-predicate"),
                    object: .variable("object")
                ),
            ])
        )
        let missingResponse = try await invoke(
            QueryExecuteOperation.Request(
                input: .ir(.ask(missing)),
                page: QueryExecuteOperation.Page(limit: 1)
            ),
            requestID: 116,
            fixture: fixture
        )
        let missingResult: QueryBooleanResult
        switch missingResponse {
        case .success(.boolean(let result)):
            missingResult = result
        default:
            Issue.record("Expected a successful negative Composition ASK result")
            throw TestFailure.unexpectedResponse
        }
        #expect(!missingResult.value)
        var missingOrigins = try #require(missingResult.provenance)
            .makeOriginIterator()
        #expect(
            try missingOrigins.next()
                == .derived(
                    contributors: [
                        fixture.primaryBaseID,
                        fixture.secondaryBaseID,
                    ].sorted()
                )
        )
        #expect(try missingOrigins.next() == nil)
    }

    @Test("SPARQL ASK rejects a non-local SERVICE pattern")
    func sparqlAskServiceIsUnsupported() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let query = AskQuery(
            pattern: .service(
                endpoint: "https://example.invalid/sparql",
                pattern: .basic([]),
                silent: false
            )
        )

        let error = try await remoteFailure(
            QueryExecuteOperation.Request(
                input: .ir(.ask(query)),
                page: QueryExecuteOperation.Page(limit: 1)
            ),
            requestID: 117,
            fixture: fixture
        )
        #expect(error.category == .invalidRequest)
        #expect(error.code == "COMPOSITION_PLAN_UNSUPPORTED")
    }

    @Test("CONSTRUCT unions exact quads with Base-qualified blank nodes and durable paging")
    func constructUnionPreservesIdentityAndSnapshot() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let sourcePredicate = try RDFTerm.iri(
            validating: "urn:composition:construct-source"
        )
        let object = try RDFTerm.iri(
            validating: "urn:composition:construct-object"
        )
        let sharedSubject = try RDFTerm.iri(
            validating: "urn:composition:construct-shared"
        )
        let blankSubject = RDFTerm.blankNode(
            try RDFBlankNodeIdentifier("same-local-label")
        )
        for baseID in [fixture.primaryBaseID, fixture.secondaryBaseID] {
            try await insertRDF(
                [
                    try RDFQuad(
                        validatingSubject: blankSubject,
                        predicate: sourcePredicate,
                        object: object
                    ),
                    try RDFQuad(
                        validatingSubject: sharedSubject,
                        predicate: sourcePredicate,
                        object: object
                    ),
                ],
                into: baseID,
                container: fixture.container
            )
        }
        let query = ConstructQuery(
            template: [
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:composition:construct-derived"),
                    object: .variable("object")
                ),
            ],
            pattern: .basic([
                TriplePattern(
                    subject: .variable("subject"),
                    predicate: .iri("urn:composition:construct-source"),
                    object: .variable("object")
                ),
            ])
        )
        var continuation: ByteString?
        var quads: [RDFQuad] = []
        var originByQuad: [RDFQuad: CompositionOrigin] = [:]
        var requestID: UInt64 = 118

        repeat {
            let page = try await successfulGraphPage(
                QueryExecuteOperation.Request(
                    input: .ir(.construct(query)),
                    page: QueryExecuteOperation.Page(
                        limit: 1,
                        continuation: continuation
                    )
                ),
                requestID: requestID,
                fixture: fixture
            )
            var origins = try #require(page.provenance)
                .makeOriginIterator()
            let pageQuads = try page.materializedQuads(
                maximumCount: page.quadCount
            )
            for quad in pageQuads {
                quads.append(quad)
                let origin = try origins.next()
                originByQuad[quad] = try #require(origin)
            }
            #expect(try origins.next() == nil)
            continuation = page.continuation
            if requestID == 118 {
                try await insertRDF(
                    [
                        try RDFQuad(
                            validatingSubject: .iri(
                                validating:
                                    "urn:composition:inserted-after-snapshot"
                            ),
                            predicate: sourcePredicate,
                            object: object
                        ),
                    ],
                    into: fixture.primaryBaseID,
                    container: fixture.container
                )
            }
            requestID += 1
        } while continuation != nil

        #expect(quads.count == 3)
        #expect(Set(quads).count == 3)
        let blankIdentifiers = quads.compactMap { quad -> String? in
            guard case .blankNode(let identifier) = quad.subject else {
                return nil
            }
            return identifier.rawValue
        }
        #expect(blankIdentifiers.count == 2)
        #expect(Set(blankIdentifiers).count == 2)
        let sharedIRI = try RDFIRI("urn:composition:construct-shared")
        let shared = try #require(quads.first { quad in
            quad.subject == .iri(sharedIRI)
        })
        #expect(
            originByQuad[shared] == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
    }

    @Test("DESCRIBE unions outgoing quads and merges exact contributors")
    func describeUnionMergesContributors() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let subject = try RDFTerm.iri(
            validating: "urn:composition:described"
        )
        let predicate = try RDFTerm.iri(
            validating: "urn:composition:detail"
        )
        let commonObject = try RDFTerm.iri(
            validating: "urn:composition:common"
        )
        try await insertRDF(
            [
                try RDFQuad(
                    validatingSubject: subject,
                    predicate: predicate,
                    object: commonObject
                ),
                try RDFQuad(
                    validatingSubject: subject,
                    predicate: predicate,
                    object: .iri(validating: "urn:composition:primary")
                ),
            ],
            into: fixture.primaryBaseID,
            container: fixture.container
        )
        try await insertRDF(
            [
                try RDFQuad(
                    validatingSubject: subject,
                    predicate: predicate,
                    object: commonObject
                ),
                try RDFQuad(
                    validatingSubject: subject,
                    predicate: predicate,
                    object: .iri(validating: "urn:composition:secondary")
                ),
            ],
            into: fixture.secondaryBaseID,
            container: fixture.container
        )
        let page = try await successfulGraphPage(
            QueryExecuteOperation.Request(
                input: .ir(
                    .describe(
                        DescribeQuery(
                            selection: .resources(
                                first: .iri("urn:composition:described"),
                                additional: []
                            )
                        )
                    )
                ),
                page: QueryExecuteOperation.Page(limit: 10)
            ),
            requestID: 130,
            fixture: fixture
        )
        let quads = try page.materializedQuads(maximumCount: page.quadCount)
        #expect(quads.count == 3)
        var origins = try #require(page.provenance).makeOriginIterator()
        var originByObject: [RDFTerm: CompositionOrigin] = [:]
        for quad in quads {
            let origin = try origins.next()
            originByObject[quad.object] = try #require(origin)
        }
        #expect(
            originByObject[commonObject] == .derived(
                contributors: [
                    fixture.primaryBaseID,
                    fixture.secondaryBaseID,
                ].sorted()
            )
        )
        #expect(try origins.next() == nil)
    }
    #endif

    @Test("expired durable snapshots are removed from every index")
    func expiredSnapshotCleanup() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await seed(fixture)
        let first = try await successfulPage(
            request(pageLimit: 1),
            requestID: 30,
            fixture: fixture
        )
        let continuation = try #require(first.continuation)

        try fixture.clock.advance(seconds: 15 * 60 + 1)
        try await fixture.snapshotStore.cleanupExpired()

        let error = try await remoteFailure(
            request(pageLimit: 1, continuation: continuation),
            requestID: 31,
            fixture: fixture
        )
        #expect(error.category == .invalidRequest)
        #expect(error.code == "INVALID_CONTINUATION")
    }

    @Test("concurrent snapshot creation atomically enforces eight slots")
    func concurrentSnapshotLimit() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        try await seed(fixture)

        let outcomes = try await withThrowingTaskGroup(
            of: SnapshotCreationOutcome.self
        ) { group in
            for requestID in 40..<49 {
                group.addTask {
                    let response = try await invoke(
                        request(pageLimit: 1),
                        requestID: UInt64(requestID),
                        fixture: fixture
                    )
                    switch response {
                    case .success:
                        return .success
                    case .failure(let error):
                        guard error.category == .resourceLimit,
                              error.code == "COMPOSITION_SNAPSHOT_LIMIT" else {
                            throw TestFailure.unexpectedResponse
                        }
                        return .limit
                    }
                }
            }
            var values: [SnapshotCreationOutcome] = []
            for try await value in group {
                values.append(value)
            }
            return values
        }
        #expect(outcomes.filter { $0 == .success }.count == 8)
        #expect(outcomes.filter { $0 == .limit }.count == 1)
    }

    private struct Fixture {
        let container: DBContainer
        let endpoint: DatabaseEndpoint
        let primaryBaseID: Base.ID
        let secondaryBaseID: Base.ID
        let compositionID: Base.Composition.ID
        let snapshotStore: DatabaseCompositionSnapshotStore
        let clock: Clock
    }

    private enum SnapshotCreationOutcome: Sendable {
        case success
        case limit
    }

    private final class Clock: WallClock, Sendable {
        private let timestamp = Mutex(Timestamp(secondsSinceUnixEpoch: 0))

        var now: Timestamp {
            timestamp.withLock { $0 }
        }

        func advance(seconds: Int64) throws {
            try timestamp.withLock { timestamp in
                let next = timestamp.secondsSinceUnixEpoch
                    .addingReportingOverflow(seconds)
                guard !next.overflow else {
                    throw TestFailure.unexpectedResponse
                }
                timestamp = try Timestamp(
                    secondsSinceUnixEpoch:
                        next.partialValue,
                    nanoseconds: timestamp.nanoseconds
                )
            }
        }
    }

    private actor Scheduler: DatabaseJobScheduler {
        private var earliest: Timestamp?

        func ensureWakeUp(noLaterThan timestamp: Timestamp) async throws {
            earliest = earliest.map { min($0, timestamp) } ?? timestamp
        }
    }

    private func makeFixture(
        schema: Schema? = nil,
        runtimeConfiguration: DatabaseRuntimeConfiguration? = nil
    ) async throws -> Fixture {
        let controlDomainID = try DatabaseStorageDomain.ID("control")
        let dataDomainID = try DatabaseStorageDomain.ID("secondary-data")
        let primaryPlacementID = try Base.Placement.ID("primary")
        let secondaryPlacementID = try Base.Placement.ID("secondary")
        let primaryBaseID = try TestBaseEnvironment.id()
        let principal = Principal(
            identifier: "test-runner",
            roles: ["test-runner"]
        )
        let topology = try DatabaseStorageTopology(
            controlDomainID: controlDomainID,
            domains: [
                try DatabaseStorageDomain(
                    id: controlDomainID,
                    namespacePath: ["database", "control"],
                    storageEngine: InMemoryEngine()
                ),
                try DatabaseStorageDomain(
                    id: dataDomainID,
                    namespacePath: ["database", "secondary-data"],
                    storageEngine: InMemoryEngine()
                ),
            ],
            placements: [
                try DatabaseStoragePlacement(
                    id: primaryPlacementID,
                    domainID: controlDomainID,
                    path: ["bases"]
                ),
                try DatabaseStoragePlacement(
                    id: secondaryPlacementID,
                    domainID: dataDomainID,
                    path: ["bases"]
                ),
            ],
            defaultPlacementID: primaryPlacementID
        )
        let resolvedSchema = try schema ?? Schema(
            entities: [try DatabaseEndpointEntity.schemaEntity],
            version: Schema.Version(1, 0, 0)
        )
        let resolvedRuntime = try runtimeConfiguration
            ?? DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        DatabaseEndpointEntity.self
                    ),
                ]
            )
        let container = try await DBContainer.open(
            for: resolvedSchema,
            configuration: DBConfiguration(
                name: "composition-multi-domain",
                storageTopology: topology,
                monotonicClock: TestProcessMonotonicClock(),
                wallClock: FixedTestWallClock()
            ),
            runtimeConfiguration: resolvedRuntime,
            security: .testingDisabled
        )
        _ = try await container.provisionBase(
            primaryBaseID,
            placementID: primaryPlacementID,
            initialGrants: [
                Security.Grant(
                    subject: .principal(principal.identifier),
                    resource: .base(primaryBaseID),
                    access: .all
                ),
            ],
            expectedRevision: 0
        )
        let secondaryBaseID = try Base.ID("other")
        _ = try await container.provisionBase(
            secondaryBaseID,
            placementID: secondaryPlacementID,
            initialGrants: [
                Security.Grant(
                    subject: .principal("test-runner"),
                    resource: .base(secondaryBaseID),
                    access: .all
                ),
            ],
            expectedRevision: 0
        )
        let compositionID = try Base.Composition.ID("shared")
        _ = try await container.withControlMetadataTransaction {
            transaction in
            try await container.compositionCatalog.create(
                try Base.Composition(
                    id: compositionID,
                    bases: [primaryBaseID, secondaryBaseID]
                ),
                expectedRevision: 0,
                transaction: transaction.storageAccess
            )
        }
        let clock = Clock()
        let snapshotStore = DatabaseCompositionSnapshotStore(
            container: container,
            clock: AnyDatabaseWallClock(clock),
            identifierGenerator: AnyDatabaseUUIDGenerator(
                RandomDatabaseUUIDGenerator()
            ),
            scheduler: AnyDatabaseJobScheduler(Scheduler()),
            wireLimits: .default
        )
        let registry = try DatabaseOperationRegistry(
            handlers: [
                AnyDatabaseOperationHandler(
                    QueryExecuteHandler(
                        runtimeLimits: .default,
                        compositionSnapshotStore: snapshotStore
                    )
                ),
            ],
            requiredOperations: [.queryExecute]
        )
        return Fixture(
            container: container,
            endpoint: DatabaseEndpoint(
                container: container,
                registry: registry,
                admissionPolicy: AnyDatabaseOperationAdmissionPolicy(
                    UnrestrictedDatabaseOperationAdmissionPolicy()
                )
            ),
            primaryBaseID: primaryBaseID,
            secondaryBaseID: secondaryBaseID,
            compositionID: compositionID,
            snapshotStore: snapshotStore,
            clock: clock
        )
    }

    private func seed(_ fixture: Fixture) async throws {
        try await insert(
            [("primary-1", 1), ("primary-3", 3)],
            into: fixture.primaryBaseID,
            container: fixture.container
        )
        try await insert(
            [("other-2", 2), ("other-4", 4)],
            into: fixture.secondaryBaseID,
            container: fixture.container
        )
    }

    private func insert(
        _ values: [(String, Int64)],
        into baseID: Base.ID,
        container: DBContainer
    ) async throws {
        let context = container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(baseID).newContext()
        for (id, priority) in values {
            var entity = DatabaseEndpointEntity()
            entity.id = id
            entity.title = id
            entity.priority = priority
            try context.insert(entity)
        }
        try await context.save()
    }

    #if DATABASE_SERVER_TEST_VECTOR_INDEXES
    private func insertVectors(
        _ values: [(String, [Float])],
        into baseID: Base.ID,
        container: DBContainer
    ) async throws {
        let context = container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(baseID).newContext()
        for (id, embedding) in values {
            try context.insert(
                DatabaseCompositionVectorEntity(
                    id: id,
                    title: id,
                    embedding: try Vector(float32: embedding)
                )
            )
        }
        try await context.save()
    }
    #endif

    #if DATABASE_SERVER_TEST_GRAPH_INDEXES
    private func insertRDF(
        _ quads: [RDFQuad],
        into baseID: Base.ID,
        container: DBContainer
    ) async throws {
        let context = container.session(
            authorization: TestBaseEnvironment.authorization
        ).base(baseID).newContext()
        try await context.withBaseOperation {
            let lease = try context.requireOperationBaseLease()
            let store = CanonicalRDFGraphStore(
                rootSubspace: CanonicalRDFGraphStore.rootSubspace(
                    forBaseRoot: lease.root
                )
            )
            _ = try await context.container.transactionExecutor
                .withTransaction(
                    configuration: .batch,
                    clock: context.container.monotonicClock
                ) { transaction in
                    let workMeter = DatabaseWorkMeter(
                        budget: ExecutionBudget(),
                        monotonicClock: context.container.monotonicClock
                    )
                    for quad in quads {
                        _ = try await store.insert(
                            quad,
                            transaction: transaction,
                            workMeter: workMeter
                        )
                    }
                }
        }
    }

    private func rdfTerms(
        _ name: String,
        page: QueryRowPage
    ) throws -> [RDFTerm] {
        let index = try #require(page.columns.firstIndex { $0.name == name })
        return try page.materializedRows(maximumCount: page.rowCount).map {
            row in
            guard case .rdfTerm(let term) = row.values[index] else {
                throw TestFailure.unexpectedResponse
            }
            return term
        }
    }

    private func successfulGraphPage(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        fixture: Fixture
    ) async throws -> RDFGraphPage {
        let response = try await invoke(
            request,
            requestID: requestID,
            fixture: fixture
        )
        switch response {
        case .success(.rdfGraph(let page)):
            return page
        case .failure(let error):
            Issue.record(
                "Expected a successful Composition RDF graph page, got \(error.code): \(error.message)"
            )
            throw TestFailure.unexpectedResponse
        default:
            Issue.record("Expected a successful Composition RDF graph page")
            throw TestFailure.unexpectedResponse
        }
    }
    #endif

    private func request(
        pageLimit: UInt32,
        continuation: ByteString? = nil,
        budget: ExecutionBudget = ExecutionBudget()
    ) -> QueryExecuteOperation.Request {
        QueryExecuteOperation.Request(
            input: .ir(
                .select(
                    SelectQuery(
                        projection: .all,
                        source: .table(
                            TableRef(DatabaseEndpointEntity.persistableType)
                        ),
                        orderBy: [SortKey(.col("priority"))]
                    )
                )
            ),
            page: QueryExecuteOperation.Page(
                limit: pageLimit,
                continuation: continuation
            ),
            budget: budget
        )
    }

    private func distinctPriorityRequest(
        pageLimit: UInt32,
        continuation: ByteString? = nil
    ) -> QueryExecuteOperation.Request {
        QueryExecuteOperation.Request(
            input: .ir(
                .select(
                    SelectQuery(
                        projection: .distinctItems([
                            .column("priority"),
                        ]),
                        source: .table(
                            TableRef(DatabaseEndpointEntity.persistableType)
                        ),
                        orderBy: [SortKey(.col("priority"))]
                    )
                )
            ),
            page: QueryExecuteOperation.Page(
                limit: pageLimit,
                continuation: continuation
            )
        )
    }

    #if DATABASE_SERVER_TEST_VECTOR_INDEXES
    private func vectorRequest(
        pageLimit: UInt32,
        continuation: ByteString? = nil
    ) throws -> QueryExecuteOperation.Request {
        QueryExecuteOperation.Request(
            input: .ir(
                .select(
                    SelectQuery(
                        projection: .all,
                        source: .table(
                            TableRef(
                                DatabaseCompositionVectorEntity.persistableType
                            )
                        ),
                        accessPath: .index(
                            IndexScanSource(
                                indexName:
                                    "DatabaseCompositionVectorEntity_embedding",
                                kindIdentifier: "vector",
                                parameters: [
                                    "fieldName": .string("embedding"),
                                    "dimensions": .int64(2),
                                    "queryVector": .vector(
                                        try Vector(float32: [0, 0])
                                    ),
                                    "k": .int64(3),
                                    "metric": .string("euclidean"),
                                ]
                            )
                        ),
                        limit: 3
                    )
                )
            ),
            page: QueryExecuteOperation.Page(
                limit: pageLimit,
                continuation: continuation
            )
        )
    }

    private func strings(
        _ name: String,
        page: QueryRowPage
    ) throws -> [String] {
        let index = try #require(page.columns.firstIndex { $0.name == name })
        return try page.materializedRows(maximumCount: page.rowCount).map {
            row in
            guard case .string(let value) = row.values[index] else {
                throw TestFailure.unexpectedResponse
            }
            return value
        }
    }

    private func distances(_ page: QueryRowPage) throws -> [Double] {
        try page.materializedRows(maximumCount: page.rowCount).map { row in
            guard case .float64(let value)? = row.annotations["distance"] else {
                throw TestFailure.unexpectedResponse
            }
            return value
        }
    }
    #endif

    private func successfulPage(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        fixture: Fixture
    ) async throws -> QueryRowPage {
        let response = try await invoke(
            request,
            requestID: requestID,
            fixture: fixture
        )
        switch response {
        case .success(.rows(let page)):
            return page
        case .failure(let error):
            Issue.record(
                "Expected a successful Composition row page, got \(error.code): \(error.message)"
            )
            throw TestFailure.unexpectedResponse
        default:
            Issue.record("Expected a successful Composition row page")
            throw TestFailure.unexpectedResponse
        }
    }

    private func remoteFailure(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        fixture: Fixture
    ) async throws -> RemoteOperationError {
        let response = try await invoke(
            request,
            requestID: requestID,
            fixture: fixture
        )
        guard case .failure(let error) = response else {
            Issue.record("Expected a typed Composition query failure")
            throw TestFailure.unexpectedResponse
        }
        return error
    }

    private func invoke(
        _ request: QueryExecuteOperation.Request,
        requestID: UInt64,
        fixture: Fixture
    ) async throws -> Result<
        QueryExecuteOperation.Response,
        RemoteOperationError
    > {
        let bytes = try DatabaseWireEncoder().encodeRequest(
            DatabaseOperations.queryExecute,
            requestID: requestID,
            target: .composition(fixture.compositionID),
            request: request
        )
        return try DatabaseWireDecoder().decodeResponse(
            DatabaseOperations.queryExecute,
            from: try await fixture.endpoint.execute(
                bytes,
                context: DatabaseRequestExecutionContext(
                    authorization: TestBaseEnvironment.authorization
                )
            ),
            matching: requestID
        )
    }

    private func priorities(_ page: QueryRowPage) throws -> [Int64] {
        let index = try #require(
            page.columns.firstIndex { $0.name == "priority" }
        )
        return try page.materializedRows(maximumCount: page.rowCount).map {
            row in
            guard case .int64(let value) = row.values[index] else {
                throw TestFailure.unexpectedResponse
            }
            return value
        }
    }

    private func value(
        _ name: String,
        row: DatabaseWire.QueryRow,
        page: QueryRowPage
    ) throws -> FieldValue {
        let index = try #require(page.columns.firstIndex { $0.name == name })
        return row.values[index]
    }

    private enum TestFailure: Error {
        case unexpectedResponse
    }
}
