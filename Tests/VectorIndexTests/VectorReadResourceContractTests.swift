import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine
@testable import VectorIndex

@Persistable
private struct RetainedVectorDocument {
    #Directory<RetainedVectorDocument>("retained-vector-documents")
    #Index(
        .vector(
            name: "RetainedVectorDocument_embedding",
            embedding: \RetainedVectorDocument.embedding,
            dimensions: 2
        )
    )

    var id: String
    var title: String
    var embedding: Vector
}

@Persistable
private struct SecuredVectorDocument: SecurityPolicy {
    #Directory<SecuredVectorDocument>("secured-vector-documents")
    #Index(
        .vector(
            name: "SecuredVectorDocument_embedding",
            embedding: \SecuredVectorDocument.embedding,
            dimensions: 2
        )
    )

    var id: String

    @Restricted(read: .roles(["vector-reader"]))
    var embedding: Vector = Vector(uint8: [])

    static func permitsRead(
        of resource: borrowing SecuredVectorDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsCreate(
        _ newResource: borrowing SecuredVectorDocument,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Polymorphable(identifier: "RetainedVectorAsset")
@PolymorphicDirectory("retained-vector-assets")
@PolymorphicIndex(
    .vector(
        name: "RetainedVectorAsset_embedding",
        embedding: "embedding",
        dimensions: 2,
        metric: .cosine
    )
)
private protocol RetainedVectorAsset:
    Polymorphable<RetainedVectorAssetPolymorphicGroup>
{
    var id: String { get }
    var embedding: Vector { get }
}

@Persistable
private struct RetainedVectorArticle: RetainedVectorAsset {
    #Directory<RetainedVectorArticle>("retained-vector-articles")

    var id: String
    var embedding: Vector
    var body: String
}

@Persistable
private struct RetainedVectorReport: RetainedVectorAsset {
    #Directory<RetainedVectorReport>("retained-vector-reports")

    var id: String
    var embedding: Vector
    var pageCount: Int64
}

@Polymorphable(identifier: "SecuredVectorAsset")
@PolymorphicDirectory("secured-vector-assets")
@PolymorphicIndex(
    .vector(
        name: "SecuredVectorAsset_embedding",
        embedding: "embedding",
        dimensions: 2,
        metric: .cosine
    )
)
private protocol SecuredVectorAsset:
    Polymorphable<SecuredVectorAssetPolymorphicGroup>
{
    var id: String { get }
    var embedding: Vector { get }
}

@Persistable
private struct SecuredVectorArticle: SecuredVectorAsset, SecurityPolicy {
    #Directory<SecuredVectorArticle>("secured-vector-articles")

    var id: String

    @Restricted(read: .roles(["vector-reader"]))
    var embedding: Vector = Vector(uint8: [])

    static func permitsRead(
        of resource: borrowing SecuredVectorArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsCreate(
        _ newResource: borrowing SecuredVectorArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

private enum RetainedApproximateAlgorithm: Sendable {
    case ivf
    case pq

    var name: String {
        switch self {
        case .ivf: "ivf"
        case .pq: "pq"
        }
    }
}

@Suite("Vector read resource contract", .serialized)
struct VectorReadResourceContractTests {
    @Test("Match owner retains every claim until release")
    func matchOwnerRetainsEveryClaimUntilRelease() throws {
        let meter = makeMeter()
        var matches: VectorRetainedMatches?
        do {
            var accumulator = try VectorSearchAccumulator(
                k: 1,
                workMeter: meter
            )
            try accumulator.insert(
                packedPrimaryKey: Tuple("retained-vector-id").pack(),
                distance: 0.25
            )
            matches = try accumulator.finish()
        }

        #expect(matches?.count == 1)
        #expect(matches?.distance(at: 0) == 0.25)
        #expect(meter.retainedIntermediateRows > 0)
        #expect(meter.retainedIntermediateBytes > 0)

        var observedIdentifier: String?
        try matches?.withRetainedPrimaryKey(at: 0) { primaryKey in
            guard case .string(let identifier) = try primaryKey.value(at: 0)
            else {
                return
            }
            observedIdentifier = identifier
        }
        #expect(observedIdentifier == "retained-vector-id")

        matches = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Top-k capacity is admitted before scanning")
    func topKCapacityIsAdmittedBeforeScanning() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let meter = makeMeter(maximumIntermediateBytes: 1)
        let reader = FlatVectorIndexReader(
            subspace: Subspace(prefix: Tuple("vector", "bounded").pack()),
            dimensions: 2,
            metric: .euclidean
        )

        await #expect(throws: DatabaseWorkLimitError.self) {
            _ = try await engine.withTransaction { transaction in
                try await reader.search(
                    queryVector: try Vector(float32: [0, 0]),
                    k: 8,
                    transaction: transaction,
                    snapshot: false,
                    workMeter: meter
                )
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Top-k rejects a match retained by another request")
    func topKRejectsForeignWorkMeter() throws {
        let sourceMeter = makeMeter()
        let destinationMeter = makeMeter()

        do {
            var builder = try VectorRetainedMatches.Builder(
                limit: 1,
                workMeter: destinationMeter
            )
            let match = try VectorRetainedMatch.make(
                packedPrimaryKey: Tuple("foreign-vector-id").pack(),
                distance: 0.5,
                workMeter: sourceMeter
            )
            #expect(
                throws: DatabaseIntermediateReservationError.workMeterMismatch
            ) {
                try builder.consider(match)
            }
        }

        #expect(sourceMeter.retainedIntermediateRows == 0)
        #expect(sourceMeter.retainedIntermediateBytes == 0)
        #expect(destinationMeter.retainedIntermediateRows == 0)
        #expect(destinationMeter.retainedIntermediateBytes == 0)
    }

    @Test("Later malformed vector releases earlier retained matches")
    func laterMalformedVectorReleasesEarlierMatches() async throws {
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let subspace = Subspace(
            prefix: Tuple("vector", "later-malformed").pack()
        )
        try await engine.withTransaction { transaction in
            try transaction.setValue(
                VectorConversion.floatArrayToBytes([0, 0]),
                for: subspace.pack(Tuple("first"))
            )
            try transaction.setValue(
                ByteString([0]),
                for: subspace.pack(Tuple("second"))
            )
        }
        let meter = makeMeter()

        await #expect(throws: VectorIndexError.self) {
            _ = try await engine.withTransaction { transaction in
                try await FlatVectorIndexReader(
                    subspace: subspace,
                    dimensions: 2,
                    metric: .euclidean
                ).search(
                    queryVector: try Vector(float32: [0, 0]),
                    k: 2,
                    transaction: transaction,
                    snapshot: false,
                    workMeter: meter
                )
            }
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("IVF and PQ untrained reads are bounded, deterministic, and retained")
    func approximateUntrainedReadsAreBoundedDeterministicAndRetained() async throws {
        for algorithm in [
            RetainedApproximateAlgorithm.ivf,
            .pq,
        ] {
            let storage = ControlledStorageEngine(base: InMemoryEngine())
            defer { await storage.shutdown() }
            let subspace = Subspace(
                prefix: Tuple("vector", algorithm.name, "bounded").pack()
            )
            try await writeUntrainedFixture(
                algorithm: algorithm,
                subspace: subspace,
                values: [
                    ("a", VectorConversion.floatArrayToBytes([0, 0])),
                    ("b", VectorConversion.floatArrayToBytes([2, 0])),
                ],
                storage: storage
            )
            let meter = makeMeter()
            var matches: VectorRetainedMatches? = try await storage
                .withTransaction { transaction in
                try await searchUntrained(
                    algorithm: algorithm,
                    subspace: subspace,
                    query: try Vector(float32: [0, 0]),
                    transaction: transaction,
                    workMeter: meter
                )
            }

            var identifiers: [String] = []
            for index in 0..<(matches?.count ?? 0) {
                try matches?.withRetainedPrimaryKey(at: index) { primaryKey in
                    guard case .string(let identifier) =
                        try primaryKey.value(at: 0) else {
                        return
                    }
                    identifiers.append(identifier)
                }
            }
            #expect(identifiers == ["a", "b"])
            #expect(!storage.control.boundedValueReadMaximums.isEmpty)
            #expect(
                storage.control.boundedValueReadMaximums.allSatisfy {
                    $0 >= 0 && $0 <= 64 * 1_024
                }
            )
            #expect(meter.retainedIntermediateRows > 0)
            #expect(meter.retainedIntermediateBytes > 0)

            matches = nil
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("IVF and PQ release earlier matches after later corruption")
    func approximateReadersReleaseEarlierMatchesAfterLaterCorruption() async throws {
        for algorithm in [
            RetainedApproximateAlgorithm.ivf,
            .pq,
        ] {
            let storage = ControlledStorageEngine(base: InMemoryEngine())
            defer { await storage.shutdown() }
            let subspace = Subspace(
                prefix: Tuple("vector", algorithm.name, "corruption").pack()
            )
            try await writeUntrainedFixture(
                algorithm: algorithm,
                subspace: subspace,
                values: [
                    ("a", VectorConversion.floatArrayToBytes([0, 0])),
                    ("b", ByteString([0])),
                ],
                storage: storage
            )
            let meter = makeMeter()

            await #expect(throws: VectorIndexError.self) {
                _ = try await storage.withTransaction { transaction in
                    try await searchUntrained(
                        algorithm: algorithm,
                        subspace: subspace,
                        query: try Vector(float32: [0, 0]),
                        transaction: transaction,
                        workMeter: meter
                    )
                }
            }
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("IVF and PQ cancellation closes cursors and releases reservations")
    func approximateReaderCancellationReleasesResources() async throws {
        for algorithm in [
            RetainedApproximateAlgorithm.ivf,
            .pq,
        ] {
            let storage = ControlledStorageEngine(base: InMemoryEngine())
            defer { await storage.shutdown() }
            let subspace = Subspace(
                prefix: Tuple("vector", algorithm.name, "cancel").pack()
            )
            try await writeUntrainedFixture(
                algorithm: algorithm,
                subspace: subspace,
                values: [
                    ("a", VectorConversion.floatArrayToBytes([0, 0])),
                ],
                storage: storage
            )
            let meter = makeMeter()
            let barrier = storage.control.suspendNextRangeAdvance()

            try await storage.withTransaction { transaction in
                let execution = Task {
                    try await searchUntrained(
                        algorithm: algorithm,
                        subspace: subspace,
                        query: try Vector(float32: [0, 0]),
                        transaction: transaction,
                        workMeter: meter
                    )
                }
                await barrier.waitUntilEntered()
                execution.cancel()
                barrier.release()
                do {
                    _ = try await execution.value
                    Issue.record(
                        "Expected \(algorithm.name) cancellation"
                    )
                } catch is CancellationError {
                    // Expected.
                } catch {
                    Issue.record(
                        "Expected CancellationError for \(algorithm.name), got \(error)"
                    )
                }
            }

            #expect(
                storage.control.finishedRangeCursorCount
                    == storage.control.openedRangeCursorCount
            )
            #expect(meter.retainedIntermediateRows == 0)
            #expect(meter.retainedIntermediateBytes == 0)
        }
    }

    @Test("HNSW normalization is admitted before allocation")
    func hnswNormalizationIsAdmittedBeforeAllocation() throws {
        let storage = makeHNSWStorage("admission")
        let meter = makeMeter(maximumIntermediateBytes: 1)
        let query = try extremeCosineVector()

        #expect(throws: DatabaseWorkLimitError.self) {
            _ = try storage.retainedGraphVector(
                from: query,
                workMeter: meter
            )
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("HNSW normalization retains its admission through use")
    func hnswNormalizationRetainsAdmissionThroughUse() throws {
        let storage = makeHNSWStorage("lifetime")
        let meter = makeMeter()
        var retained: HNSWRetainedGraphVector? = try storage
            .retainedGraphVector(
                from: try extremeCosineVector(),
                workMeter: meter
            )

        #expect(retained != nil)
        #expect(meter.retainedIntermediateBytes > 0)
        withExtendedLifetime(retained) {}

        retained = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("HNSW cache rejection transfers restore ownership to the request")
    func hnswCacheRejectionRetainsRestoreOwnership() async throws {
        let container = try await makeRegularContainer(
            algorithm: .hnsw(.default),
            runtimeIdentifier: "vector-hnsw-request-owned-restore"
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            RetainedVectorDocument(
                id: "hnsw-retained",
                title: "HNSW Retained",
                embedding: try Vector(float32: [1, 0])
            )
        )
        try await context.save()
        let meter = makeMeter(maximumIntermediateBytes: 16 * 1_024 * 1_024)
        var retained: HNSWRetainedSearchSnapshot? = try await container
            .withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "RetainedVectorDocument_embedding",
                    indexType: .vector,
                    for: RetainedVectorDocument.self,
                    transaction: transaction
                )
            )
            let storage = HNSWIndexStorage(
                subspace: readable.subspace,
                dimensions: 2,
                metric: .cosine,
                parameters: HNSWParameters(
                    m: 16,
                    efConstruction: 200,
                    efSearch: 50
                ),
                graphCache: HNSWGraphCache(maximumCost: 0),
                resourceLimits: .default
            )
            let retained = try await storage.loadSearchSnapshot(
                transaction: transaction,
                snapshot: false,
                workMeter: meter
            )
            #expect(meter.retainedIntermediateBytes > 0)
            let results = try retained.search(
                queryVector: try Vector(float32: [1, 0]),
                k: 1,
                efSearch: 50,
                workMeter: meter
            )
            #expect(results.count == 1)
            return retained
        }

        #expect(retained != nil)
        #expect(meter.retainedIntermediateBytes > 0)
        retained = nil
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Regular canonical execution releases its session meter")
    func regularCanonicalExecutionReleasesSessionMeter() async throws {
        let container = try await makeRegularContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            RetainedVectorDocument(
                id: "near",
                title: "Near",
                embedding: try Vector(float32: [1, 0])
            )
        )
        try context.insert(
            RetainedVectorDocument(
                id: "far",
                title: "Far",
                embedding: try Vector(float32: [0, 1])
            )
        )
        try await context.save()

        let execution = ReadExecutionContext(
            options: ReadExecutionOptions(
                budget: ExecutionBudget(
                    maximumWorkUnits: 100_000,
                    maximumIntermediateRows: 128,
                    maximumIntermediateBytes: 1_048_576
                )
            ),
            monotonicClock: container.monotonicClock
        )
        let query = try context.findSimilar(RetainedVectorDocument.self)
            .vector(RetainedVectorDocument.fields.embedding, dimensions: 2)
            .query(try Vector(float32: [1, 0]), k: 1)
            .toSelectQuery()
        let response = try await context.executeCanonicalQuery(
            query,
            execution: execution
        )

        #expect(response.rows.count == 1)
        #expect(response.rows[0].annotations["distance"]?.float64Value == 0)
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Regular canonical IVF and PQ executors use bounded point reads")
    func approximateCanonicalExecutorsUseBoundedPointReads() async throws {
        let algorithms: [(name: String, value: VectorAlgorithm)] = [
            (
                "ivf",
                .ivf(try VectorIVFParameters(
                    nlist: 2,
                    nprobe: 1,
                    kmeansIterations: 1
                ))
            ),
            ("pq", .pq(try VectorPQParameters(m: 1, niter: 1))),
        ]
        for algorithm in algorithms {
            let storage = ControlledStorageEngine(base: InMemoryEngine())
            let container = try await makeRegularContainer(
                algorithm: algorithm.value,
                storageEngine: storage,
                runtimeIdentifier: "vector-canonical-\(algorithm.name)"
            )
            do {
                let context = container.testBaseContext()
                try await initializeApproximateMetadata(
                    algorithmName: algorithm.name,
                    context: context,
                    container: container
                )
                try context.insert(
                    RetainedVectorDocument(
                        id: "near",
                        title: "Near",
                        embedding: try Vector(float32: [1, 0])
                    )
                )
                try await context.save()
                let boundedReadsBefore = storage.control
                    .boundedValueReadMaximums.count
                let execution = ReadExecutionContext(
                    options: ReadExecutionOptions(
                        budget: ExecutionBudget(
                            maximumWorkUnits: 100_000,
                            maximumIntermediateRows: 128,
                            maximumIntermediateBytes: 64 * 1_024
                        )
                    ),
                    monotonicClock: container.monotonicClock
                )
                let query = try context
                    .findSimilar(RetainedVectorDocument.self)
                    .vector(
                        RetainedVectorDocument.fields.embedding,
                        dimensions: 2
                    )
                    .query(try Vector(float32: [1, 0]), k: 1)
                    .toSelectQuery()
                let response = try await context.executeCanonicalQuery(
                    query,
                    execution: execution
                )

                #expect(response.rows.count == 1)
                let maxima = storage.control.boundedValueReadMaximums
                    .dropFirst(boundedReadsBefore)
                #expect(!maxima.isEmpty)
                #expect(maxima.allSatisfy { $0 >= 0 && $0 <= 64 * 1_024 })
                #expect(execution.workMeter.retainedIntermediateRows == 0)
                #expect(execution.workMeter.retainedIntermediateBytes == 0)
            } catch {
                await container.shutdown()
                throw error
            }
            await container.shutdown()
        }
    }

    @Test("Authorization denial precedes vector-index storage reads")
    func authorizationDenialPrecedesVectorIndexStorageReads() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeSecuredContainer(storage: storage)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            SecuredVectorDocument(
                id: "secured",
                embedding: try Vector(float32: [1, 0])
            )
        )
        try await context.save()
        let readsBefore = storage.control.dataReadOperationCount

        await #expect {
            _ = try await context.findSimilar(SecuredVectorDocument.self)
                .vector(SecuredVectorDocument.fields.embedding, dimensions: 2)
                .query(try Vector(float32: [1, 0]), k: 1)
                .execute()
        } throws: { error in
            guard case .readNotAllowed(let type, let fields) = error
                as? FieldSecurityError
            else {
                return false
            }
            return type == SecuredVectorDocument.persistableType
                && fields == ["embedding"]
        }
        #expect(storage.control.dataReadOperationCount == readsBefore)
    }

    @Test("Polymorphic authorization denial precedes vector-index storage reads")
    func polymorphicAuthorizationDenialPrecedesStorageReads() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeSecuredPolymorphicContainer(
            storage: storage
        )
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            SecuredVectorArticle(
                id: "secured-polymorphic",
                embedding: try Vector(float32: [1, 0])
            )
        )
        try await context.save()
        let readsBefore = storage.control.dataReadOperationCount

        await #expect {
            _ = try await context.findPolymorphic(SecuredVectorArticle.self)
                .vector(SecuredVectorArticle.fields.embedding, dimensions: 2)
                .query([1, 0], k: 1)
                .executePage()
        } throws: { error in
            guard case .readNotAllowed(let type, let fields) = error
                as? FieldSecurityError else {
                return false
            }
            return type == SecuredVectorArticle.persistableType
                && fields == ["embedding"]
        }
        #expect(storage.control.dataReadOperationCount == readsBefore)
    }

    @Test("Polymorphic canonical execution uses retained member rows")
    func polymorphicCanonicalExecutionUsesRetainedMemberRows() async throws {
        let container = try await makePolymorphicContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let article = RetainedVectorArticle(
            id: "article",
            embedding: try Vector(float32: [1, 0]),
            body: "body"
        )
        let report = RetainedVectorReport(
            id: "report",
            embedding: try Vector(float32: [0, 1]),
            pageCount: 1
        )
        try context.insert(article)
        try context.insert(report)
        try await context.save()

        let page = try await context.findPolymorphic(
            RetainedVectorArticle.self
        )
        .vector(RetainedVectorArticle.fields.embedding, dimensions: 2)
        .query([1, 0], k: 2)
        .executePage()
        let identifiers = try Set(page.results.compactMap { result in
            if let article = try result.decodedModel(
                as: RetainedVectorArticle.self
            ) {
                return article.id
            }
            return try result.decodedModel(
                as: RetainedVectorReport.self
            )?.id
        })

        #expect(identifiers == Set([article.id, report.id]))
    }

    private func writeUntrainedFixture(
        algorithm: RetainedApproximateAlgorithm,
        subspace: Subspace,
        values: [(identifier: String, payload: ByteString)],
        storage: ControlledStorageEngine<InMemoryEngine>
    ) async throws {
        try await storage.withTransaction { transaction in
            let metadata: ByteString
            let valueSubspace: Subspace
            switch algorithm {
            case .ivf:
                metadata = Tuple(
                    IVFMetadata.formatVersion,
                    Int64(2),
                    Int64(2),
                    false,
                    Int64(values.count)
                ).pack()
                valueSubspace = subspace
                    .subspace(IVFIndexStorageKey.lists.rawValue)
                    .subspace(Int64(0))
            case .pq:
                metadata = Tuple(
                    PQMetadata.formatVersion,
                    Int64(1),
                    Int64(2),
                    false,
                    Int64(values.count)
                ).pack()
                valueSubspace = subspace.subspace(
                    PQIndexStorageKey.vectors.rawValue
                )
            }
            try transaction.setValue(
                metadata,
                for: subspace.pack(Tuple([metadataKey(for: algorithm)]))
            )
            for value in values {
                try transaction.setValue(
                    value.payload,
                    for: valueSubspace.pack(Tuple(value.identifier))
                )
            }
        }
    }

    private func searchUntrained(
        algorithm: RetainedApproximateAlgorithm,
        subspace: Subspace,
        query: Vector,
        transaction: any TransactionReadAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> VectorRetainedMatches {
        switch algorithm {
        case .ivf:
            return try await IVFIndexReader(
                subspace: subspace,
                dimensions: 2,
                metric: .euclidean,
                parameters: IVFParameters(
                    nlist: 2,
                    nprobe: 1,
                    kmeansIterations: 1
                )
            ).search(
                queryVector: query,
                k: 2,
                transaction: transaction,
                snapshot: false,
                workMeter: workMeter
            )
        case .pq:
            return try await PQIndexReader(
                subspace: subspace,
                dimensions: 2,
                metric: .euclidean,
                parameters: PQParameters(m: 1, niter: 1)
            ).search(
                queryVector: query,
                k: 2,
                transaction: transaction,
                snapshot: false,
                workMeter: workMeter
            )
        }
    }

    private func metadataKey(
        for algorithm: RetainedApproximateAlgorithm
    ) -> Int {
        switch algorithm {
        case .ivf: IVFIndexStorageKey.metadata.rawValue
        case .pq: PQIndexStorageKey.metadata.rawValue
        }
    }

    private func makeRegularContainer(
        algorithm: VectorAlgorithm = .flat,
        storageEngine: any StorageEngine = InMemoryEngine(),
        runtimeIdentifier: String = "vector-retained-regular-tests"
    ) async throws -> DBContainer {
        let provider = VectorIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(RetainedVectorDocument.self)
        try VectorReadExecutors.register(with: &runtime)
        try runtime.register(provider)
        return try await DBContainer.open(
            testing: try Schema(
                entities: [try RetainedVectorDocument.schemaEntity]
            ),
            configuration: .testing(storageEngine: storageEngine),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: runtimeIdentifier,
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                entityRuntimes: [runtime.registration()],
                indexConfigurations: [
                    VectorIndexConfiguration(
                        indexName: "RetainedVectorDocument_embedding",
                        algorithm: algorithm
                    )
                ]
            ),
            security: .testingDisabled
        )
    }

    private func initializeApproximateMetadata(
        algorithmName: String,
        context: DatabaseContext,
        container: DBContainer
    ) async throws {
        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "RetainedVectorDocument_embedding",
                    indexType: .vector,
                    for: RetainedVectorDocument.self,
                    transaction: transaction
                )
            )
            let metadata: ByteString
            let metadataKey: Int
            switch algorithmName {
            case "ivf":
                metadata = Tuple(
                    IVFMetadata.formatVersion,
                    Int64(2),
                    Int64(2),
                    false,
                    Int64(0)
                ).pack()
                metadataKey = IVFIndexStorageKey.metadata.rawValue
            case "pq":
                metadata = Tuple(
                    PQMetadata.formatVersion,
                    Int64(1),
                    Int64(2),
                    false,
                    Int64(0)
                ).pack()
                metadataKey = PQIndexStorageKey.metadata.rawValue
            default:
                throw VectorIndexError.invalidArgument(
                    "Unsupported approximate vector test layout"
                )
            }
            try transaction.setValue(
                metadata,
                for: readable.subspace.pack(Tuple(metadataKey))
            )
        }
    }

    private func makeSecuredContainer(
        storage: ControlledStorageEngine<InMemoryEngine>
    ) async throws -> DBContainer {
        let provider = VectorIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(SecuredVectorDocument.self)
        try VectorReadExecutors.register(with: &runtime)
        try runtime.register(provider)
        return try await DBContainer.open(
            for: try Schema(
                entities: [try SecuredVectorDocument.schemaEntity]
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "vector-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                entityRuntimes: [runtime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(SecuredVectorDocument.self)
                ]
            )
        )
    }

    private func makePolymorphicContainer() async throws -> DBContainer {
        let provider = VectorIndexMaintainerProvider()
        return try await DBContainer.open(
            testing: try Schema(
                entities: [
                    try RetainedVectorArticle.schemaEntity,
                    try RetainedVectorReport.schemaEntity,
                ]
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "vector-retained-polymorphic-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                polymorphicIndexReadExecutors: [
                    VectorReadExecutors.polymorphicIndexExecutor()
                ],
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        RetainedVectorArticle.self
                    ),
                    try DatabaseFrameworkRuntime.entity(
                        RetainedVectorReport.self
                    ),
                ]
            ),
            security: .testingDisabled
        )
    }

    private func makeSecuredPolymorphicContainer(
        storage: ControlledStorageEngine<InMemoryEngine>
    ) async throws -> DBContainer {
        let provider = VectorIndexMaintainerProvider()
        return try await DBContainer.open(
            testing: try Schema(
                entities: [try SecuredVectorArticle.schemaEntity]
            ),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "vector-secured-polymorphic-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                polymorphicIndexReadExecutors: [
                    VectorReadExecutors.polymorphicIndexExecutor()
                ],
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        SecuredVectorArticle.self
                    )
                ],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(SecuredVectorArticle.self)
                ]
            )
        )
    }

    private func makeMeter(
        maximumIntermediateBytes: UInt64 = 64 * 1_024
    ) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumWorkUnits: 100_000,
                maximumIntermediateRows: 128,
                maximumIntermediateBytes: maximumIntermediateBytes
            ),
            monotonicClock: TestProcessMonotonicClock()
        )
    }

    private func makeHNSWStorage(_ suffix: String) -> HNSWIndexStorage {
        HNSWIndexStorage(
            subspace: Subspace(
                prefix: Tuple("vector", "hnsw", suffix).pack()
            ),
            dimensions: 2,
            metric: .cosine,
            parameters: .default,
            graphCache: HNSWGraphCache(maximumCost: 1_048_576),
            resourceLimits: .default
        )
    }

    private func extremeCosineVector() throws -> Vector {
        try Vector(
            float32: [
                Float.greatestFiniteMagnitude,
                Float.greatestFiniteMagnitude,
            ]
        )
    }
}
