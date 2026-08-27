import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine
@testable import FullTextIndex

@Persistable(type: "FullTextRetainedRegularArticle")
private struct FullTextRetainedRegularArticle {
    #Directory<FullTextRetainedRegularArticle>(
        "fulltext-retained-regular-article"
    )
    #Index(
        .text(
            name: "FullTextRetainedRegularArticle_body_fulltext",
            fields: [\FullTextRetainedRegularArticle.body],
            mode: .fullText(tokenizer: .simple)
        )
    )

    var id: String
    var body: String
}

@Polymorphable(identifier: "FullTextRetainedDocument")
@PolymorphicDirectory("fulltext-retained-document")
@PolymorphicIndex(
    .text(
        name: "FullTextRetainedDocument_body_fulltext",
        fields: ["body"],
        mode: .fullText(tokenizer: .simple)
    )
)
private protocol FullTextRetainedDocument:
    Polymorphable<FullTextRetainedDocumentPolymorphicGroup>
{
    var id: String { get }
    var body: String { get }
}

@Persistable
private struct FullTextRetainedArticle: FullTextRetainedDocument {
    #Directory<FullTextRetainedArticle>("fulltext-retained-article")

    var id: String
    var body: String
}

@Persistable
private struct FullTextRetainedReport: FullTextRetainedDocument {
    #Directory<FullTextRetainedReport>("fulltext-retained-report")

    var id: String
    var body: String
}

@Persistable(type: "FullTextDeniedArticle")
private struct FullTextDeniedArticle: SecurityPolicy {
    #Directory<FullTextDeniedArticle>("fulltext-denied-article")
    #Index(
        .text(
            name: "FullTextDeniedArticle_body_fulltext",
            fields: [\FullTextDeniedArticle.body],
            mode: .fullText(tokenizer: .simple)
        )
    )

    var id: String
    var body: String

    static func permitsRead(
        of resource: borrowing FullTextDeniedArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier == "fulltext-allowed"
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier == "fulltext-allowed"
    }

    static func permitsCreate(
        _ newResource: borrowing FullTextDeniedArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing FullTextDeniedArticle,
        to newResource: borrowing FullTextDeniedArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing FullTextDeniedArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Suite("Full-text retained read contracts", .serialized)
struct FullTextReadContractTests {
    @Test("Regular canonical search retains and releases its result owner")
    func regularCanonicalSearchUsesRetainedResultOwner() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeRegularContainer(storage: storage)
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try context.insert(
            FullTextRetainedRegularArticle(
                id: "regular-article",
                body: "swift database"
            )
        )
        try await context.save()

        let execution = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        let query = try context.search(FullTextRetainedRegularArticle.self)
            .fullText(FullTextRetainedRegularArticle.fields.body)
            .terms(["swift"])
            .toSelectQuery()
        let response = try await context.executeCanonicalQuery(
            query,
            execution: execution
        )

        #expect(response.rows.count == 1)
        guard let row = response.rows.first else {
            Issue.record("The canonical full-text search returned no rows")
            return
        }
        #expect(row.fields["id"]?.stringValue == "regular-article")
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
        #expect(!storage.control.boundedValueReadMaximums.isEmpty)
    }

    @Test("Polymorphic canonical search consumes retained entries")
    func polymorphicCanonicalSearchPreservesConcreteRows() async throws {
        let container = try await makePolymorphicContainer()
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try context.insert(
            FullTextRetainedArticle(
                id: "article",
                body: "swift database"
            )
        )
        try context.insert(
            FullTextRetainedReport(
                id: "report",
                body: "swift engine"
            )
        )
        try await context.save()

        let page = try await context.findPolymorphic(
            FullTextRetainedArticle.self
        )
        .fullText(FullTextRetainedArticle.fields.body)
        .terms(["swift"])
        .executePage()

        #expect(page.results.count == 2)
        #expect(Set(page.results.map(\.typeName)) == Set([
            FullTextRetainedArticle.persistableType,
            FullTextRetainedReport.persistableType,
        ]))
        for result in page.results {
            #expect(result.annotations.count == 2)
            #expect(
                result.annotations[PolymorphicRowAnnotation.typeName]?.stringValue
                    == result.typeName
            )
            #expect(
                result.annotations[PolymorphicRowAnnotation.typeCode]?.int64Value
                    == result.typeCode
            )
        }
    }

    @Test("Autocomplete promotes only the requested sorted prefix")
    func autocompleteLimitIsAppliedAfterOrdering() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let root = Subspace(
            prefix: Tuple("fulltext-autocomplete-contract").pack()
        )
        defer { await storage.shutdown() }
        try await writeAutocompleteSuggestions(
            storage: storage,
            root: root,
            entries: [
                ("swift", 10),
                ("swirl", 30),
                ("sweet", 20),
            ]
        )

        let reader = AutocompleteIndexReader(
            subspace: root,
            minPrefixLength: 1
        )
        let meter = makeMeter()
        let suggestions = try await storage.withTransaction { transaction in
            try await reader.suggestions(
                field: "title",
                prefix: "sw",
                limit: 2,
                transaction: transaction,
                workMeter: meter
            )
        }

        #expect(suggestions.map(\.term) == ["swirl", "sweet"])
        #expect(suggestions.map(\.score) == [30, 20])
        #expect(storage.control.finishedRangeCursorCount == 1)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Malformed autocomplete input finishes its cursor and fails")
    func malformedAutocompleteInputReleasesCursorAndMeter() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let root = Subspace(
            prefix: Tuple("fulltext-autocomplete-malformed").pack()
        )
        defer { await storage.shutdown() }
        let suggestionsSubspace = root
            .subspace("suggestions")
            .subspace("title")
            .subspace("sw")
        try await storage.executeTransaction { transaction in
            try transaction.setValue(
                ByteConversion.int64ToBytes(1),
                for: suggestionsSubspace.pack(Tuple(Int64(1)))
            )
        }

        let reader = AutocompleteIndexReader(
            subspace: root,
            minPrefixLength: 1
        )
        let meter = makeMeter()
        await #expect(throws: FullTextStorageError.self) {
            _ = try await storage.withTransaction { transaction in
                try await reader.suggestions(
                    field: "title",
                    prefix: "sw",
                    limit: 10,
                    transaction: transaction,
                    workMeter: meter
                )
            }
        }

        #expect(storage.control.finishedRangeCursorCount == 1)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Autocomplete cancellation finishes its cursor and releases ownership")
    func cancelledAutocompleteFinishesCursorAndReleasesOwnership() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let root = Subspace(
            prefix: Tuple("fulltext-autocomplete-cancel").pack()
        )
        defer { await storage.shutdown() }
        try await writeAutocompleteSuggestions(
            storage: storage,
            root: root,
            entries: [("swift", 10)]
        )

        let reader = AutocompleteIndexReader(
            subspace: root,
            minPrefixLength: 1
        )
        let meter = makeMeter()
        let barrier = storage.control.suspendNextRangeAdvance()
        let operation: Task<[AutocompleteSuggestion], any Error> = Task {
            try await storage.withTransaction { transaction in
                try await reader.suggestions(
                    field: "title",
                    prefix: "sw",
                    limit: 10,
                    transaction: transaction,
                    workMeter: meter
                )
            }
        }
        let completionMonitor: Task<Void, Never>
        do {
            completionMonitor = try await barrier.waitUntilEntered(
                beforeCompletionOf: operation
            )
        } catch {
            barrier.release()
            operation.cancel()
            _ = await operation.result
            throw error
        }

        operation.cancel()
        barrier.release()
        let result = await operation.result
        await completionMonitor.value

        guard case .failure(let error) = result else {
            Issue.record("Cancelled autocomplete unexpectedly succeeded")
            return
        }
        #expect(error is CancellationError)
        #expect(storage.control.finishedRangeCursorCount == 1)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Authorization denial occurs before full-text storage reads")
    func deniedCanonicalSearchPerformsNoStorageRead() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeDeniedContainer(storage: storage)
        defer { await container.shutdown() }

        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal("fulltext-allowed"),
            access: .write
        )
        try await container.grantTestBaseAccess(
            to: .principal("fulltext-denied"),
            access: .read
        )
        #endif
        let allowed = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "fulltext-allowed")
            )
        )
        try allowed.insert(
            FullTextDeniedArticle(
                id: "denied-article",
                body: "swift database"
            )
        )
        try await allowed.save()

        let denied = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "fulltext-denied")
            )
        )
        let query = try denied.search(FullTextDeniedArticle.self)
            .fullText(FullTextDeniedArticle.fields.body)
            .terms(["swift"])
            .toSelectQuery()
        let execution = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        let readsBefore = storage.control.dataReadOperationCount

        await #expect(throws: SecurityError.self) {
            _ = try await denied.executeCanonicalQuery(
                query,
                execution: execution
            )
        }

        #expect(storage.control.dataReadOperationCount == readsBefore)
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }
}

private func makeMeter() -> DatabaseWorkMeter {
    DatabaseWorkMeter(
        budget: ExecutionBudget(
            maximumRows: 100,
            maximumWorkUnits: 10_000,
            maximumIntermediateRows: 100,
            maximumIntermediateBytes: 100_000,
            timeoutMilliseconds: 30_000
        ),
        monotonicClock: TestProcessMonotonicClock()
    )
}

private func makeRegularContainer(
    storage: any StorageEngine
) async throws -> DBContainer {
    let provider = FullTextIndexMaintainerProvider()
    var runtime = try EntityRuntimeDefinition(
        FullTextRetainedRegularArticle.self
    )
    try FullTextReadExecutors.register(with: &runtime)
    try runtime.register(provider)
    return try await DBContainer.open(
        for: try Schema(
            entities: [try FullTextRetainedRegularArticle.schemaEntity]
        ),
        configuration: .testing(storageEngine: storage),
        runtimeConfiguration: try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "fulltext-retained-regular-contract-tests",
                revision: 1
            ),
            indexMaintainerProviderDescriptors: [
                .init(describing: provider)
            ],
            entityRuntimes: [runtime.registration()]
        ),
        security: .testingDisabled
    )
}

private func makePolymorphicContainer() async throws -> DBContainer {
    let provider = FullTextIndexMaintainerProvider()
    var articleRuntime = try EntityRuntimeDefinition(
        FullTextRetainedArticle.self
    )
    try FullTextReadExecutors.register(with: &articleRuntime)
    try articleRuntime.register(provider)
    var reportRuntime = try EntityRuntimeDefinition(
        FullTextRetainedReport.self
    )
    try FullTextReadExecutors.register(with: &reportRuntime)
    try reportRuntime.register(provider)
    return try await DBContainer.open(
        for: try Schema(
            entities: [
                try FullTextRetainedArticle.schemaEntity,
                try FullTextRetainedReport.schemaEntity,
            ]
        ),
        configuration: .testing(storageEngine: InMemoryEngine()),
        runtimeConfiguration: try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "fulltext-retained-polymorphic-contract-tests",
                revision: 1
            ),
            indexMaintainerProviderDescriptors: [
                .init(describing: provider)
            ],
            polymorphicIndexReadExecutors: [
                FullTextReadExecutors.polymorphicIndexExecutor
            ],
            entityRuntimes: [
                articleRuntime.registration(),
                reportRuntime.registration(),
            ]
        ),
        security: .testingDisabled
    )
}

private func makeDeniedContainer(
    storage: any StorageEngine
) async throws -> DBContainer {
    let provider = FullTextIndexMaintainerProvider()
    var runtime = try EntityRuntimeDefinition(FullTextDeniedArticle.self)
    try FullTextReadExecutors.register(with: &runtime)
    try runtime.register(provider)
    return try await DBContainer.open(
        for: try Schema(
            entities: [try FullTextDeniedArticle.schemaEntity]
        ),
        configuration: .testing(storageEngine: storage),
        runtimeConfiguration: try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: "fulltext-retained-denial-contract-tests",
                revision: 1
            ),
            indexMaintainerProviderDescriptors: [
                .init(describing: provider)
            ],
            entityRuntimes: [runtime.registration()],
            authorizationPolicies: [
                AuthorizationPolicyHandler(FullTextDeniedArticle.self)
            ]
        ),
        security: .enabled()
    )
}

private func writeAutocompleteSuggestions(
    storage: ControlledStorageEngine<InMemoryEngine>,
    root: Subspace,
    entries: [(term: String, score: Int64)]
) async throws {
    let suggestionsSubspace = root
        .subspace("suggestions")
        .subspace("title")
        .subspace("sw")
    try await storage.executeTransaction { transaction in
        for entry in entries {
            try transaction.setValue(
                ByteConversion.int64ToBytes(entry.score),
                for: suggestionsSubspace.pack(Tuple(entry.term))
            )
        }
    }
}
