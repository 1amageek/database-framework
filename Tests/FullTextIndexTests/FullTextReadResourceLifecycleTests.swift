import DatabaseKit
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine
@testable import FullTextIndex

@Persistable
private struct FullTextReadResourceArticle {
    #Directory<FullTextReadResourceArticle>("fulltext-read-resource")
    #Index(
        .text(
            name: "FullTextReadResourceArticle_body",
            fields: [\FullTextReadResourceArticle.body],
            mode: .fullText(tokenizer: .simple)
        )
    )

    var id: String
    var body: String
    var payload: String
}

@Suite("Full-text read resource lifecycle", .serialized)
struct FullTextReadResourceLifecycleTests {
    @Test("Oversized persisted payload is rejected before decode retention")
    func oversizedPersistedPayloadIsRejectedAndReleasesReservation() async throws {
        let schema = try Schema(
            entities: [try FullTextReadResourceArticle.schemaEntity]
        )
        let maintainerProvider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FullTextReadResourceArticle.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(maintainerProvider)
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-payload-resource-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: maintainerProvider)
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try context.insert(
            FullTextReadResourceArticle(
                id: "oversized",
                body: "oversized",
                payload: String(repeating: "x", count: 120_000)
            )
        )
        try context.insert(
            FullTextReadResourceArticle(
                id: "small",
                body: "small",
                payload: "small"
            )
        )
        try await context.save()

        let execution = ReadExecutionContext(
            options: ReadExecutionOptions(
                budget: ExecutionBudget(
                    maximumIntermediateBytes: 8_000
                )
            ),
            monotonicClock: container.monotonicClock
        )
        let oversizedQuery = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["oversized"], mode: .all)
        .toSelectQuery()

        await #expect {
            _ = try await context.executeCanonicalQuery(
                oversizedQuery,
                execution: execution
            )
        } throws: { error in
            guard case .maximumIntermediateBytes(
                stage: .storageRow,
                consumed: _,
                requested: _,
                maximum: 8_000
            ) = error as? DatabaseWorkLimitError else {
                return false
            }
            return true
        }
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)

        let retryQuery = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["small"], mode: .all)
        .toSelectQuery()
        let retry = try await context.executeCanonicalQuery(
            retryQuery,
            execution: execution
        )
        #expect(retry.rows.count == 1)
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Rejected read releases reservations before the meter is reused")
    func rejectedReadReleasesReservationsBeforeReuse() async throws {
        let schema = try Schema(
            entities: [try FullTextReadResourceArticle.schemaEntity]
        )
        let maintainerProvider = FullTextIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FullTextReadResourceArticle.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(maintainerProvider)
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-resource-lifecycle-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: maintainerProvider)
                ],
                entityRuntimes: [
                    entityRuntime.registration()
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try context.insert(
            FullTextReadResourceArticle(
                id: "first",
                body: "common alpha",
                payload: "first"
            )
        )
        try context.insert(
            FullTextReadResourceArticle(
                id: "second",
                body: "common beta",
                payload: "second"
            )
        )
        try context.insert(
            FullTextReadResourceArticle(
                id: "third",
                body: "beta unique",
                payload: "third"
            )
        )
        try await context.save()

        let execution = ReadExecutionContext(
            options: ReadExecutionOptions(
                budget: ExecutionBudget(maximumIntermediateBytes: 2_500)
            ),
            monotonicClock: container.monotonicClock
        )
        let rejectedQuery = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["common", "beta"], mode: .any)
        .toSelectQuery()

        do {
            _ = try await context.executeCanonicalQuery(
                rejectedQuery,
                execution: execution
            )
            Issue.record("The union must exceed the intermediate byte budget")
        } catch let workError as DatabaseWorkLimitError {
            guard case .maximumIntermediateBytes(
                    stage: _,
                    consumed: _,
                    requested: _,
                    maximum: 2_500
                  ) = workError else {
                Issue.record("Unexpected work limit: \(workError)")
                return
            }
        } catch {
            throw error
        }
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)

        let retryQuery = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["unique"], mode: .all)
        .toSelectQuery()
        let retry = try await context.executeCanonicalQuery(
            retryQuery,
            execution: execution
        )

        #expect(retry.rows.count == 1)
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Canonical plain reads preserve items and index order")
    func plainCanonicalReadPreservesItemsAndOrderAndReleasesResources() async throws {
        let container = try await makeFullTextReadResourceContainer(
            identifier: "fulltext-plain-read-resource-tests"
        )
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try await saveFullTextReadResourceArticles(
            [
                FullTextReadResourceArticle(
                    id: "first",
                    body: "swift database",
                    payload: "news"
                ),
                FullTextReadResourceArticle(
                    id: "second",
                    body: "swift engine",
                    payload: "guide"
                ),
                FullTextReadResourceArticle(
                    id: "outside",
                    body: "database engine",
                    payload: "reference"
                ),
            ],
            in: context
        )

        let execution = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        let query = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["swift"], mode: .all)
        .toSelectQuery()

        let response = try await context.executeCanonicalQuery(
            query,
            execution: execution
        )

        #expect(response.rows.map { $0.fields["id"]?.stringValue } == [
            "first",
            "second",
        ])
        #expect(response.rows.map { $0.fields["body"]?.stringValue } == [
            "swift database",
            "swift engine",
        ])
        #expect(response.rows.allSatisfy { $0.annotations.isEmpty })
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Canonical scored reads preserve scores and ranked order")
    func scoredCanonicalReadPreservesScoresAndOrderAndReleasesResources() async throws {
        let container = try await makeFullTextReadResourceContainer(
            identifier: "fulltext-scored-read-resource-tests"
        )
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try await saveFullTextReadResourceArticles(
            [
                FullTextReadResourceArticle(
                    id: "low",
                    body: "swift",
                    payload: "news"
                ),
                FullTextReadResourceArticle(
                    id: "high",
                    body: "swift swift swift",
                    payload: "guide"
                ),
                FullTextReadResourceArticle(
                    id: "outside-one",
                    body: "database",
                    payload: "reference"
                ),
                FullTextReadResourceArticle(
                    id: "outside-two",
                    body: "engine",
                    payload: "reference"
                ),
                FullTextReadResourceArticle(
                    id: "outside-three",
                    body: "storage",
                    payload: "reference"
                ),
            ],
            in: context
        )

        let execution = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        let query = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["swift"], mode: .all)
        .bm25()
        .toSelectQuery(returnScores: true)

        let response = try await context.executeCanonicalQuery(
            query,
            execution: execution
        )

        var identifiers: [String] = []
        var scores: [Double] = []
        for row in response.rows {
            identifiers.append(try #require(row.fields["id"]?.stringValue))
            scores.append(try #require(row.annotations["score"]?.float64Value))
        }
        #expect(identifiers == ["high", "low"])
        #expect(response.rows.map { $0.fields["body"]?.stringValue } == [
            "swift swift swift",
            "swift",
        ])
        #expect(scores.count == 2)
        guard scores.count == 2 else { return }
        #expect(scores.allSatisfy { $0.isFinite && $0 > 0 })
        #expect(scores[0] > scores[1])
        #expect(response.rows.allSatisfy { $0.annotations.count == 1 })
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }

    @Test("Canonical faceted reads preserve items and facet metadata")
    func facetedCanonicalReadPreservesItemsAndFacetMetadataAndReleasesResources() async throws {
        let container = try await makeFullTextReadResourceContainer(
            identifier: "fulltext-faceted-read-resource-tests"
        )
        defer { await container.shutdown() }

        let context = container.testBaseContext()
        try await saveFullTextReadResourceArticles(
            [
                FullTextReadResourceArticle(
                    id: "first",
                    body: "swift alpha",
                    payload: "news"
                ),
                FullTextReadResourceArticle(
                    id: "second",
                    body: "swift beta",
                    payload: "news"
                ),
                FullTextReadResourceArticle(
                    id: "third",
                    body: "swift gamma",
                    payload: "guide"
                ),
                FullTextReadResourceArticle(
                    id: "outside",
                    body: "database",
                    payload: "reference"
                ),
            ],
            in: context
        )

        let execution = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        let query = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["swift"], mode: .all)
        .facet(FullTextReadResourceArticle.fields.payload, limit: 10)
        .toSelectQuery(includeFacets: true)

        let response = try await context.executeCanonicalQuery(
            query,
            execution: execution
        )

        #expect(response.rows.map { $0.fields["id"]?.stringValue } == [
            "first",
            "second",
            "third",
        ])
        #expect(response.rows.map { $0.fields["body"]?.stringValue } == [
            "swift alpha",
            "swift beta",
            "swift gamma",
        ])
        #expect(response.metadata[FullTextReadParameter.totalCount]?.uint64Value == 3)
        let facetKey = FullTextReadParameter.facetMetadataPrefix + "payload"
        let facetMetadata = try #require(response.metadata[facetKey])
        #expect(facetMetadata == .array([
            .array([.string("news"), .uint64(2)]),
            .array([.string("guide"), .uint64(1)]),
        ]))
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)
    }
}

private func makeFullTextReadResourceContainer(
    identifier: String
) async throws -> DBContainer {
    let schema = try Schema(
        entities: [try FullTextReadResourceArticle.schemaEntity]
    )
    let maintainerProvider = FullTextIndexMaintainerProvider()
    var entityRuntime = try EntityRuntimeDefinition(
        FullTextReadResourceArticle.self
    )
    try FullTextReadExecutors.register(with: &entityRuntime)
    try entityRuntime.register(maintainerProvider)
    return try await DBContainer.open(
        for: schema,
        configuration: .testing(storageEngine: InMemoryEngine()),
        runtimeConfiguration: try DatabaseRuntimeConfiguration(
            executionIdentity: DatabaseExecutionRuntimeIdentity(
                identifier: identifier,
                revision: 1
            ),
            indexMaintainerProviderDescriptors: [
                .init(describing: maintainerProvider)
            ],
            entityRuntimes: [entityRuntime.registration()]
        ),
        security: .testingDisabled
    )
}

private func saveFullTextReadResourceArticles(
    _ articles: [FullTextReadResourceArticle],
    in context: DatabaseContext
) async throws {
    for article in articles {
        try context.insert(article)
    }
    try await context.save()
}
