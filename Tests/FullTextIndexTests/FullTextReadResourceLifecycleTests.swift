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
}

@Suite("Full-text read resource lifecycle", .serialized)
struct FullTextReadResourceLifecycleTests {
    @Test("Rejected union releases reservations before the meter is reused")
    func rejectedUnionReleasesReservationsBeforeReuse() async throws {
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
                body: "common alpha"
            )
        )
        try context.insert(
            FullTextReadResourceArticle(
                id: "second",
                body: "common beta"
            )
        )
        try context.insert(
            FullTextReadResourceArticle(
                id: "third",
                body: "beta unique"
            )
        )
        try await context.save()

        let execution = ReadExecutionContext(
            options: ReadExecutionOptions(
                budget: ExecutionBudget(maximumIntermediateBytes: 1_300)
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
                    stage: .indexScan,
                    consumed: _,
                    requested: _,
                    maximum: 1_300
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
}
