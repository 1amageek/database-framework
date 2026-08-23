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

@Persistable
private struct FullTextFusionSecuredArticle: SecurityPolicy {
    #Directory<FullTextFusionSecuredArticle>("fulltext-fusion-secured")
    #Index(
        .text(
            name: "FullTextFusionSecuredArticle_body",
            fields: [\FullTextFusionSecuredArticle.body],
            mode: .fullText(tokenizer: .simple)
        )
    )
    #Index(
        .text(
            name: "FullTextFusionSecuredArticle_body_autocomplete",
            fields: [\FullTextFusionSecuredArticle.body],
            mode: .autocomplete()
        )
    )

    var id: String
    var ownerID: String
    var body: String

    static func permitsRead(
        of resource: borrowing FullTextFusionSecuredArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.roles.contains("search") == true
    }

    static func permitsCreate(
        _ newResource: borrowing FullTextFusionSecuredArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        newResource.ownerID == context.principal?.identifier
    }

    static func permitsUpdate(
        from resource: borrowing FullTextFusionSecuredArticle,
        to newResource: borrowing FullTextFusionSecuredArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
            && newResource.ownerID == resource.ownerID
    }

    static func permitsDelete(
        _ resource: borrowing FullTextFusionSecuredArticle,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        resource.ownerID == context.principal?.identifier
    }
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

        let rejectedQuery = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["common", "beta"], mode: .any)
        .toSelectQuery()
        let retryQuery = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["unique"], mode: .all)
        .toSelectQuery()

        let singleMeasurement = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        _ = try await context.executeCanonicalQuery(
            retryQuery,
            execution: singleMeasurement
        )
        let unionMeasurement = ReadExecutionContext(
            monotonicClock: container.monotonicClock
        )
        _ = try await context.executeCanonicalQuery(
            rejectedQuery,
            execution: unionMeasurement
        )
        let constrainedMaximum = singleMeasurement.workMeter
            .peakIntermediateBytes
        #expect(constrainedMaximum > 0)
        #expect(
            unionMeasurement.workMeter.peakIntermediateBytes
                > constrainedMaximum
        )

        let execution = ReadExecutionContext(
            options: ReadExecutionOptions(
                budget: ExecutionBudget(
                    maximumIntermediateBytes: constrainedMaximum
                )
            ),
            monotonicClock: container.monotonicClock
        )

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
                    maximum: let maximum
                  ) = workError else {
                Issue.record("Unexpected work limit: \(workError)")
                return
            }
            #expect(maximum == constrainedMaximum)
        } catch {
            throw error
        }
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)

        let retry = try await context.executeCanonicalQuery(
            retryQuery,
            execution: execution
        )

        #expect(retry.rows.count == 1)
        #expect(execution.workMeter.retainedIntermediateRows == 0)
        #expect(execution.workMeter.retainedIntermediateBytes == 0)

        let commonQuery = try context.search(
            FullTextReadResourceArticle.self
        )
        .fullText(FullTextReadResourceArticle.fields.body)
        .terms(["common"], mode: .all)
        .toSelectQuery()
        guard case .some(.index(let commonSource)) = commonQuery.accessPath,
              case .some(.index(let uniqueSource)) = retryQuery.accessPath else {
            Issue.record("Full-text builders must produce index access paths")
            return
        }
        let fusionExecution = ReadExecutionContext(
            options: .default,
            monotonicClock: container.monotonicClock
        )
        let fusionResponse = try await context.executeCanonicalQuery(
            SelectQuery(
                projection: .all,
                source: commonQuery.source,
                accessPath: .fusion(
                    FusionSource(
                        inputs: [commonSource, uniqueSource],
                        strategy: .reciprocalRank(rankConstant: 0)
                    )
                )
            ),
            execution: fusionExecution
        )

        #expect(fusionResponse.rows.count == 3)
        #expect(fusionResponse.rows.allSatisfy {
            guard let score = $0.annotations[
                FusionSource.scoreAnnotation
            ]?.float64Value else {
                return false
            }
            return score.isFinite
        })
        #expect(fusionExecution.workMeter.retainedIntermediateRows == 0)
        #expect(fusionExecution.workMeter.retainedIntermediateBytes == 0)

        await #expect(throws: BM25ScoringError.invalidK1) {
            _ = try await context.search(FullTextReadResourceArticle.self)
                .fullText(FullTextReadResourceArticle.fields.body)
                .terms(["common"])
                .bm25(k1: .nan)
                .limit(0)
                .executeWithScores()
        }
    }

    @Test("Fusion search preserves LIST and GET authorization")
    func fusionSearchPreservesListAndGetAuthorization() async throws {
        let maintainerProvider = FullTextIndexMaintainerProvider()
        let autocompleteMaintainerProvider =
            AutocompleteIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            FullTextFusionSecuredArticle.self
        )
        try FullTextReadExecutors.register(with: &entityRuntime)
        try entityRuntime.register(maintainerProvider)
        try entityRuntime.register(autocompleteMaintainerProvider)
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try FullTextFusionSecuredArticle.schemaEntity]
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "fulltext-fusion-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: maintainerProvider),
                    .init(describing: autocompleteMaintainerProvider),
                ],
                entityRuntimes: [entityRuntime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(
                        FullTextFusionSecuredArticle.self
                    )
                ]
            ),
            security: .enabled()
        )
        defer { await container.shutdown() }

        let owner = Principal(identifier: "owner", roles: ["search"])
        let reader = Principal(identifier: "reader", roles: ["search"])
        let blocked = Principal(identifier: "blocked")
        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal(owner.identifier),
            access: [.read, .write]
        )
        try await container.grantTestBaseAccess(
            to: .principal(reader.identifier),
            access: .read
        )
        try await container.grantTestBaseAccess(
            to: .principal(blocked.identifier),
            access: .read
        )
        #endif

        let ownerContext = container.testBaseContext(
            authorization: .authenticated(owner)
        )
        try ownerContext.insert(
            FullTextFusionSecuredArticle(
                id: "secured-article",
                ownerID: owner.identifier,
                body: "needle"
            )
        )
        try await ownerContext.save()

        let blockedContext = container.testBaseContext(
            authorization: .authenticated(blocked)
        )
        do {
            _ = try await blockedContext.search(
                FullTextFusionSecuredArticle.self
            )
            .fullText(FullTextFusionSecuredArticle.fields.body)
            .terms([])
            .execute()
            Issue.record("Empty full-text search must enforce LIST authorization")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
        }
        do {
            _ = try await blockedContext.search(
                FullTextFusionSecuredArticle.self
            )
            .fullText(FullTextFusionSecuredArticle.fields.body)
            .terms([])
            .executeWithFacets()
            Issue.record("Empty faceted search must enforce LIST authorization")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
        }
        do {
            _ = try await blockedContext.search(
                FullTextFusionSecuredArticle.self
            )
            .fullText(FullTextFusionSecuredArticle.fields.body)
            .terms([])
            .executeWithScores()
            Issue.record("Empty scored search must enforce LIST authorization")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
        }
        do {
            _ = try await blockedContext.indexQueryContext.batchFetchItems(
                ids: [],
                type: FullTextFusionSecuredArticle.self
            )
            Issue.record("Empty batch fetch must enforce LIST authorization")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
        }
        do {
            _ = try await blockedContext
                .autocomplete(FullTextFusionSecuredArticle.self)
                .field(FullTextFusionSecuredArticle.fields.body)
                .prefix("")
                .execute()
            Issue.record("Autocomplete must enforce LIST authorization")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
        }
        do {
            _ = try await blockedContext
                .autocomplete(FullTextFusionSecuredArticle.self)
                .field(FullTextFusionSecuredArticle.fields.body)
                .popularTerms()
            Issue.record("Popular terms must enforce LIST authorization")
        } catch let error as SecurityError {
            #expect(error.operation == .list)
        }
        for terms in [[], ["absent"]] {
            do {
                _ = try await Search(
                    FullTextFusionSecuredArticle.fields.body,
                    context: blockedContext.indexQueryContext
                )
                .terms(terms)
                .execute(
                    candidates: nil,
                    execution: ReadExecutionContext(
                        monotonicClock: container.monotonicClock
                    )
                )
                Issue.record("Fusion search must enforce LIST authorization")
            } catch let error as SecurityError {
                #expect(error.operation == .list)
            }
        }

        let readerContext = container.testBaseContext(
            authorization: .authenticated(reader)
        )
        do {
            _ = try await Search(
                FullTextFusionSecuredArticle.fields.body,
                context: readerContext.indexQueryContext
            )
            .terms(["needle"])
            .execute(
                candidates: nil,
                execution: ReadExecutionContext(
                    monotonicClock: container.monotonicClock
                )
            )
            Issue.record("Fusion model fetch must enforce GET authorization")
        } catch let error as SecurityError {
            #expect(error.operation == .get)
        }
    }
}
