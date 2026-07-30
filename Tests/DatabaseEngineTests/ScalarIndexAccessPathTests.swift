#if !os(WASI)
#if FOUNDATION_DB
import Testing
import TestHeartbeat
import Foundation
import StorageKit
import FDBStorage
import DatabaseKit
import DatabaseTypes
import TestSupport
@testable import DatabaseEngine
import DatabaseRuntime
@testable import ScalarIndex
@testable import AggregationIndex

@Persistable
private struct ScalarAccessPathEntity {
    #Directory<ScalarAccessPathEntity>(
        "test",
        "scalar_access_path",
        "entities"
    )

    var id: String = UUID().uuidString
    var group: String
    var rank: Int64

    #Index(
        .scalar,
        fields: [\ScalarAccessPathEntity.group],
        name: "scalar_access_path_group"
    )
    #Index(
        .scalar,
        fields: [\ScalarAccessPathEntity.group, \ScalarAccessPathEntity.rank],
        name: "scalar_access_path_group_rank"
    )
}

@Persistable
private struct AggregationOnlyAccessPathEntity {
    #Directory<AggregationOnlyAccessPathEntity>(
        "test",
        "scalar_access_path",
        "aggregation_only"
    )

    var id: String = UUID().uuidString
    var group: String

    #Index(
        .count,
        groupBy: [\AggregationOnlyAccessPathEntity.group],
        name: "scalar_access_path_count_group"
    )
}

@Persistable
private struct CompoundOnlyAccessPathEntity {
    #Directory<CompoundOnlyAccessPathEntity>(
        "test",
        "scalar_access_path",
        "compound_only"
    )

    var id: String = UUID().uuidString
    var group: String
    var rank: Int64

    #Index(
        .scalar,
        fields: [\CompoundOnlyAccessPathEntity.group, \CompoundOnlyAccessPathEntity.rank],
        name: "scalar_access_path_compound_only"
    )
}

@Suite("Scalar index access paths", .foundationDBScenario, .serialized, .heartbeat)
struct ScalarIndexAccessPathTests {
    private func setupContainer() async throws -> DBContainer {
        try await FoundationDBScenarioEnvironment.shared.ensureInitialized()
        let database = try await FoundationDBScenarioCoordinator.shared.makeEngine()
        let schema = try Schema(
            entities: [
                try ScalarAccessPathEntity.schemaEntity,
                try AggregationOnlyAccessPathEntity.schemaEntity,
                try CompoundOnlyAccessPathEntity.schemaEntity,
            ],
            version: Schema.Version(1, 0, 0)
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(backend: .custom(database)),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(entityRuntimes: [try DatabaseFrameworkRuntime.entity(ScalarAccessPathEntity.self), try DatabaseFrameworkRuntime.entity(AggregationOnlyAccessPathEntity.self), try DatabaseFrameworkRuntime.entity(CompoundOnlyAccessPathEntity.self)]),
            security: .disabled
        )
    }

    private func resetStorage(in container: DBContainer) async throws {
        let path = ["test", "scalar_access_path"]
        if try await container.engine.namespaceExists(path: path) {
            try await container.engine.removeNamespace(path: path)
        }
        try await container.ensureIndexesReady()
    }

    @Test("Planning is storage-neutral and preserves descriptor identity")
    func planningPreservesDescriptorIdentity() throws {
        let predicate: DatabaseEngine.Predicate<ScalarAccessPathEntity> =
            (ScalarAccessPathEntity.fields.group == "alpha")
            && (ScalarAccessPathEntity.fields.rank == Int64(2))
        let selection = try ScalarIndexAccessPlanner.select(
            for: predicate,
            descriptors: ScalarAccessPathEntity.indexDescriptors,
            forcedIndexName: nil
        )

        #expect(selection?.descriptor.name == "scalar_access_path_group_rank")
        let clauses = try #require(selection?.clauses)
        #expect(clauses.map { $0.fieldName } == ["group", "rank"])
        #expect(clauses.map { $0.value } == [FieldValue.string("alpha"), FieldValue.int64(2)])
        #expect(selection?.requiresPostFilter == false)
    }

    @Test("Planning rejects a forced non-scalar descriptor")
    func planningRejectsForcedNonScalarDescriptor() {
        let predicate: DatabaseEngine.Predicate<AggregationOnlyAccessPathEntity> =
            AggregationOnlyAccessPathEntity.fields.group == "alpha"

        #expect(throws: CanonicalReadError.self) {
            _ = try ScalarIndexAccessPlanner.select(
                for: predicate,
                descriptors: AggregationOnlyAccessPathEntity.indexDescriptors,
                forcedIndexName: "scalar_access_path_count_group"
            )
        }
    }

    @Test("Compound selection preserves the selected descriptor")
    func compoundSelectionPreservesDescriptor() async throws {
        let container = try await setupContainer()
        try await resetStorage(in: container)
        let context = container.newContext()

        let expected = ScalarAccessPathEntity(group: "alpha", rank: 2)
        try context.insert(ScalarAccessPathEntity(group: "alpha", rank: 1))
        try context.insert(expected)
        try context.insert(ScalarAccessPathEntity(group: "beta", rank: 2))
        try await context.save()

        let results = try await context.fetch(ScalarAccessPathEntity.self)
            .where(ScalarAccessPathEntity.fields.group == "alpha")
            .where(ScalarAccessPathEntity.fields.rank == Int64(2))
            .execute()
        #expect(results.map { $0.id } == [expected.id])

        let count = try await context.fetch(ScalarAccessPathEntity.self)
            .where(ScalarAccessPathEntity.fields.group == "alpha")
            .where(ScalarAccessPathEntity.fields.rank == Int64(2))
            .count()
        #expect(count == 1)
    }

    @Test("Execution plan reports the access path used by model queries")
    func executionPlanReportsSelectedAccessPath() async throws {
        let container = try await setupContainer()
        try await resetStorage(in: container)
        let context = container.newContext()

        let plan = try await context.fetch(ScalarAccessPathEntity.self)
            .where(ScalarAccessPathEntity.fields.group == "alpha")
            .where(ScalarAccessPathEntity.fields.rank == Int64(2))
            .executionPlan()

        guard case .scalarIndex(
            let name,
            let kind,
            let indexedFields
        ) = plan.accessPath else {
            Issue.record("Expected the readable compound scalar index")
            return
        }
        #expect(name == "scalar_access_path_group_rank")
        #expect(kind == "scalar")
        #expect(indexedFields == ["group", "rank"])
        #expect(plan.indexedConditions.map { $0.fieldName } == ["group", "rank"])
        #expect(plan.residualFilterRequired == false)
    }

    @Test("Administrative analysis reports the selected path and actual rows")
    func administrativeAnalysisReportsExecution() async throws {
        let container = try await setupContainer()
        try await resetStorage(in: container)
        let context = container.newContext()
        try context.insert(ScalarAccessPathEntity(group: "alpha", rank: 2))
        try await context.save()

        let query = Query<ScalarAccessPathEntity>()
            .where(ScalarAccessPathEntity.fields.group == "alpha")
            .where(ScalarAccessPathEntity.fields.rank == Int64(2))
        let admin = container.newAdminContext()
        let plan = try await admin.explain(query)

        #expect(plan.kind == .indexScan)
        #expect(plan.selectedIndexName == "scalar_access_path_group_rank")
        #expect(plan.indexConditions.count == 2)
        #expect(plan.filterConditions.isEmpty)
        #expect(plan.requiresSort == false)

        let statistics = try await admin.explainAnalyze(query)
        #expect(statistics.plan == plan)
        #expect(statistics.actualRowCount == 1)
        #expect(statistics.readVersion > 0)
    }

    @Test("A partial compound prefix preserves entity identity")
    func partialCompoundPrefixPreservesEntityIdentity() async throws {
        let container = try await setupContainer()
        try await resetStorage(in: container)
        let context = container.newContext()

        let first = CompoundOnlyAccessPathEntity(group: "alpha", rank: 1)
        let second = CompoundOnlyAccessPathEntity(group: "alpha", rank: 2)
        try context.insert(first)
        try context.insert(second)
        try context.insert(
            CompoundOnlyAccessPathEntity(group: "beta", rank: 1)
        )
        try await context.save()

        let results = try await context.fetch(CompoundOnlyAccessPathEntity.self)
            .where(CompoundOnlyAccessPathEntity.fields.group == "alpha")
            .execute()
        #expect(Set(results.map { $0.id }) == Set([first.id, second.id]))

        let count = try await context.fetch(CompoundOnlyAccessPathEntity.self)
            .where(CompoundOnlyAccessPathEntity.fields.group == "alpha")
            .count()
        #expect(count == 2)
    }

    @Test("Typed queries never interpret aggregation storage as scalar storage")
    func aggregationIndexIsNotScalarAccessPath() async throws {
        let container = try await setupContainer()
        try await resetStorage(in: container)
        let context = container.newContext()

        let expected = AggregationOnlyAccessPathEntity(group: "alpha")
        try context.insert(expected)
        try context.insert(AggregationOnlyAccessPathEntity(group: "beta"))
        try await context.save()

        let fallbackResults = try await context.fetch(
            AggregationOnlyAccessPathEntity.self
        )
        .where(AggregationOnlyAccessPathEntity.fields.group == "alpha")
        .execute()
        #expect(fallbackResults.map { $0.id } == [expected.id])

        var forcedQuery = Query<AggregationOnlyAccessPathEntity>()
            .where(AggregationOnlyAccessPathEntity.fields.group == "alpha")
        forcedQuery.forcedIndex = IndexHint(
            indexName: "scalar_access_path_count_group"
        )

        do {
            _ = try await QueryExecutor<AggregationOnlyAccessPathEntity>(
                context: context,
                query: forcedQuery
            ).execute()
            Issue.record("A non-scalar forced index must fail")
        } catch CanonicalReadError.unsupportedAccessPath {
            // Expected.
        } catch {
            Issue.record("Unexpected forced-index error: \(error)")
        }
    }
}
#endif
#endif
