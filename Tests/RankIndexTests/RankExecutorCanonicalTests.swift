@testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@testable import RankIndex

@Persistable(type: "RankExecutorPlayer")
private struct RankExecutorPlayer {
    #Directory<RankExecutorPlayer>("rank-executor-players")
    #Index(
        .rank(
            name: "rank_executor_player_score",
            score: \RankExecutorPlayer.score
        )
    )

    var id: String = ""
    var name: String = ""
    var score: Int64 = 0
}

@Polymorphable(identifier: "RankExecutorDocument")
@PolymorphicDirectory("rank-executor-documents")
@PolymorphicIndex(
    .rank(
        name: "RankExecutorDocument_score",
        score: "score"
    )
)
private protocol RankExecutorDocument:
    Polymorphable<RankExecutorDocumentPolymorphicGroup>
{
    var id: String { get }
    var score: Int64 { get }
    var title: String { get }
}

@Persistable(type: "RankExecutorArticle")
private struct RankExecutorArticle: RankExecutorDocument {
    #Directory<RankExecutorArticle>("rank-executor-articles")

    var id: String = ""
    var score: Int64 = 0
    var title: String = ""
    var body: String = ""
}

@Persistable(type: "RankExecutorReport")
private struct RankExecutorReport: RankExecutorDocument {
    #Directory<RankExecutorReport>("rank-executor-reports")

    var id: String = ""
    var score: Int64 = 0
    var title: String = ""
    var pageCount: Int64 = 0
}

@Persistable(type: "RankAuthorizationPlayer")
private struct RankAuthorizationPlayer: SecurityPolicy {
    #Directory<RankAuthorizationPlayer>("rank-authorization-players")
    #Index(
        .rank(
            name: "rank_authorization_player_score",
            score: \RankAuthorizationPlayer.score
        )
    )

    var id: String = ""
    var score: Int64 = 0

    static func permitsRead(
        of resource: borrowing RankAuthorizationPlayer,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier == "rank-reader"
    }

    static func permitsQuery(
        _ query: borrowing SecurityQuery,
        in context: borrowing AuthorizationContext
    ) -> Bool {
        context.principal?.identifier == "rank-reader"
    }

    static func permitsCreate(
        _ newResource: borrowing RankAuthorizationPlayer,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsUpdate(
        from resource: borrowing RankAuthorizationPlayer,
        to newResource: borrowing RankAuthorizationPlayer,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }

    static func permitsDelete(
        _ resource: borrowing RankAuthorizationPlayer,
        in context: borrowing AuthorizationContext
    ) -> Bool { true }
}

@Suite("Rank canonical executor")
struct RankExecutorCanonicalTests {
    @Test("Regular executor preserves rank order and releases one session meter")
    func regularExecutorUsesSessionMeter() async throws {
        let scenario = try await makeRegularScenario()
        defer { await scenario.container.shutdown() }

        for player in [
            RankExecutorPlayer(id: "low", name: "Low", score: 100),
            RankExecutorPlayer(id: "middle", name: "Middle", score: 500),
            RankExecutorPlayer(id: "high", name: "High", score: 1_000),
        ] {
            try scenario.context.insert(player)
        }
        try await scenario.context.save()

        let query = try scenario.context
            .rank(RankExecutorPlayer.self)
            .by(RankExecutorPlayer.fields.score)
            .top(3)
            .toSelectQuery()
        let meter = makeMeter(for: scenario.container)
        let response = try await execute(
            query,
            context: scenario.context,
            container: scenario.container,
            workMeter: meter
        )

        #expect(response.rows.map { $0.fields["id"]?.stringValue } == [
            "high", "middle", "low",
        ])
        #expect(response.rows.map { $0.annotations["rank"]?.int64Value } == [
            0, 1, 2,
        ])
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Regular executor reports a missing fetched entity and releases its owner")
    func regularExecutorReportsMissingEntity() async throws {
        let scenario = try await makeRegularScenario()
        defer { await scenario.container.shutdown() }
        let present = RankExecutorPlayer(
            id: "present",
            name: "Present",
            score: 200
        )
        let dangling = RankExecutorPlayer(
            id: "dangling",
            name: "Dangling",
            score: 100
        )
        try scenario.context.insert(present)
        try scenario.context.insert(dangling)
        try await scenario.context.save()

        let store = try await scenario.container.testBaseStore(
            for: RankExecutorPlayer.self
        )
        let rowKey = store.itemSubspace
            .subspace(RankExecutorPlayer.persistableType)
            .pack(Tuple(dangling.id))
        try await scenario.container.engine.withTransaction { transaction in
            try transaction.clear(key: rowKey)
        }

        let query = try scenario.context
            .rank(RankExecutorPlayer.self)
            .by(RankExecutorPlayer.fields.score)
            .top(2)
            .toSelectQuery()
        let meter = makeMeter(for: scenario.container)
        await #expect(
            throws: RankReadError.missingFetchedEntity(
                primaryKey: Tuple(dangling.id).pack()
            )
        ) {
            _ = try await execute(
                query,
                context: scenario.context,
                container: scenario.container,
                workMeter: meter
            )
        }
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Denied regular rank reads authorize before storage")
    func authorizationPrecedesRegularExecutorStorage() async throws {
        let storage = ControlledStorageEngine(base: InMemoryEngine())
        let container = try await makeAuthorizationContainer(storage: storage)
        defer { await container.shutdown() }

        #if MultiBase
        try await container.grantTestBaseAccess(
            to: .principal("rank-reader"),
            access: .write
        )
        try await container.grantTestBaseAccess(
            to: .principal("rank-denied"),
            access: .read
        )
        #endif
        let writer = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "rank-reader")
            )
        )
        try writer.insert(RankAuthorizationPlayer(id: "one", score: 1))
        try await writer.save()

        let denied = container.testBaseContext(
            authorization: .authenticated(
                Principal(identifier: "rank-denied")
            )
        )
        let query = try denied.rank(RankAuthorizationPlayer.self)
            .by(RankAuthorizationPlayer.fields.score)
            .top(1)
            .toSelectQuery()
        let meter = makeMeter(for: container)
        let execution = ReadExecutionContext(
            options: .default,
            monotonicClock: container.monotonicClock,
            workMeter: meter
        )
        let readsBefore = storage.control.dataReadOperationCount

        await #expect(throws: SecurityError.self) {
            _ = try await denied.query(query, execution: execution)
        }
        #expect(storage.control.dataReadOperationCount == readsBefore)
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("Polymorphic executor preserves rank order and releases one session meter")
    func polymorphicExecutorUsesSessionMeter() async throws {
        let scenario = try await makePolymorphicScenario()
        defer { await scenario.container.shutdown() }

        try scenario.context.insert(
            RankExecutorArticle(
                id: "article",
                score: 100,
                title: "Article",
                body: "Body"
            )
        )
        try scenario.context.insert(
            RankExecutorReport(
                id: "report",
                score: 300,
                title: "Report",
                pageCount: 4
            )
        )
        try scenario.context.insert(
            RankExecutorArticle(
                id: "article-middle",
                score: 200,
                title: "Article Middle",
                body: "Body"
            )
        )
        try await scenario.context.save()

        let query = SelectQuery(
            projection: .all,
            source: .logical(
                    LogicalSourceRef(
                        kindIdentifier: LogicalSourceKind.polymorphic,
                        identifier: RankExecutorArticle.polymorphableType
                    )
            ),
            accessPath: .index(
                IndexScanSource(
                    indexName: "RankExecutorDocument_score",
                    indexType: .rank,
                    parameters: [
                        RankReadParameter.fieldName: .string("score"),
                        RankReadParameter.mode: .string(RankReadParameter.topMode),
                        RankReadParameter.count: .int64(3),
                    ]
                )
            )
        )
        let meter = makeMeter(for: scenario.container)
        let response = try await execute(
            query,
            context: scenario.context,
            container: scenario.container,
            workMeter: meter
        )

        #expect(response.rows.map { $0.fields["id"]?.stringValue } == [
            "report", "article-middle", "article",
        ])
        #expect(response.rows.map { $0.annotations["rank"]?.int64Value } == [
            0, 1, 2,
        ])
        #expect(meter.retainedIntermediateRows == 0)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    private struct Scenario: Sendable {
        let container: DBContainer
        let context: DatabaseContext
    }

    private func makeRegularScenario() async throws -> Scenario {
        let provider = RankIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(RankExecutorPlayer.self)
        try runtime.register(provider)
        try RankReadExecutors.register(with: &runtime)
        let container = try await DBContainer.open(
            for: try Schema(entities: [RankExecutorPlayer.schemaEntity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "rank-executor-regular-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                entityRuntimes: [runtime.registration()]
            ),
            security: .testingDisabled
        )
        return Scenario(container: container, context: container.testBaseContext())
    }

    private func makePolymorphicScenario() async throws -> Scenario {
        let provider = RankIndexMaintainerProvider()
        var articleRuntime = try EntityRuntimeDefinition(RankExecutorArticle.self)
        try articleRuntime.register(provider)
        try RankReadExecutors.register(with: &articleRuntime)
        var reportRuntime = try EntityRuntimeDefinition(RankExecutorReport.self)
        try reportRuntime.register(provider)
        try RankReadExecutors.register(with: &reportRuntime)
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [
                    try RankExecutorArticle.schemaEntity,
                    try RankExecutorReport.schemaEntity,
                ]
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "rank-executor-polymorphic-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                polymorphicIndexReadExecutors: [
                    RankReadExecutors.polymorphicIndexExecutor,
                ],
                entityRuntimes: [
                    articleRuntime.registration(),
                    reportRuntime.registration(),
                ]
            ),
            security: .testingDisabled
        )
        return Scenario(container: container, context: container.testBaseContext())
    }

    private func makeAuthorizationContainer(
        storage: ControlledStorageEngine<InMemoryEngine>
    ) async throws -> DBContainer {
        let provider = RankIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(RankAuthorizationPlayer.self)
        try runtime.register(provider)
        try RankReadExecutors.register(with: &runtime)
        return try await DBContainer.open(
            for: try Schema(entities: [RankAuthorizationPlayer.schemaEntity]),
            configuration: .testing(storageEngine: storage),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "rank-executor-authorization-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                entityRuntimes: [runtime.registration()],
                authorizationPolicies: [
                    AuthorizationPolicyHandler(RankAuthorizationPlayer.self),
                ]
            ),
            security: .enabled()
        )
    }

    private func execute(
        _ query: SelectQuery,
        context: DatabaseContext,
        container: DBContainer,
        workMeter: DatabaseWorkMeter
    ) async throws -> QueryResponse {
        let execution = ReadExecutionContext(
            options: .default,
            monotonicClock: container.monotonicClock,
            workMeter: workMeter
        )
        return try await context.withReadSnapshot(workMeter: workMeter) {
            snapshot in
            try await context.querySessionBound(
                query,
                execution: execution,
                session: snapshot.session
            )
        }
    }

    private func makeMeter(for container: DBContainer) -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: container.monotonicClock
        )
    }
}
