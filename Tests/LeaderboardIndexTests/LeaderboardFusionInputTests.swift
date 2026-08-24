import DatabaseKit
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@testable import DatabaseEngine
@testable import LeaderboardIndex

@Persistable
private struct LeaderboardFusionInputItem {
    var id: String
    var score: Int64
}

@Persistable
private struct LeaderboardFusionExecutionItem {
    #Index(
        .leaderboard(
            name: "leaderboard_fusion_region_score",
            groupBy: [
                .ascending(\LeaderboardFusionExecutionItem.region),
            ],
            score: \LeaderboardFusionExecutionItem.score,
            window: .daily,
            windowCount: 2
        )
    )

    var id: String
    var region: String
    var score: Int64
    var eligible: Bool
}

@Suite("Leaderboard Fusion input")
struct LeaderboardFusionInputTests {
    @Test("Leaderboard lowers ranking and grouping to canonical QueryIR")
    func lowersToCanonicalInput() {
        let input = Leaderboard(LeaderboardFusionInputItem.fields.score)
            .index(named: "weekly_score")
            .group("team-a")
            .window(42)
            .top(5)
            .fusionInput

        #expect(input.scoring == .position)
        #expect(input.limit == 5)
        guard case .index(let source) = input.operation else {
            Issue.record("Leaderboard must lower to an index operation")
            return
        }
        #expect(source.selection == .named(
            name: "weekly_score",
            type: .leaderboard
        ))
        #expect(source.referencedFields == [
            LeaderboardFusionInputItem.fields.score.identity,
        ])
        #expect(source.parameters[
            LeaderboardFusionReadParameter.scoreField
        ] == .string("score"))
        #expect(source.parameters[
            LeaderboardFusionReadParameter.grouping
        ] == .array([.string("team-a")]))
        #expect(source.parameters[
            LeaderboardFusionReadParameter.windowID
        ] == .int64(42))
    }

    @Test("Leaderboard validates its physical contract")
    func validatesPhysicalContract() throws {
        let descriptor = try #require(
            LeaderboardFusionExecutionItem.schemaEntity.indexes.first
        )
        let input = Leaderboard(
            LeaderboardFusionExecutionItem.fields.score
        ).group("apac").fusionInput
        guard case .index(let source) = input.operation else {
            Issue.record("Leaderboard must lower to an index operation")
            return
        }
        let executor = LeaderboardFusionIndexReadExecutor()
        try executor.validate(
            FusionIndexValidationRequest(
                source: source,
                scoring: .position,
                descriptor: descriptor
            )
        )

        var invalidGrouping = source.parameters
        invalidGrouping[LeaderboardFusionReadParameter.grouping] = .array([])
        #expect {
            try executor.validate(
                FusionIndexValidationRequest(
                    source: FusionIndexSource(
                        selection: source.selection,
                        referencedFields: source.referencedFields,
                        parameters: invalidGrouping
                    ),
                    scoring: .position,
                    descriptor: descriptor
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .invalidIndexInput(
                indexType: .leaderboard,
                parameter: LeaderboardFusionReadParameter.grouping
            )
        }

        #expect {
            try executor.validate(
                FusionIndexValidationRequest(
                    source: source,
                    scoring: .annotation(
                        name: "score",
                        order: .higherIsBetter
                    ),
                    descriptor: descriptor
                )
            )
        } throws: { error in
            error as? FusionExecutionError == .invalidIndexInput(
                indexType: .leaderboard,
                parameter: "scoring"
            )
        }
    }

    @Test("Leaderboard executes grouped and global Int64 ordering")
    func executesGroupedAndGlobalOrdering() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for item in [
            LeaderboardFusionExecutionItem(
                id: "absolute-high",
                region: "apac",
                score: .max,
                eligible: false
            ),
            LeaderboardFusionExecutionItem(
                id: "global-second",
                region: "emea",
                score: .max - 1,
                eligible: true
            ),
            LeaderboardFusionExecutionItem(
                id: "apac-low",
                region: "apac",
                score: 50,
                eligible: true
            ),
        ] {
            try context.insert(item)
        }
        try await context.save()

        let grouped = try await context.execute(
            FusionQuery<LeaderboardFusionExecutionItem> {
                Leaderboard(LeaderboardFusionExecutionItem.fields.score)
                    .group("apac")
                    .top(3)
            }
        )
        #expect(grouped.results.map(\.item.id) == [
            "absolute-high",
            "apac-low",
        ])

        let global = try await context.execute(
            FusionQuery<LeaderboardFusionExecutionItem> {
                Leaderboard(LeaderboardFusionExecutionItem.fields.score)
                    .top(2)
            }
        )
        #expect(global.results.map(\.item.id) == [
            "absolute-high",
            "global-second",
        ])

        let historical = try await context.execute(
            FusionQuery<LeaderboardFusionExecutionItem> {
                Leaderboard(LeaderboardFusionExecutionItem.fields.score)
                    .window(0)
                    .top(2)
            }
        )
        #expect(historical.results.map(\.item.id) == [
            "absolute-high",
            "global-second",
        ])

        let absentWindow = try await context.execute(
            FusionQuery<LeaderboardFusionExecutionItem> {
                Leaderboard(LeaderboardFusionExecutionItem.fields.score)
                    .window(1)
                    .top(2)
            }
        )
        #expect(absentWindow.results.isEmpty)
    }

    @Test("Leaderboard restricts before selecting exact top K")
    func restrictsBeforeTopK() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        for item in [
            LeaderboardFusionExecutionItem(
                id: "excluded-high",
                region: "apac",
                score: 1_000,
                eligible: false
            ),
            LeaderboardFusionExecutionItem(
                id: "eligible-second",
                region: "emea",
                score: 900,
                eligible: true
            ),
            LeaderboardFusionExecutionItem(
                id: "eligible-third",
                region: "apac",
                score: 800,
                eligible: true
            ),
        ] {
            try context.insert(item)
        }
        try await context.save()

        let eligible = try Filter(
            LeaderboardFusionExecutionItem.fields.eligible,
            equals: true
        )
        let result = try await context.execute(
            FusionQuery<LeaderboardFusionExecutionItem> {
                eligible
                Leaderboard(LeaderboardFusionExecutionItem.fields.score)
                    .top(2)
            }
        )
        #expect(result.results.map(\.item.id) == [
            "eligible-second",
            "eligible-third",
        ])
    }

    @Test("Malformed leaderboard positions fail as corruption")
    func malformedPositionFailsAsCorruption() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let item = LeaderboardFusionExecutionItem(
            id: "corrupted",
            region: "apac",
            score: 100,
            eligible: true
        )
        try context.insert(item)
        try await context.save()

        try await container.withTestBaseTransaction { transaction in
            let readable = try #require(
                try await context.indexQueryContext.readableIndex(
                    named: "leaderboard_fusion_region_score",
                    indexType: .leaderboard,
                    for: LeaderboardFusionExecutionItem.self,
                    transaction: transaction
                )
            )
            let primaryKey = try item.persistableIdentifierTuple().pack()
            try transaction.setValue(
                ByteString([0x01]),
                for: readable.subspace.subspace("pos").prefix
                    .appending(contentsOf: primaryKey)
            )
        }

        let eligible = try Filter(
            LeaderboardFusionExecutionItem.fields.eligible,
            equals: true
        )
        await #expect {
            _ = try await context.execute(
                FusionQuery<LeaderboardFusionExecutionItem> {
                    eligible
                    Leaderboard(
                        LeaderboardFusionExecutionItem.fields.score
                    ).top(1)
                }
            )
        } throws: { error in
            error as? FusionExecutionError == .corruptedIndex(.leaderboard)
        }
    }

    private func makeContainer() async throws -> DBContainer {
        let entity = try LeaderboardFusionExecutionItem.schemaEntity
        let provider = TimeWindowLeaderboardIndexMaintainerProvider()
        var entityRuntime = try EntityRuntimeDefinition(
            LeaderboardFusionExecutionItem.self
        )
        try entityRuntime.register(provider)
        return try await DBContainer.open(
            testing: try Schema(entities: [entity]),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "leaderboard-fusion-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider),
                ],
                fusionIndexReadExecutors: [
                    LeaderboardFusionIndexReadExecutor(),
                ],
                entityRuntimes: [entityRuntime.registration()]
            ),
            security: .testingDisabled
        )
    }
}
