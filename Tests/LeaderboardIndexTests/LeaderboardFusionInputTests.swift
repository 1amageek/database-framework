import DatabaseKit
import Testing

@testable import LeaderboardIndex

@Persistable
private struct LeaderboardFusionInputItem {
    var id: String
    var score: Int64
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

        #expect(input.scoring == .annotation(
            name: "score",
            order: .higherIsBetter
        ))
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
}
