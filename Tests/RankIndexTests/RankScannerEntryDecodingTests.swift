import DatabaseEngine
import DatabaseKit
import StorageKit
import TestSupport
import Testing
@testable import RankIndex

@Suite("Rank scanner entry decoding")
struct RankScannerEntryDecodingTests {
    @Test("Valid entries preserve composite primary keys")
    func decodesCompositePrimaryKey() throws {
        let subspace = Subspace(prefix: Tuple("rank", "scores").pack())
        let key = subspace.pack(Tuple(Int64(10), "tenant", "entity"))

        let entry = try RankScanner.decodeEntry(
            key: key,
            scoresSubspace: subspace,
            workMeter: makeMeter()
        )

        #expect(entry.scoreElement as? Int64 == 10)
        var primaryKey: Tuple?
        entry.primaryKey.withValue { primaryKey = $0 }
        #expect(primaryKey == Tuple("tenant", "entity"))
    }

    @Test("Entries without a primary key fail explicitly")
    func rejectsMissingPrimaryKey() {
        let subspace = Subspace(prefix: Tuple("rank", "scores").pack())
        let key = subspace.pack(Tuple(Int64(10)))

        #expect(throws: RankScannerError.malformedEntry(elementCount: 1)) {
            try RankScanner.decodeEntry(
                key: key,
                scoresSubspace: subspace,
                workMeter: makeMeter()
            )
        }
    }

    @Test("Keys outside the scores subspace fail explicitly")
    func rejectsOutsideKey() {
        let subspace = Subspace(prefix: Tuple("rank", "scores").pack())
        let outside = Subspace(prefix: Tuple("other").pack())
            .pack(Tuple(Int64(10), "entity"))

        #expect(throws: RankScannerError.keyOutsideScoresSubspace) {
            try RankScanner.decodeEntry(
                key: outside,
                scoresSubspace: subspace,
                workMeter: makeMeter()
            )
        }
    }

    @Test("Bottom entries retain descending leaderboard positions")
    func calculatesBottomPositions() throws {
        let start = try RankScanner.bottomStartPosition(
            totalCount: 10,
            returnedCount: 3
        )

        #expect(start == 9)
        #expect(start - 1 == 8)
        #expect(start - 2 == 7)
    }

    @Test("An inconsistent atomic count fails explicitly")
    func rejectsInconsistentCount() {
        #expect(
            throws: RankScannerError.inconsistentCount(
                totalCount: 1,
                returnedCount: 2
            )
        ) {
            try RankScanner.bottomStartPosition(
                totalCount: 1,
                returnedCount: 2
            )
        }
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(),
            monotonicClock: TestProcessMonotonicClock()
        )
    }
}
