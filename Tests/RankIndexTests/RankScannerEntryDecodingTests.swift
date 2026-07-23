import StorageKit
import Testing
@testable import RankIndex

@Suite("Rank scanner entry decoding")
struct RankScannerEntryDecodingTests {
    @Test("Valid entries preserve composite primary keys")
    func decodesCompositePrimaryKey() throws {
        let subspace = Subspace(prefix: Tuple("rank", "scores").pack())
        let key = subspace.pack(Tuple(Int64(10), "tenant", "record"))

        let entry = try RankScanner.decodeEntry(
            key: key,
            scoresSubspace: subspace
        )

        #expect(entry.scoreElement as? Int64 == 10)
        #expect(entry.primaryKey == Tuple("tenant", "record"))
    }

    @Test("Entries without a primary key fail explicitly")
    func rejectsMissingPrimaryKey() {
        let subspace = Subspace(prefix: Tuple("rank", "scores").pack())
        let key = subspace.pack(Tuple(Int64(10)))

        #expect(throws: RankScannerError.malformedEntry(elementCount: 1)) {
            try RankScanner.decodeEntry(key: key, scoresSubspace: subspace)
        }
    }

    @Test("Keys outside the scores subspace fail explicitly")
    func rejectsOutsideKey() {
        let subspace = Subspace(prefix: Tuple("rank", "scores").pack())
        let outside = Subspace(prefix: Tuple("other").pack())
            .pack(Tuple(Int64(10), "record"))

        #expect(throws: RankScannerError.keyOutsideScoresSubspace) {
            try RankScanner.decodeEntry(key: outside, scoresSubspace: subspace)
        }
    }
}
