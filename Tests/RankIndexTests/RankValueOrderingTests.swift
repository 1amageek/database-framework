import DatabaseKit
import DatabaseTypes
import Testing
@testable import RankIndex

@Suite("Rank exact value ordering")
struct RankValueOrderingTests {
    @Test("UInt64 values above Double exact range remain distinct")
    func preservesUInt64Precision() throws {
        let lower = UInt64(9_007_199_254_740_992)
        let higher = lower + 1
        let entries = [
            try entry(item: "lower", value: .uint64(lower), identifier: "a"),
            try entry(item: "higher", value: .uint64(higher), identifier: "b")
        ]

        let sorted = try RankValueOrdering.sorted(entries, direction: .descending)

        #expect(sorted.map(\.item) == ["higher", "lower"])
    }

    @Test("Mixed integer and floating values use exact comparison")
    func comparesMixedNumericValuesExactly() throws {
        let boundary = UInt64(9_007_199_254_740_992)
        let entries = [
            try entry(
                item: "unsigned",
                value: .uint64(boundary + 1),
                identifier: "u"
            ),
            try entry(
                item: "double",
                value: .float64(Double(boundary)),
                identifier: "d"
            ),
            try entry(item: "signed", value: .int64(-1), identifier: "s")
        ]

        let sorted = try RankValueOrdering.sorted(entries, direction: .descending)

        #expect(sorted.map(\.item) == ["unsigned", "double", "signed"])
    }

    @Test("Tie order matches rank index byte order")
    func usesDeterministicIdentifierTieBreak() throws {
        let entries = [
            try entry(item: "a", value: .int64(10), identifier: "a"),
            try entry(item: "b", value: .int64(10), identifier: "b")
        ]

        let descending = try RankValueOrdering.sorted(entries, direction: .descending)
        let ascending = try RankValueOrdering.sorted(entries, direction: .ascending)

        #expect(descending.map(\.item) == ["b", "a"])
        #expect(ascending.map(\.item) == ["a", "b"])
    }

    @Test("Missing, non-numeric, and NaN scores fail explicitly")
    func rejectsInvalidScores() {
        #expect(throws: RankValueError.missingField("score")) {
            try RankValueOrdering.numericValue(from: nil, fieldName: "score")
        }
        #expect(
            throws: RankValueError.nonNumericField(
                fieldName: "score",
                actualType: String(
                    reflecting: FieldValue.string("not-a-number")
                )
            )
        ) {
            try RankValueOrdering.numericValue(
                from: .string("not-a-number"),
                fieldName: "score"
            )
        }
        #expect(
            throws: RankValueError.unorderedFloatingPoint(fieldName: "score")
        ) {
            try RankValueOrdering.numericValue(
                from: .float64(.nan),
                fieldName: "score"
            )
        }
    }

    @Test("Duplicate identifiers fail instead of producing unstable ties")
    func rejectsDuplicateIdentifiers() throws {
        let entries = [
            try entry(item: "first", value: .int64(1), identifier: "same"),
            try entry(item: "second", value: .int64(2), identifier: "same")
        ]

        #expect(throws: RankValueError.duplicateIdentifier) {
            try RankValueOrdering.sorted(entries, direction: .descending)
        }
    }

    private func entry(
        item: String,
        value: FieldValue,
        identifier: String
    ) throws -> RankValueEntry<String> {
        RankValueEntry(
            item: item,
            value: value,
            identifierKey: try RankValueOrdering.identifierKey(for: identifier)
        )
    }
}
