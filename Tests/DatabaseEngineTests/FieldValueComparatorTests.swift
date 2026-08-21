import DatabaseKit
import DatabaseTypes
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Testing
@testable import DatabaseEngine

@Suite("FieldValue query comparison")
struct FieldValueComparatorTests {
    @Test("decimal and floating-point values share query numeric semantics")
    func decimalAndDoubleComparison() throws {
        let decimal = FieldValue.decimal(
            ExactDecimal(coefficient: 15, scale: 1)
        )

        #expect(
            try FieldValueComparator.compare(decimal, .float64(1.5))
                == .equal
        )
        #expect(
            try FieldValueComparator.compare(decimal, .float64(1.6))
                == .lessThan
        )
        #expect(
            try FieldValueComparator.compare(.float64(1.4), decimal)
                == .lessThan
        )
        #expect(try FieldValueComparator.equal(decimal, .float64(1.5)))

        let normalizedLargeFloat: Double = 95_367_431_640_625 * 0x1p100
        let normalizedLargeDecimal = FieldValue.decimal(
            ExactDecimal(
                coefficient: Int128(1) << 80,
                scale: -20
            )
        )
        #expect(
            try FieldValueComparator.equal(
                normalizedLargeDecimal,
                .float64(normalizedLargeFloat)
            )
        )

        let lowerRoundedToPointOne = FieldValue.decimal(
            ExactDecimal(
                coefficient: 1_000_000_000_000_000_055,
                scale: 19
            )
        )
        let upperRoundedToPointOne = FieldValue.decimal(
            ExactDecimal(
                coefficient: 1_000_000_000_000_000_056,
                scale: 19
            )
        )
        #expect(
            try FieldValueComparator.compare(
                lowerRoundedToPointOne,
                .float64(0.1)
            ) == .lessThan
        )
        #expect(
            try FieldValueComparator.compare(
                .float64(0.1),
                upperRoundedToPointOne
            ) == .lessThan
        )
        #expect(
            try FieldValueComparator.equal(
                .decimal(ExactDecimal(coefficient: 1, scale: 1)),
                .float64(0.1)
            ) == false
        )
    }

    @Test("unordered numeric values remain typed failures")
    func unorderedNumericValuesFail() {
        #expect(throws: FieldValueComparisonError.unorderedFloatingPoint) {
            _ = try FieldValueComparator.compare(
                .decimal(ExactDecimal(coefficient: 1, scale: 0)),
                .float64(.nan)
            )
        }
    }

    @Test("composite equality and byte ordering use relational identity")
    func compositeEqualityUsesRelationalIdentity() throws {
        let decimal = FieldValue.array([
            .decimal(ExactDecimal(coefficient: 15, scale: 1)),
        ])
        let floatingPoint = FieldValue.array([.float64(1.5)])

        #expect(try FieldValueComparator.equal(decimal, floatingPoint))
        #expect(
            try FieldValueComparator.compare(decimal, floatingPoint) == .equal
        )

        let lower = FieldValue.decimal(
            ExactDecimal(
                coefficient: 1_000_000_000_000_000_055,
                scale: 19
            )
        )
        let binary = FieldValue.float64(0.1)
        #expect(
            try FieldValueComparator.compare(
                .array([lower]),
                .array([binary])
            ) == .lessThan
        )
        #expect(
            try FieldValueComparator.compare(
                .array([binary]),
                .array([lower])
            ) == .greaterThan
        )

        let lowerObject = FieldValue.object(
            try FieldObject([(key: "value", value: lower)])
        )
        let binaryObject = FieldValue.object(
            try FieldObject([(key: "value", value: binary)])
        )
        #expect(
            try FieldValueComparator.compare(lowerObject, binaryObject)
                == .lessThan
        )
        #expect(
            try FieldValueComparator.compare(binaryObject, lowerObject)
                == .greaterThan
        )
        #expect(
            throws: FieldValueComparisonError.unorderedFloatingPoint
        ) {
            _ = try FieldValueComparator.equal(
                .array([.float64(.infinity)]),
                .array([.float64(.infinity)])
            )
        }

        let emptyBytes = FieldValue.bytes(ByteString())
        let prefixBytes = FieldValue.bytes(ByteString([0x00, 0x7F]))
        let extendedBytes = FieldValue.bytes(ByteString([0x00, 0x7F, 0x00]))
        let higherBytes = FieldValue.bytes(ByteString([0x00, 0xFF]))
        #expect(try FieldValueComparator.compare(emptyBytes, prefixBytes) == .lessThan)
        #expect(try FieldValueComparator.compare(prefixBytes, extendedBytes) == .lessThan)
        #expect(try FieldValueComparator.compare(higherBytes, prefixBytes) == .greaterThan)
        #expect(try FieldValueComparator.compare(prefixBytes, prefixBytes) == .equal)
        #expect(
            try FieldValueComparator.compare(
                .array([prefixBytes]),
                .array([higherBytes])
            ) == .lessThan
        )
    }

    @Test("Mixed numeric and nonnumeric equality fails explicitly")
    func mixedNumericEqualityIsIncomparable() {
        #expect(throws: FieldValueComparisonError.self) {
            _ = try FieldValueComparator.equal(.int64(1), .string("1"))
        }
    }

    @Test("Composite ordering stops after the first unequal element")
    func compositeOrderingShortCircuitsTrailingUnorderedValues() throws {
        let comparison = try FieldValueComparator.compare(
            .array([.int64(0), .float64(.nan)]),
            .array([.int64(1), .int64(0)])
        )

        #expect(comparison == .lessThan)
        #expect(
            try FieldValueComparator.equal(
                .array([.int64(0), .float64(.nan)]),
                .array([.int64(1), .int64(0)])
            ) == false
        )
    }

    @Test("sort comparison applies explicit and default NULL placement once")
    func nullSortPlacement() throws {
        let expression = Expression.column(ColumnRef(column: "value"))

        #expect(
            try FieldValueComparator.compare(
                .null,
                .int64(1),
                using: SortKey(
                    expression,
                    direction: .descending,
                    nulls: .last
                )
            ) == .greaterThan
        )
        #expect(
            try FieldValueComparator.compare(
                .null,
                .int64(1),
                using: SortKey(
                    expression,
                    direction: .descending,
                    nulls: .first
                )
            ) == .lessThan
        )
        #expect(
            try FieldValueComparator.compare(
                .null,
                .int64(1),
                using: SortKey(expression, direction: .ascending)
            ) == .lessThan
        )
        #expect(
            try FieldValueComparator.compare(
                .null,
                .int64(1),
                using: SortKey(expression, direction: .descending)
            ) == .greaterThan
        )
    }
}
