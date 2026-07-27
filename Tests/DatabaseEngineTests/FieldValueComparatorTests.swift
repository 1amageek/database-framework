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
}
