import DatabaseKit
import DatabaseTypes
import Testing
@testable import ScalarIndex

@Suite("Scalar exact range matching")
struct ScalarRangeValueMatcherTests {
    @Test("UInt64 boundaries are not converted through Double")
    func preservesUInt64Precision() throws {
        let boundary = UInt64(9_007_199_254_740_992)
        let value = FieldValue.uint64(boundary + 1)
        let minimum = FieldValue.uint64(boundary + 1)
        let maximum = FieldValue.uint64(boundary + 2)

        #expect(
            try ScalarRangeValueMatcher.matches(
                value,
                minimum: minimum,
                maximum: maximum,
                minimumInclusive: true,
                maximumInclusive: true,
                fieldName: "sequence"
            )
        )
        #expect(value == .uint64(boundary + 1))
    }

    @Test("Mixed numeric bounds use exact ordering")
    func comparesMixedNumericBoundsExactly() throws {
        let boundary = UInt64(9_007_199_254_740_992)
        let value = FieldValue.uint64(boundary + 1)
        let roundedDoubleBoundary = FieldValue.float64(Double(boundary))

        #expect(
            !(try ScalarRangeValueMatcher.matches(
                value,
                minimum: nil,
                maximum: roundedDoubleBoundary,
                minimumInclusive: false,
                maximumInclusive: true,
                fieldName: "sequence"
            ))
        )
    }

    @Test("Incompatible and unordered values fail explicitly")
    func rejectsInvalidComparisons() {
        #expect(
            throws: FilterError.incomparableValues(
                fieldName: "value",
                valueType: "string",
                boundType: "int64"
            )
        ) {
            try ScalarRangeValueMatcher.matches(
                .string("ten"),
                minimum: .int64(10),
                maximum: nil,
                minimumInclusive: true,
                maximumInclusive: false,
                fieldName: "value"
            )
        }
        #expect(
            throws: FilterError.unorderedFloatingPoint(fieldName: "value")
        ) {
            try ScalarRangeValueMatcher.matches(
                .float64(.nan),
                minimum: nil,
                maximum: nil,
                minimumInclusive: true,
                maximumInclusive: true,
                fieldName: "value"
            )
        }
    }
}
