import Core
import Testing
@testable import ScalarIndex

@Suite("Scalar exact range matching")
struct ScalarRangeValueMatcherTests {
    @Test("UInt64 boundaries are not converted through Double")
    func preservesUInt64Precision() throws {
        let boundary = UInt64(9_007_199_254_740_992)
        let value = try ScalarRangeValueMatcher.fieldValue(
            from: boundary + 1,
            fieldName: "sequence"
        )
        let minimum = try ScalarRangeValueMatcher.fieldValue(
            from: boundary + 1,
            fieldName: "sequence"
        )
        let maximum = try ScalarRangeValueMatcher.fieldValue(
            from: boundary + 2,
            fieldName: "sequence"
        )

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
        let roundedDoubleBoundary = FieldValue.double(Double(boundary))

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
            try ScalarRangeValueMatcher.fieldValue(
                from: Double.nan,
                fieldName: "value"
            )
        }
    }
}
