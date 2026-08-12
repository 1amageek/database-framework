import DatabaseTypes
import DatabaseKit
import Testing
@testable import DatabaseOperations

@Suite("Database mutation expression evaluator")
struct DatabaseExpressionEvaluatorTests {
    @Test("arithmetic and SQL null logic are deterministic")
    func arithmeticAndNullLogic() throws {
        let evaluator = DatabaseExpressionEvaluator(fields: [
            "priority": .int64(4),
            "missing": .null,
        ])

        #expect(try evaluator.evaluate(.add(.col("priority"), .int(3))) == .int64(7))
        #expect(
            try evaluator.evaluate(
                .and(.equal(.col("priority"), .int(4)), .isNull(.col("missing")))
            ) == .bool(true)
        )
        #expect(try evaluator.evaluate(.equal(.col("missing"), .null)) == .null)
    }

    @Test("LIKE and UUID casts preserve canonical semantics")
    func likeAndUUIDCast() throws {
        let uuid = DatabaseTypes.UUID(
            high: 0x0011_2233_4455_6677,
            low: 0x8899_AABB_CCDD_EEFF
        )
        let evaluator = DatabaseExpressionEvaluator(fields: [
            "title": .string("Calendar Runtime"),
        ])

        #expect(try evaluator.predicate(.like(.col("title"), pattern: "Calendar%")))
        #expect(
            try evaluator.evaluate(
                .cast(.string(uuid.description), targetType: .uuid)
            ) == .uuid(uuid)
        )
    }

    @Test("decimal arithmetic remains exact and normalized")
    func decimalArithmeticRemainsExact() throws {
        let evaluator = DatabaseExpressionEvaluator(fields: [:])

        #expect(
            try evaluator.evaluate(
                .add(
                    .literal(
                        .decimal(ExactDecimal(coefficient: 123, scale: 2))
                    ),
                    .literal(
                        .decimal(ExactDecimal(coefficient: 77, scale: 2))
                    )
                )
            ) == .decimal(ExactDecimal(coefficient: 2, scale: 0))
        )
        #expect(
            try evaluator.evaluate(
                .divide(
                    .literal(
                        .decimal(ExactDecimal(coefficient: 1, scale: 0))
                    ),
                    .literal(
                        .decimal(ExactDecimal(coefficient: 2, scale: 0))
                    )
                )
            ) == .decimal(ExactDecimal(coefficient: 5, scale: 1))
        )
        #expect(
            try evaluator.evaluate(
                .greaterThan(
                    .literal(
                        .decimal(ExactDecimal(coefficient: 1, scale: -20))
                    ),
                    .literal(.uint(UInt64.max))
                )
            ) == .bool(true)
        )
    }

    @Test("inexact decimal division is rejected")
    func inexactDecimalDivisionIsRejected() {
        let evaluator = DatabaseExpressionEvaluator(fields: [:])

        #expect(throws: DatabaseExpressionEvaluationError.inexactDecimalResult) {
            _ = try evaluator.evaluate(
                .divide(
                    .literal(
                        .decimal(ExactDecimal(coefficient: 1, scale: 0))
                    ),
                    .literal(
                        .decimal(ExactDecimal(coefficient: 3, scale: 0))
                    )
                )
            )
        }
    }

    @Test("binary ordering borrows each large operand once")
    func binaryOrderingBorrowsEachOperandOnce() throws {
        var left = [UInt8](repeating: 0x41, count: 16_384)
        var right = left
        left[left.count - 1] = 0x40
        right[right.count - 1] = 0x42
        let leftOwner = BorrowCountingByteStringOwner(left)
        let rightOwner = BorrowCountingByteStringOwner(right)
        let evaluator = DatabaseExpressionEvaluator(fields: [:])

        let result = try evaluator.evaluate(
            .lessThan(
                .literal(.binary(ByteString(retaining: leftOwner))),
                .literal(.binary(ByteString(retaining: rightOwner)))
            )
        )

        #expect(result == .bool(true))
        #expect(leftOwner.borrowCount == 1)
        #expect(rightOwner.borrowCount == 1)
    }
}
