import DatabaseTypes
import DatabaseKit
import StorageKitSystemClock
import Testing
@testable import DatabaseEngine

@Suite("Database expression evaluator")
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

    @Test("LIKE preserves wildcard and Unicode character semantics")
    func likeWildcardSemantics() throws {
        let cases: [(String, String, Bool)] = [
            ("", "", true),
            ("", "%", true),
            ("", "_", false),
            ("A📅BC", "A_B%", true),
            ("A📅BC", "%📅%", true),
            ("A📅BC", "A_C", false),
            ("aaab", "%aab", true),
            ("mississippi", "%iss%ppi", true),
            ("abefcdgiescdfimde", "ab%cd_i%de", true),
            ("abc", "%%", true),
            ("abc", "a_d", false),
        ]

        for (value, pattern, expected) in cases {
            let evaluator = DatabaseExpressionEvaluator(fields: [
                "value": .string(value),
            ])
            #expect(
                try evaluator.predicate(
                    .like(.col("value"), pattern: pattern)
                ) == expected
            )
        }
    }

    @Test("LIKE stops at the exact expression work limit")
    func likeHonorsWorkLimit() {
        let workMeter = DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 1,
                maximumWorkUnits: 1,
                timeoutMilliseconds: 30_000
            ),
            monotonicClock: SystemStorageClock()
        )
        let evaluator = DatabaseExpressionEvaluator(
            fields: ["value": .string("abc")],
            workMeter: workMeter
        )

        #expect(
            throws: DatabaseWorkLimitError.maximumWorkUnits(
                stage: .expressionEvaluation,
                consumed: 1,
                requested: 1,
                maximum: 1
            )
        ) {
            _ = try evaluator.predicate(
                .like(.col("value"), pattern: "abc")
            )
        }
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

    @Test("Canonical arithmetic accepts every stored numeric width")
    func storedNumericWidths() throws {
        let evaluator = DatabaseExpressionEvaluator(fields: [
            "small": .int16(7),
            "unsigned": .uint32(4),
            "floating": .float32(0.5),
        ])

        #expect(
            try evaluator.evaluate(.add(.col("small"), .int(2)))
                == .int64(9)
        )
        #expect(
            try evaluator.evaluate(.multiply(.col("unsigned"), .literal(.uint(3))))
                == .uint64(12)
        )
        #expect(
            try evaluator.evaluate(.add(.col("floating"), .int(2)))
                == .float64(2.5)
        )
        #expect(
            try evaluator.evaluate(
                .subtract(.col("small"), .col("unsigned"))
            ) == .decimal(ExactDecimal(coefficient: 3, scale: 0))
        )
        #expect(
            try evaluator.evaluate(
                .greaterThan(.col("floating"), .literal(.double(0.25)))
            ) == .bool(true)
        )
        #expect(
            try evaluator.evaluate(
                .function(
                    FunctionCall(name: "ABS", arguments: [.col("small")])
                )
            ) == .int64(7)
        )
        #expect(
            try evaluator.evaluate(
                .cast(.col("unsigned"), targetType: .bigint)
            ) == .int64(4)
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
