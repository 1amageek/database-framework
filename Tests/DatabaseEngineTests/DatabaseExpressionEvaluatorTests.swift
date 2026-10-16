import DatabaseTypes
import DatabaseKit
import StorageKitSystemClock
import Testing
@testable import DatabaseEngine

@Suite("Database expression evaluator")
struct DatabaseExpressionEvaluatorTests {

    @Test("Only literal NULL casts bypass deferred temporal precision conversion")
    func deferredSQLSemanticsFailEvaluation() throws {
        let evaluator = DatabaseExpressionEvaluator(fields: ["missing": .null])
        for expression: Expression in [.array([]), .row([.null])] {
            #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("SQL ARRAY/ROW constructor")) {
                try evaluator.evaluate(expression)
            }
        }
        for expression: Expression in [
            .arrayAccess(.col("unread"), subscripts: [.index(.col("unreadIndex"))]),
            .collectionOperation(.contains, left: .col("unread"), right: .col("unreadRight")),
        ] {
            #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("SQL collection expression requires selected SQL execution")) {
                try evaluator.evaluate(expression)
            }
        }
        #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("SQL VARIADIC requires selected SQL execution")) {
            try evaluator.evaluate(.function(FunctionCall(name: "concat", arguments: [.col("unread")], finalArrayIsVariadic: true)))
        }
        #expect(try evaluator.evaluate(.literal(.array([.int(1)]))) == .array([.int64(1)]))
        #expect(SQLCollectionError.invalidArrayInput(offset: 0, reason: "invalid").description.contains("UTF-8 offset 0"))
        #expect(SQLCollectionError.nullJSONObjectKey(argument: 1).description.contains("argument 1"))
        #expect(SQLCollectionError.nullJSONMutationPathElement(position: 1).description.contains("element 1"))
        for type: DataType in [.time(withTimeZone: false, precision: 0),
                               .timestamp(withTimeZone: true, precision: 6)] {
            for operand: Expression in [.null, .literal(.queryValue(.primitive(.null), type: .sql(.integer)))] {
                #expect(try evaluator.evaluate(.cast(operand, targetType: type)) == .null)
            }
            for operand: Expression in [.string("12:00"), .col("missing")] {
                #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("temporal precision conversion")) {
                    try evaluator.evaluate(.cast(operand, targetType: type))
                }
            }
        }
        for precision in [-1, 7] {
            #expect(throws: DatabaseExpressionEvaluationError.invalidCast("TIME")) {
                try evaluator.evaluate(.cast(.null, targetType: .time(withTimeZone: false, precision: precision)))
            }
        }
    }

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
        ], workMeter: patternMeter())

        #expect(try evaluator.predicate(.patternMatch(subject: .col("title"), pattern: .string("Calendar%"), escape: nil, kind: .like)))
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
            ("e\u{301}", "__", true),
            ("A", "a", false),
            ("a\\b", "a\\b", true),
        ]

        for (value, pattern, expected) in cases {
            let meter = patternMeter()
            let evaluator = DatabaseExpressionEvaluator(fields: [
                "value": .string(value),
            ], workMeter: meter)
            #expect(
                try evaluator.predicate(
                    .patternMatch(subject: .col("value"), pattern: .string(pattern), escape: nil, kind: .like)
                ) == expected
            )
            #expect(meter.retainedIntermediateBytes == 0)
            #expect(meter.retainedIntermediateRows == 0)
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
                .patternMatch(subject: .col("value"), pattern: .string("abc"), escape: nil, kind: .like)
            )
        }
        #expect(workMeter.retainedIntermediateBytes == 0)
        #expect(workMeter.retainedIntermediateRows == 0)
    }

    @Test("Raw patterns preserve NULL, explicit escape, typed failure, and owner release")
    func patternFailureAndNullSemantics() throws {
        let meter = patternMeter()
        let evaluator = DatabaseExpressionEvaluator(fields: [:], workMeter: meter)
        #expect(try evaluator.evaluate(.patternMatch(subject: .null, pattern: .string("["),
                                                    escape: nil, kind: .regularExpression)) == .null)
        #expect(try evaluator.evaluate(.patternMatch(subject: .string("a"), pattern: .null,
                                                    escape: nil, kind: .like)) == .null)
        #expect(try evaluator.evaluate(.patternMatch(subject: .string("a"), pattern: .string("a"),
                                                    escape: .null, kind: .like)) == .null)
        #expect(try evaluator.evaluate(.patternMatch(subject: .string("a_b"), pattern: .string("a!_b"),
                                                    escape: .string("!"), kind: .like)) == .bool(true))
        #expect(try evaluator.evaluate(.patternMatch(subject: .string("A"), pattern: .string("a"),
                                                    escape: nil, kind: .caseInsensitiveLike)) == .bool(true))
        do {
            _ = try evaluator.evaluate(.patternMatch(subject: .string("a"), pattern: .string("["),
                                                     escape: nil, kind: .regularExpression))
            Issue.record("An invalid pattern was accepted")
        } catch let error as DatabaseExpressionEvaluationError {
            guard case .invalidPattern(let offset, let reason) = error else {
                Issue.record("Unexpected pattern error: \(error)"); return
            }
            #expect(offset <= 1)
            #expect(!reason.isEmpty)
        }
        do {
            _ = try evaluator.evaluate(.patternMatch(subject: .string("a"), pattern: .string("(?=a)"),
                                                     escape: nil, kind: .regularExpression))
            Issue.record("An unsupported pattern construct was accepted")
        } catch let error as DatabaseExpressionEvaluationError {
            guard case .unsupportedPattern(let offset, let reason) = error else {
                Issue.record("Unexpected unsupported-pattern error: \(error)"); return
            }
            #expect(offset <= 5)
            #expect(!reason.isEmpty)
        }
        do {
            _ = try evaluator.evaluate(.patternMatch(subject: .string("a"), pattern: .string("a"),
                                                     escape: .string("!!"), kind: .like))
            Issue.record("A multiple-codepoint escape was accepted")
        } catch let error as DatabaseExpressionEvaluationError {
            guard case .invalidFunctionArgument(let function, let reason) = error else {
                Issue.record("Unexpected invalid-escape error: \(error)"); return
            }
            #expect(!function.isEmpty)
            #expect(!reason.isEmpty)
        }
        do {
            _ = try evaluator.evaluate(.patternMatch(subject: .string("a"), pattern: .string("a"),
                                                     escape: .string("!"), kind: .regularExpression))
            Issue.record("An ERE operator accepted ESCAPE")
        } catch let error as DatabaseExpressionEvaluationError {
            guard case .invalidFunctionArgument = error else {
                Issue.record("Unexpected ERE-escape error: \(error)"); return
            }
        }
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(meter.retainedIntermediateRows == 0)
        let noMeter = DatabaseExpressionEvaluator(fields: [:])
        #expect(throws: DatabaseExpressionEvaluationError.unsupportedExpression("Pattern matching requires an operation work meter")) {
            try noMeter.evaluate(.patternMatch(subject: .string("a"), pattern: .string("a"),
                                               escape: nil, kind: .like))
        }
    }

    private func patternMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumRows: 1, maximumWorkUnits: 100_000,
                                                  timeoutMilliseconds: 30_000),
                          monotonicClock: SystemStorageClock())
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
                    FunctionCall(name: "abs", arguments: [.col("small")])
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
    @Test("Primitive query wrappers evaluate while rich operations fail explicitly")
    func queryLiteralEvaluationPreservesPrimitiveBoundary() throws {
        let evaluator = DatabaseExpressionEvaluator(fields: [:])
        #expect(
            try evaluator.evaluate(.add(
                .literal(.queryValue(.primitive(.int64(40)))),
                .literal(.queryValue(.primitive(.int64(2))))
            )) == .int64(42)
        )
        let literal = Literal.queryValue(.numeric(try QueryNumericValue(
            value: ExactDecimal(coefficient: 123, scale: 2), displayScale: 3
        )))
        #expect(
            throws: DatabaseExpressionEvaluationError.unsupportedExpression(
                "Rich query value requires query-scoped evaluation"
            )
        ) {
            _ = try evaluator.evaluate(.add(.literal(literal), .literal(.int(1))))
        }
        #expect(
            throws: DatabaseExpressionEvaluationError.unsupportedExpression(
                "Rich query value requires query-scoped evaluation"
            )
        ) {
            _ = try evaluator.evaluate(.cast(.literal(literal), targetType: .text))
        }
    }

}
