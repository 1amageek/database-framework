import DatabaseKit
import Testing
@testable import QueryAST

@Suite("S04 conditional and quantified SQL syntax")
struct SQLConditionalSyntaxTests {
    private func expression(_ sql: String) throws -> Expression {
        let query = try SQLParser().parseSelect("SELECT " + sql)
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }), items.count == 1 else {
            throw SQLSerializationError.invalidStructure("Expected one projection")
        }
        return try projectionExpression(items[0])
    }

    private func roundTrip(_ sql: String) throws -> Expression {
        let original = try expression(sql)
        let canonical = try SQLSerializer().serialize(original)
        #expect(try expression(canonical) == original)
        #expect(try SQLSerializer().serialize(expression(canonical)) == canonical)
        return original
    }

    @Test("Searched and simple CASE retain their distinct operand contracts")
    func caseForms() throws {
        let searched = try roundTrip("CASE WHEN flag THEN a WHEN other THEN b ELSE c END")
        #expect(searched == .caseWhen(cases: [CaseWhenPair(condition: .col("flag"), result: .col("a")),
                                           CaseWhenPair(condition: .col("other"), result: .col("b"))], elseResult: .col("c")))
        let simple = try roundTrip("CASE probe($1) WHEN a THEN b WHEN c THEN d END")
        #expect(simple == .caseWhen(operand: .function(FunctionCall(name: "probe", arguments: [.parameter(.position(1))])),
                                   cases: [CaseWhenPair(condition: .col("a"), result: .col("b")),
                                           CaseWhenPair(condition: .col("c"), result: .col("d"))], elseResult: nil))
        _ = try roundTrip("CASE CASE x WHEN a THEN b END WHEN b THEN CASE WHEN flag THEN c ELSE d END ELSE e END")
    }

    @Test("Only unquoted unqualified GREATEST and LEAST use dedicated forms")
    func extremaIdentity() throws {
        #expect(try roundTrip("GrEaTeSt(a, b, NULL)") == .greatest([.col("a"), .col("b"), .null]))
        #expect(try roundTrip("least(a)") == .least([.col("a")]))
        let genericCalls: [(String, String, String?)] = [("\"GrEaTeSt\"(a)", "GrEaTeSt", nil),
                                     ("\"greatest\"(a)", "greatest", nil),
                                     ("custom.greatest(a)", "greatest", "custom"),
                                     ("\"Schema\".\"LEAST\"(a)", "LEAST", "Schema"),
                                     ("\"schema.greatest\"(a)", "schema.greatest", nil)]
        for (sql, name, schema) in genericCalls {
            #expect(try roundTrip(sql) == .function(FunctionCall(name: name, arguments: [.col("a")], schema: schema)))
        }
        #expect(try roundTrip("custom.greatest(DISTINCT a)") == .function(FunctionCall(name: "greatest", arguments: [.col("a")], distinct: true, schema: "custom")))
    }

    @Test("Truth tests preserve all truth values and negation with SQL precedence")
    func truthPredicates() throws {
        for (keyword, value) in [("TRUE", TruthValue.true), ("FALSE", .false), ("UNKNOWN", .unknown)] {
            for negated in [false, true] {
                let suffix = negated ? " IS NOT " : " IS "
                #expect(try roundTrip("a = b" + suffix + keyword) == .truthTest(.equal(.col("a"), .col("b")), value: value, negated: negated))
            }
        }
        #expect(try roundTrip("a IN (b) IS TRUE") == .truthTest(.inList(.col("a"), values: [.col("b")]), value: .true, negated: false))
        #expect(try roundTrip("NOT a IS UNKNOWN AND flag") == .and(.not(.truthTest(.col("a"), value: .unknown, negated: false)), .col("flag")))
        #expect(try roundTrip("a IS NULL") == .isNull(.col("a")))
        #expect(try roundTrip("a IS NOT NULL") == .isNotNull(.col("a")))
    }

    @Test("Distinct predicates preserve null-safe comparison and arithmetic operands")
    func distinctPredicates() throws {
        for negated in [false, true] {
            #expect(try roundTrip("a + b IS " + (negated ? "NOT " : "") + "DISTINCT FROM c") ==
                    .isDistinctFrom(.add(.col("a"), .col("b")), .col("c"), negated: negated))
        }
    }

    @Test("All six comparisons and quantifiers retain parameter and query sources")
    func quantifiedComparisons() throws {
        let comparisons: [(String, DatabaseKit.ComparisonOperator)] = [("=", .equal), ("<>", .notEqual),
            ("<", .lessThan), ("<=", .lessThanOrEqual), (">", .greaterThan), (">=", .greaterThanOrEqual)]
        for (symbol, comparison) in comparisons {
            for (keyword, quantifier) in [("ANY", ComparisonQuantifier.any), ("SOME", .any), ("ALL", .all)] {
                #expect(try roundTrip("x " + symbol + " " + keyword + " ($1)") ==
                        .quantifiedComparison(.col("x"), comparison: comparison, quantifier: quantifier, source: .array(.parameter(.position(1)))))
                let parsed = try roundTrip("x " + symbol + " " + keyword + " (SELECT y FROM records WHERE flag)")
                guard case .quantifiedComparison(_, let op, let q, .subquery(let query)) = parsed else {
                    Issue.record("Expected quantified subquery"); continue
                }
                #expect(op == comparison && q == quantifier)
                #expect(query.projection == [ProjectionItem(.col("y"))])
            }
        }
        #expect(try roundTrip("x != SOME (:values)") == .quantifiedComparison(.col("x"), comparison: .notEqual, quantifier: .any, source: .array(.parameter(.name("values")))))
        _ = try roundTrip("x = ANY (WITH candidates AS (SELECT y FROM records) SELECT y FROM candidates)")
        _ = try roundTrip("x = ANY (array_source($1)) IS NOT FALSE")
    }

    @Test("Existing typed arrays and typed NULL arrays preserve canonical carriage")
    func typedArraySources() throws {
        for sql in ["x = ANY ('{1,NULL,3}'::INTEGER[])", "x <> ALL (CAST('{}' AS INTEGER[]))",
                    "x = ANY (CAST(NULL AS INTEGER[]))"] {
            guard case .quantifiedComparison(_, _, _, .array(.literal(.queryValue(_, let type)))) = try roundTrip(sql) else {
                Issue.record("Expected a typed constant array source"); continue
            }
            #expect(type == .sql(.array(.integer)))
        }
    }

    @Test("Binding owns candidate type and subquery cardinality checks")
    func bindingBoundary() throws {
        _ = try roundTrip("x = ANY (scalar_value)")
        _ = try roundTrip("x = ALL (SELECT a, b FROM records)")
        _ = try roundTrip("CASE scalar_value WHEN other THEN flag ELSE result END")
        for (source, target) in [("2.5::NUMERIC", DataType.integer), ("(-2.5)::NUMERIC", .integer),
                                 ("2.5::REAL", .integer), ("2.5::DOUBLE PRECISION", .bigint),
                                 ("1.23456789::REAL", .decimal(precision: nil, scale: nil)),
                                 ("1.2345678901234567::DOUBLE PRECISION", .decimal(precision: nil, scale: nil))] {
            let operand = try expression(source)
            #expect(try roundTrip("CAST(" + source + " AS " + SQLSerializer().serialize(target) + ")") == .cast(operand, targetType: target))
        }
        _ = try roundTrip("1250::NUMERIC(3,-1)")
        let sql = "SELECT 2.5::NUMERIC::INTEGER"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let cast = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        let operand = try #require(parsed.occurrences.child(of: cast, field: .operand, index: 0))
        _ = try #require(parsed.occurrences.child(of: cast, field: .castType))
        #expect(try sourceText(sql, parsed, cast) == "2.5::NUMERIC::INTEGER")
        #expect(try sourceText(sql, parsed, operand) == "2.5::NUMERIC")
    }

    @Test("Malformed conditional and quantified syntax fails at the offending token")
    func malformedSyntax() throws {
        for (sql, token) in [("CASE x ELSE y END", "ELSE"), ("CASE WHEN x y END", "y"),
                             ("CASE WHEN x THEN END", "END"), ("CASE x WHEN y THEN z", ""),
                             ("greatest()", "greatest"), ("least(DISTINCT x)", "least"),
                             ("greatest(x,)", ")"), ("x IS NOT", ""), ("x IS DISTINCT y", "y"),
                             ("x IS UNKNOWNISH", "UNKNOWNISH"), ("x = ANY ()", ")"),
                             ("x = ALL (SELECT)", ")"), ("x = SOME ($1, $2)", ","),
                             ("x = ANY $1", "$1"), ("x = ANY ($1", "")] {
            let source = "SELECT " + sql
            let expected = token.isEmpty ? source.utf8.count : try #require(source.range(of: token, options: .backwards)).lowerBound.utf8Offset(in: source)
            do { _ = try SQLParser().parseSelectWithSource(source); Issue.record("Accepted malformed SQL: \(source)") }
            catch let error as SQLParser.ParseError {
                switch error {
                case .unexpectedToken(_, _, let position), .unexpectedEndOfInput(_, let position),
                     .invalidSyntax(_, let position), .literalError(_, let position): #expect(position == expected)
                case .unsupportedFeature: Issue.record("Expected positioned syntax failure")
                }
            }
        }
    }

    @Test("Simple CASE source capture owns one operand and independent repeated match occurrences")
    func caseSourceOccurrences() throws {
        let sql = "SELECT CASE probe(missing) WHEN missing THEN missing WHEN missing THEN missing ELSE missing END"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        let conditional = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        let operand = try #require(parsed.occurrences.child(of: conditional, field: .operand, index: 0))
        #expect(parsed.occurrences.child(of: conditional, field: .operand, index: 1) == nil)
        let argument = try #require(parsed.occurrences.child(of: operand, field: .argument, index: 0))
        var repeated = [argument]
        for index in UInt32(0)..<2 {
            repeated.append(try #require(parsed.occurrences.child(of: conditional, field: .caseCondition, index: index)))
            repeated.append(try #require(parsed.occurrences.child(of: conditional, field: .caseResult, index: index)))
        }
        repeated.append(try #require(parsed.occurrences.child(of: conditional, field: .caseElse)))
        let offsets = try repeated.map { try #require(parsed.sourceMap[$0]).range.start }
        #expect(Set(repeated).count == 6)
        #expect(offsets == [18, 32, 45, 58, 71, 84])
        for occurrence in repeated { #expect(try sourceText(sql, parsed, occurrence) == "missing") }
        #expect(try sourceText(sql, parsed, operand) == "probe(missing)")
        #expect(try sourceText(sql, parsed, conditional) == String(sql.dropFirst(7)))
    }

    @Test("New predicate and extrema source edges preserve each decisive child")
    func predicateSourceOccurrences() throws {
        let sql = "SELECT greatest(missing, missing), missing IS NOT TRUE, missing IS DISTINCT FROM missing, missing = SOME ($1), missing > ALL (SELECT missing FROM records)"
        let parsed = try SQLParser().parseSelectWithSource(sql)
        func projection(_ index: UInt32) throws -> QueryOccurrenceID {
            try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: index))
        }
        let extrema = try projection(0)
        let first = try #require(parsed.occurrences.child(of: extrema, field: .argument, index: 0))
        let second = try #require(parsed.occurrences.child(of: extrema, field: .argument, index: 1))
        #expect(first != second)
        #expect(parsed.sourceMap[first]?.range.start == 16)
        #expect(parsed.sourceMap[second]?.range.start == 25)
        let truth = try projection(1)
        let truthOperand = try #require(parsed.occurrences.child(of: truth, field: .operand, index: 0))
        #expect(try sourceText(sql, parsed, truthOperand) == "missing")
        let distinct = try projection(2)
        for index in UInt32(0)..<2 {
            let operand = try #require(parsed.occurrences.child(of: distinct, field: .operand, index: index))
            #expect(try sourceText(sql, parsed, operand) == "missing")
        }
        let array = try projection(3)
        let parameter = try #require(parsed.occurrences.child(of: array, field: .operand, index: 1))
        #expect(try sourceText(sql, parsed, parameter) == "$1")
        #expect(try sourceText(sql, parsed, array) == "missing = SOME ($1)")
        let quantified = try projection(4)
        let query = try #require(parsed.occurrences.child(of: quantified, field: .subquery))
        let candidate = try #require(parsed.occurrences.child(of: query, field: .projection, index: 0))
        #expect(try sourceText(sql, parsed, candidate) == "missing")
        #expect(parsed.occurrences.child(of: quantified, field: .operand, index: 1) == nil)
        #expect(try sourceText(sql, parsed, quantified) == "missing > ALL (SELECT missing FROM records)")
    }

    @Test("Nested quantified CASE source offsets survive UTF-8 and parser reuse")
    func nestedSourceReuse() throws {
        let sql = "-- É\r\nSELECT CASE \"名前\" WHEN x THEN x = ANY (SELECT missing FROM records) ELSE x IS UNKNOWN END"
        let parser = SQLParser()
        let parsed = try parser.parseSelectWithSource(sql)
        let conditional = try #require(parsed.occurrences.child(of: parsed.root, field: .projection, index: 0))
        let result = try #require(parsed.occurrences.child(of: conditional, field: .caseResult, index: 0))
        let query = try #require(parsed.occurrences.child(of: result, field: .subquery))
        let missing = try #require(parsed.occurrences.child(of: query, field: .projection, index: 0))
        let annotation = try #require(parsed.sourceMap[missing])
        let expected = try #require(sql.range(of: "missing")).lowerBound.utf8Offset(in: sql)
        #expect(annotation.range.start == expected)
        #expect(try sourceText(sql, parsed, missing) == "missing")
        #expect(throws: SQLParser.ParseError.self) { _ = try parser.parseSelectWithSource("SELECT x = ALL (") }
        _ = try parser.parseSelectWithSource("SELECT least(x)")
        #expect(parsed.sourceMap[missing] == annotation)
    }

    private func sourceText(_ sql: String, _ parsed: SQLParsedSelect, _ occurrence: QueryOccurrenceID) throws -> String {
        let range = try #require(parsed.sourceMap[occurrence]).range
        return String(decoding: Array(sql.utf8)[range.start..<range.end], as: UTF8.self)
    }

    @Test("CASE arm collections and quantified token input enforce exact bounds")
    func parserBounds() throws {
        // One projection, one CASE arm, and the implicit singleton source row.
        _ = try SQLParser(structuralLimits: QueryStructuralLimits(maximumCollectionElements: 3)).parseSelect("SELECT CASE x WHEN y THEN z END")
        #expect(throws: QueryStructuralValidationError.resourceLimitExceeded(resource: .collectionElements, actual: 3, maximum: 2)) {
            _ = try SQLParser(structuralLimits: QueryStructuralLimits(maximumCollectionElements: 2)).parseSelect("SELECT CASE x WHEN y THEN z END")
        }
        _ = try SQLParser(structuralLimits: QueryStructuralLimits(maximumInputTokens: 7)).parseSelect("SELECT x = ANY ($1)")
        #expect(throws: QueryStructuralValidationError.self) {
            _ = try SQLParser(structuralLimits: QueryStructuralLimits(maximumInputTokens: 6)).parseSelect("SELECT x = ANY ($1)")
        }
    }

    @Test("Canonical emission rejects empty CASE and extrema and enforces byte bounds")
    func serializerBounds() throws {
        for value in [Expression.caseWhen(operand: .col("x"), cases: [], elseResult: nil), .greatest([]), .least([])] {
            #expect(throws: SQLSerializationError.self) { try SQLSerializer().serialize(value) }
        }
        let value = Expression.quantifiedComparison(.col("x"), comparison: .equal, quantifier: .any, source: .array(.parameter(.position(1))))
        let count = UInt64(try SQLSerializer().serialize(value).utf8.count)
        _ = try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: count)).serialize(value)
        #expect(throws: SQLSerializationError.self) {
            try SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: count - 1)).serialize(value)
        }
    }
}

private extension String.Index {
    func utf8Offset(in string: String) -> Int { string[..<self].utf8.count }
}
