import DatabaseTypes
import Testing
@testable import QueryAST

@Suite("Typed literal builders")
struct TypedLiteralBuilderTests {
    @Test("SQL builder preserves unsigned integer range")
    func sqlBuilderPreservesUnsignedRange() throws {
        let query = try SQLQueryBuilder(from: "events")
            .where("sequence", equals: UInt64.max)
            .buildAST()

        #expect(
            query.filter
                == .equal(
                    .column(ColumnRef(column: "sequence")),
                    .literal(.uint(UInt64.max))
                )
        )
    }

    @Test("SQL IN accepts only one compile-time element type")
    func sqlInConvertsHomogeneousValues() throws {
        let query = try SQLQueryBuilder(from: "events")
            .whereIn("priority", [Int16.min, Int16.max])
            .buildAST()

        #expect(
            query.filter
                == .inList(
                    .column(ColumnRef(column: "priority")),
                    values: [
                        .literal(.int(Int64(Int16.min))),
                        .literal(.int(Int64(Int16.max))),
                    ]
                )
        )
    }

    @Test("SPARQL VALUES preserves nil as UNDEF")
    func sparqlValuesPreservesUndef() throws {
        let query = try SPARQLQueryBuilder()
            .values(["value"], [[Int64(7)], [nil]])
            .buildAST()

        guard case .graphPattern(let pattern) = query.source,
              case .join(_, .values(let variables, let bindings)) = pattern else {
            Issue.record("Expected a SPARQL VALUES graph pattern")
            return
        }
        #expect(variables == ["value"])
        #expect(bindings == [[.int(7)], [nil]])
    }

    @Test("unsupported database values fail before query construction")
    func unsupportedFieldValueFails() {
        #expect(throws: QueryLiteralConversionError.unsupportedFieldValue) {
            try SQLQueryBuilder(from: "events")
                .where("payload", equals: FieldValue.object(FieldObject()))
        }
    }
}
