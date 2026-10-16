import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("Query value SQL serialization", .heartbeat)
struct QueryValueSQLSerializationTests {
    private let limits = QueryValueLimits(
        maximumRetainedBytes: 4 * 1024 * 1024,
        maximumTotalNodes: 10_000,
        maximumNestingDepth: 64,
        maximumCollectionElements: 10_000
    )

    private func richValues() throws -> [QueryValue] {
        let json = try QueryJSONValue(bytes: ByteString(utf8: "{\"x\":1}"), limits: limits)
        return [
            .numeric(try QueryNumericValue(value: ExactDecimal(coefficient: 1, scale: 0), displayScale: 2)),
            .json(json),
            .jsonb(try QueryJSONBValue(json: json, limits: limits)),
            .array(try QueryArrayValue(
                elementType: .bigint,
                dimensions: [try QueryArrayDimension(lowerBound: -2, length: 2)],
                elements: [.primitive(.int64(7)), .primitive(.null)],
                limits: limits
            )),
            .interval(try QueryIntervalValue(
                period: CalendarPeriod(months: -2, days: 3),
                time: TimeSpan(seconds: -4, nanoseconds: 0)
            )),
        ]
    }

    private func projection(_ text: String) throws -> Expression {
        let query = try SQLParser().parseSelect("SELECT " + text)
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }), items.count == 1 else {
            throw SQLSerializationError.invalidStructure("Expected one projection item")
        }
        return try projectionExpression(items[0])
    }

    @Test("Direct, array-nested and query-nested rich SQL retain exact values")
    func richSQLGenerationRoundTrips() throws {
        let serializer = SQLSerializer()
        for value in try richValues() {
            let literal = Literal.queryValue(value)
            let direct = try projection(serializer.serialize(literal))
            guard case .literal(.queryValue(let decoded, _)) = direct else {
                Issue.record("Expected a normalized rich constant")
                continue
            }
            #expect(decoded == value)

            let nested = Expression.array([.literal(.int(1)), .literal(literal)])
            let parsedNested = try projection(serializer.serialize(nested))
            guard case .array(let children) = parsedNested,
                  case .literal(.queryValue(let nestedValue, _))? = children.last else {
                Issue.record("Expected an array constructor containing the rich constant")
                continue
            }
            #expect(nestedValue == value)
            #expect(try projection(serializer.serialize(parsedNested)) == parsedNested)

            let query = SelectQuery(
                projection: [ProjectionItem(.coalesce([.literal(literal), .null]))],
                source: .table(TableRef("values"))
            )
            let parsedQuery = try SQLParser().parseSelect(serializer.serialize(query))
            let items = parsedQuery.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }),
                  case .coalesce(let alternatives) = try projectionExpression(#require(items.first)),
                  case .literal(.queryValue(let queryValue, _))? = alternatives.first else {
                Issue.record("Expected a nested rich constant in the SELECT projection")
                continue
            }
            #expect(queryValue == value)
            #expect(try SQLParser().parseSelect(serializer.serialize(parsedQuery)) == parsedQuery)
        }
    }

    @Test("Wrapped primitives preserve SQL values and reject non-SQL representations")
    func primitiveSQLGenerationPreservesBehavior() throws {
        let serializer = SQLSerializer()
        let value = Literal.queryValue(.primitive(.string("a'b")))
        #expect(try serializer.serialize(value) == "CAST('a''b' AS TEXT)")
        #expect(try projection(serializer.serialize(value)) == .literal(.queryValue(.primitive(.string("a'b")), type: .sql(.text))))
        #expect(throws: SQLSerializationError.self) {
            _ = try serializer.serialize(Literal.queryValue(.primitive(.uint64(.max))))
        }
        #expect(throws: SQLSerializationError.self) {
            _ = try serializer.serialize(Literal.queryValue(.primitive(.object(FieldObject()))))
        }
    }
}
