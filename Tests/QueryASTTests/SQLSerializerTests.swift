import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing
@testable import QueryAST

@Suite("Canonical SQL serializer", .heartbeat)
struct SQLSerializerTests {
    private let serializer = SQLSerializer()
    private let valueLimits = QueryValueLimits(maximumRetainedBytes: 65536, maximumTotalNodes: 1024,
                                               maximumNestingDepth: 32, maximumCollectionElements: 1024)

    private func parsedLiteral(_ text: String) throws -> Literal {
        let query = try SQLParser().parseSelect("SELECT " + text)
        let items = query.projection
        guard items.allSatisfy({ if case .expression = $0 { return true }; return false }), items.count == 1,
              case .literal(let value) = try projectionExpression(items[0]) else {
            throw SQLSerializationError.invalidStructure("Expected one normalized constant")
        }
        return value
    }

    @Test("Identifiers quote exact semantic names and embedded quotes")
    func identifiers() throws {
        let query = TableRef(schema: "Mixed Schema", table: "a\"b", alias: "t")
            .select(ProjectionItem(.column(ColumnRef(table: "t", column: "a\"b")), alias: "Answer"))
        let sql = try serializer.serialize(query)
        #expect(sql == "SELECT \"t\".\"a\"\"b\" AS \"Answer\" FROM \"Mixed Schema\".\"a\"\"b\" AS \"t\"")
        #expect(try SQLParser().parseSelect(sql) == query)
    }

    @Test("Scalar escaping recognizes quote before a combining mark")
    func combiningMarkEscaping() throws {
        #expect(try serializer.serialize(Literal.string("a'\u{301}b")) == "CAST('a''\u{301}b' AS TEXT)")
        #expect(try parsedLiteral(serializer.serialize(Literal.string("a'\u{301}b"))) == .queryValue(.primitive(.string("a'\u{301}b")), type: .sql(.text)))
        #expect(throws: SQLSerializationError.self) { try serializer.serialize(Literal.string("a\0b")) }
    }

    @Test("Parentheses preserve subtraction and mixed boolean precedence")
    func expressionPrecedence() throws {
        let expression = Expression.and(.equal(.subtract(.column(ColumnRef("a")), .subtract(.column(ColumnRef("b")), .column(ColumnRef("c")))), .column(ColumnRef("d"))),
                                        .or(.column(ColumnRef("e")), .column(ColumnRef("f"))))
        let text = try serializer.serialize(expression)
        let parsed = try SQLParser().parseSelect("SELECT " + text)
        #expect(parsed.projection == [ProjectionItem(expression)])
        let concatenation = Expression.aggregate(.groupConcat(.column(ColumnRef("name")), separator: "'|", distinct: true))
        let aggregateQuery = try SQLParser().parseSelect("SELECT " + serializer.serialize(concatenation))
        #expect(aggregateQuery.projection == [ProjectionItem(concatenation)])
    }

    @Test("CTE materialization, DISTINCT, lateral placement and NULL sorting survive")
    func queryMetadata() throws {
        let inner = SelectQuery(projection: [.wildcard()], source: .table(TableRef("events")))
        let query = SelectQuery(projection: [.wildcard()],
            source: .join(JoinClause(type: .leftLateral, left: .table(TableRef("events")),
                                    right: .aliased(.subquery(inner), name: "nested"), condition: .on(.literal(.bool(true))))),
            orderBy: [SortKey(.column(ColumnRef("id")), direction: .descending, nulls: .first)],
            quantifier: .distinct,
            subqueries: [NamedSubquery(name: "cte", columns: ["id"], query: inner, materialized: .notMaterialized)])
        let text = try serializer.serialize(query)
        #expect(text.contains("AS NOT MATERIALIZED"))
        #expect(text.contains("LEFT JOIN LATERAL"))
        #expect(text.contains("SELECT DISTINCT *"))
        #expect(text.hasSuffix("DESC NULLS FIRST"))
        #expect(try SQLParser().parseSelect(text) == query)
    }

    @Test("Temporal precision and nested type modifiers survive SQL text")
    func typeModifiers() throws {
        for type in [DataType.time(withTimeZone: true, precision: 0),
                     .timestamp(withTimeZone: false, precision: 6),
                     .array(.decimal(precision: 18, scale: 4)), .varchar(length: 27)] {
            let literal = Literal.queryValue(.primitive(.null), type: .sql(type))
            #expect(try parsedLiteral(serializer.serialize(literal)) == literal)
        }
        #expect(throws: SQLSerializationError.self) { try serializer.serialize(DataType.time(withTimeZone: false, precision: 7)) }
        #expect(throws: SQLSerializationError.self) { try serializer.serialize(DataType.decimal(precision: nil, scale: 2)) }
        for type in [DataType.char(length: 3), .varchar(length: 3)] {
            let literal = Literal.queryValue(.primitive(.string("abc")), type: .sql(type))
            #expect(try parsedLiteral(serializer.serialize(literal)) == literal)
        }
        for (text, type) in [("a", DataType.char(length: 3)), ("abcd", .char(length: 3)), ("abcd", .varchar(length: 3))] {
            #expect(throws: SQLSerializationError.self) {
                try serializer.serialize(Literal.queryValue(.primitive(.string(text)), type: .sql(type)))
            }
        }
    }

    @Test("Width and floating declarations survive rather than becoming NUMERIC")
    func scalarWidths() throws {
        let values: [(FieldValue, DataType)] = [(.int16(-42), .smallint), (.int32(42), .integer),
            (.int64(42), .bigint), (.float32(1.25), .real), (.float64(1.25), .doublePrecision)]
        for (value, type) in values {
            let literal = Literal.queryValue(.primitive(value), type: .sql(type))
            #expect(try parsedLiteral(serializer.serialize(literal)) == literal)
        }
        #expect(try serializer.serialize(Literal.queryValue(.primitive(.int64(42)))) == "CAST(42 AS BIGINT)")
        #expect(throws: SQLSerializationError.self) { try serializer.serialize(Literal.uint(1)) }
        #expect(throws: SQLSerializationError.self) { try serializer.serialize(Literal.double(.infinity)) }
    }

    @Test("All rich families retain exact values and stabilize after parsing")
    func richFamilies() throws {
        let values: [(QueryValue, DataType)] = [
            (.numeric(try QueryNumericValue(value: .init(coefficient: 123, scale: 2), displayScale: 4)), .decimal(precision: nil, scale: nil)),
            (.json(try QueryJSONValue(bytes: ByteString(utf8: " { \"a\": 1, \"a\": 2 } "), limits: valueLimits)), .json),
            (.jsonb(try QueryJSONBValue(json: QueryJSONValue(bytes: ByteString(utf8: "{\"z\": [null,true,1.2300], \"a\": \"x\\n'y\"}"), limits: valueLimits), limits: valueLimits)), .jsonb),
            (.interval(try QueryIntervalValue(period: CalendarPeriod(months: -2, days: 3), time: TimeSpan(seconds: -1, nanoseconds: 999_999_000))), .interval()),
            (.array(try QueryArrayValue(elementType: .integer,
                dimensions: [QueryArrayDimension(lowerBound: -2, length: 2), QueryArrayDimension(lowerBound: 3, length: 2)],
                elements: [.primitive(.int32(1)), .primitive(.null), .primitive(.int32(-3)), .primitive(.int32(4))], limits: valueLimits)), .array(.integer))
        ]
        for (value, type) in values {
            let literal = Literal.queryValue(value, type: .sql(type))
            let first = try parsedLiteral(serializer.serialize(literal))
            #expect(first == literal)
            #expect(try parsedLiteral(serializer.serialize(first)) == first)
        }
    }

    @Test("Scaled zero and integral NUMERIC remain NUMERIC with exact display scales")
    func numericDisplayScale() throws {
        for numeric in [try QueryNumericValue(value: .init(coefficient: 0, scale: 0), displayScale: 5),
                        try QueryNumericValue(value: .init(coefficient: 42, scale: 0), displayScale: 0)] {
            let literal = Literal.queryValue(.numeric(numeric), type: .sql(.decimal(precision: nil, scale: nil)))
            #expect(try parsedLiteral(serializer.serialize(literal)) == literal)
        }
        let zero = Literal.queryValue(.numeric(try QueryNumericValue(value: .init(coefficient: 0, scale: 0), displayScale: 5)))
        #expect(try serializer.serialize(zero) == "0.00000")
    }

    @Test("Array input distinguishes NULL from quoted NULL and escapes every delimiter")
    func arrayStrings() throws {
        let array = try QueryArrayValue(elementType: .text,
            dimensions: [QueryArrayDimension(lowerBound: 0, length: 4)],
            elements: [.primitive(.null), .primitive(.string("NULL")), .primitive(.string("\"\u{301}\\,{}'")), .primitive(.string(""))], limits: valueLimits)
        let literal = Literal.queryValue(.array(array), type: .sql(.array(.text)))
        #expect(try parsedLiteral(serializer.serialize(literal)) == literal)
        let empty = Literal.queryValue(.array(try QueryArrayValue(elementType: .jsonb, dimensions: [], elements: [], limits: valueLimits)), type: .sql(.array(.jsonb)))
        #expect(try parsedLiteral(serializer.serialize(empty)) == empty)
    }

    @Test("Rich array elements retain numeric, JSONB and interval metadata")
    func richArrayElements() throws {
        let cases: [(DataType, QueryValue)] = [
            (.decimal(precision: 8, scale: 3), .numeric(try QueryNumericValue(value: .init(coefficient: 12, scale: 1), displayScale: 3))),
            (.jsonb, .jsonb(try QueryJSONBValue(json: QueryJSONValue(bytes: ByteString(utf8: "{\"n\":0.00}"), limits: valueLimits), limits: valueLimits))),
            (.interval(), .interval(try QueryIntervalValue(period: CalendarPeriod(months: -1, days: 2), time: TimeSpan(seconds: 3))))
        ]
        for (type, value) in cases {
            let array = try QueryArrayValue(elementType: type, dimensions: [QueryArrayDimension(lowerBound: 1, length: 1)], elements: [value], limits: valueLimits)
            let literal = Literal.queryValue(.array(array), type: .sql(.array(type)))
            #expect(try parsedLiteral(serializer.serialize(literal)) == literal)
        }
    }

    @Test("SQL/PGQ graph names, pattern labels and DDL keys use identifier grammar")
    func graphGrammar() throws {
        let graph = CreateGraphStatement(graphName: "Graph Name", vertexTables: [
            VertexTableDefinition(tableName: "People", alias: "p", keyColumns: ["Id"], labelExpression: .single("Person"), propertiesSpec: .columns(["Name"]))
        ], edgeTables: [EdgeTableDefinition(tableName: "Knows", keyColumns: ["Id"],
            sourceVertex: VertexReference(tableName: "p", keyColumns: [KeyColumnMapping(source: "From", target: "Id")]),
            destinationVertex: VertexReference(tableName: "p", keyColumns: [KeyColumnMapping(source: "To", target: "Id")]))])
        #expect(try SQLParser().parse(serializer.serialize(graph)) == .createGraph(graph))
        let source = GraphTableSource(graphName: "Graph Name", matchPattern: MatchPattern(paths: [PathPattern(elements: [
            .node(NodePattern(variable: "a", labels: ["Person"])), .edge(EdgePattern(variable: "e", direction: .outgoing)), .node(NodePattern(variable: "b"))
        ])]), columns: [GraphTableColumn(expression: .column(ColumnRef(table: "b", column: "Name")), alias: "Name")])
        let query = SelectQuery(projection: [.wildcard()], source: .graphTable(source))
        #expect(try SQLParser().parseSelect(serializer.serialize(query)) == query)
    }

    @Test("Constructors and positional/named parameters retain child order and identity")
    func constructorsAndParameters() throws {
        let expression = Expression.row([.array([.parameter(.position(2)), .parameter(.name("first"))]), .parameter(.position(1))])
        let query = try SQLParser().parseSelect("SELECT " + serializer.serialize(expression))
        #expect(query.projection == [ProjectionItem(expression)])
        #expect(throws: SQLSerializationError.self) { try serializer.serialize(Expression.parameter(.position(0))) }
        for name in ["LOWER", "coalesce", "NULLIF"] {
            let call = Expression.function(FunctionCall(name: name, arguments: [.parameter(.name("x"))], distinct: true))
            let parsed = try SQLParser().parseSelect("SELECT " + serializer.serialize(call))
            #expect(parsed.projection == [ProjectionItem(call)])
        }
        #expect(throws: SQLSerializationError.self) {
            try serializer.serialize(Expression.function(FunctionCall(name: "f", arguments: [], distinct: true)))
        }
    }

    @Test("Non-SQL language, physical sources and unrepresentable metadata fail explicitly")
    func unsupportedContracts() throws {
        let queries = [
            SelectQuery(projection: [.wildcard()], source: .logical(LogicalSourceRef(kindIdentifier: "custom", identifier: "data"))),
            SelectQuery(projection: [.wildcard()], source: .values([])),
            SelectQuery(projection: [.wildcard()], source: .table(TableRef("a")), reduced: true),
            SelectQuery(projection: [.wildcard()], source: .table(TableRef("a", partitions: try FieldObject([("tenant", .string("t"))]))))
        ]
        for query in queries { #expect(throws: SQLSerializationError.self) { try serializer.serialize(query) } }
        for expression in [Expression.variable(Variable("x")), .regex(.column(ColumnRef("x")), pattern: "a.*", flags: "i")] {
            #expect(throws: SQLSerializationError.self) { try serializer.serialize(expression) }
        }
        #expect(throws: SQLSerializationError.self) { try serializer.serialize(Literal.iri("urn:a")) }
    }

    @Test("UTF-8 output and cumulative multi-statement budgets reject before growth")
    func boundedOutput() throws {
        let tiny = SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: 5))
        #expect(try tiny.serialize(Literal.sqlUnknownString("é")) == "'é'")
        #expect(throws: SQLSerializationError.self) { try tiny.serialize(Literal.sqlUnknownString("''")) }
        let query = SelectQuery(projection: [.wildcard()], source: .table(TableRef("a")))
        let one = try serializer.serialize(query)
        let block = SQLSerializer(limits: QueryStructuralLimits(maximumRetainedBytes: UInt64(one.utf8.count)))
        #expect(try block.serialize(query) == one)
        #expect(throws: SQLSerializationError.self) { try block.serialize([.select(query), .select(query)]) }
        let shallow = SQLSerializer(limits: QueryStructuralLimits(maximumNestingDepth: 3))
        #expect(throws: SQLSerializationError.self) { try shallow.serialize(Expression.not(.not(.not(.not(.null))))) }
    }

    @Test("DML assignment and RETURNING scopes round-trip through one statement writer")
    func mutations() throws {
        let update = QueryStatement.update(UpdateQuery(target: TableRef("events"), assignments: [Assignment(column: "value", value: .parameter(.position(1)))],
                                                       filter: .equal(.column(ColumnRef("id")), .parameter(.position(2))), returning: [.column("value")]))
        let delete = QueryStatement.delete(DeleteQuery(target: TableRef("events"), filter: .isNull(.column(ColumnRef("value"))), returning: [.column("id")]))
        for statement in [update, delete] { #expect(try SQLParser().parse(serializer.serialize(statement)) == statement) }
    }
}
