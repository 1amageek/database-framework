/// QueryStatementTests.swift
/// Comprehensive tests for QueryStatement types

import Testing
@testable import QueryAST

// MARK: - SQL DML Statement Tests

@Suite("SQL DML Statement Tests")
struct SQLDMLStatementTests {

    // MARK: - INSERT Tests

    @Test("InsertQuery with VALUES")
    func testInsertWithValues() throws {
        let insert = InsertQuery(
            target: TableRef("users"),
            columns: ["name", "age"],
            source: .values([
                [.literal(.string("Alice")), .literal(.int(30))],
                [.literal(.string("Bob")), .literal(.int(25))]
            ])
        )

        #expect(insert.target.table == "users")
        #expect(insert.columns == ["name", "age"])
        if case .values(let rows) = insert.source {
            #expect(rows.count == 2)
        } else {
            Issue.record("Expected VALUES source")
        }
    }

    @Test("InsertQuery with SELECT")
    func testInsertWithSelect() throws {
        let selectQuery = SelectQuery(
            projection: .all,
            source: .table(TableRef("other_users"))
        )
        let insert = InsertQuery(
            target: TableRef("users"),
            source: .select(selectQuery)
        )

        if case .select(let query) = insert.source {
            #expect(query.projection == .all)
        } else {
            Issue.record("Expected SELECT source")
        }
    }

    @Test("InsertQuery with DEFAULT VALUES")
    func testInsertWithDefaultValues() throws {
        let insert = InsertQuery(
            target: TableRef("users"),
            source: .defaultValues
        )

        if case .defaultValues = insert.source {
            // OK
        } else {
            Issue.record("Expected DEFAULT VALUES source")
        }
    }

    @Test("InsertQuery with ON CONFLICT DO NOTHING")
    func testInsertOnConflictDoNothing() throws {
        let insert = InsertQuery(
            target: TableRef("users"),
            source: .values([[.literal(.string("Alice"))]]),
            onConflict: .doNothing
        )

        #expect(insert.onConflict == .doNothing)
    }

    @Test("InsertQuery with ON CONFLICT DO UPDATE")
    func testInsertOnConflictDoUpdate() throws {
        let insert = InsertQuery(
            target: TableRef("users"),
            source: .values([[.literal(.string("Alice"))]]),
            onConflict: .doUpdate(
                assignments: [("name", .literal(.string("Updated")))],
                where: nil
            )
        )

        if case .doUpdate(let assignments, _) = insert.onConflict {
            #expect(assignments.count == 1)
            #expect(assignments[0].0 == "name")
        } else {
            Issue.record("Expected DO UPDATE")
        }
    }

    @Test("InsertQuery with RETURNING")
    func testInsertWithReturning() throws {
        let insert = InsertQuery(
            target: TableRef("users"),
            source: .values([[.literal(.string("Alice"))]]),
            returning: [ProjectionItem(.column(ColumnRef(column: "id")))]
        )

        #expect(insert.returning?.count == 1)
    }

    // MARK: - UPDATE Tests

    @Test("UpdateQuery basic")
    func testUpdateBasic() throws {
        let update = UpdateQuery(
            target: TableRef("users"),
            assignments: [
                ("name", .literal(.string("Updated"))),
                ("age", .literal(.int(31)))
            ],
            filter: .equal(.column(ColumnRef(column: "id")), .literal(.int(1)))
        )

        #expect(update.target.table == "users")
        #expect(update.assignments.count == 2)
        #expect(update.filter != nil)
    }

    @Test("UpdateQuery with FROM")
    func testUpdateWithFrom() throws {
        let update = UpdateQuery(
            target: TableRef("users"),
            assignments: [("name", .column(ColumnRef(table: "other", column: "name")))],
            from: .table(TableRef("other")),
            filter: .equal(
                .column(ColumnRef(table: "users", column: "id")),
                .column(ColumnRef(table: "other", column: "user_id"))
            )
        )

        #expect(update.from != nil)
    }

    @Test("UpdateQuery with RETURNING")
    func testUpdateWithReturning() throws {
        let update = UpdateQuery(
            target: TableRef("users"),
            assignments: [("name", .literal(.string("Updated")))],
            returning: [
                ProjectionItem(.column(ColumnRef(column: "id"))),
                ProjectionItem(.column(ColumnRef(column: "name")))
            ]
        )

        #expect(update.returning?.count == 2)
    }

    // MARK: - DELETE Tests

    @Test("DeleteQuery basic")
    func testDeleteBasic() throws {
        let delete = DeleteQuery(
            target: TableRef("users"),
            filter: .equal(.column(ColumnRef(column: "id")), .literal(.int(1)))
        )

        #expect(delete.target.table == "users")
        #expect(delete.filter != nil)
    }

    @Test("DeleteQuery with USING")
    func testDeleteWithUsing() throws {
        let delete = DeleteQuery(
            target: TableRef("users"),
            using: .table(TableRef("audit")),
            filter: .equal(
                .column(ColumnRef(table: "users", column: "id")),
                .column(ColumnRef(table: "audit", column: "user_id"))
            )
        )

        #expect(delete.using != nil)
    }

    @Test("DeleteQuery with RETURNING")
    func testDeleteWithReturning() throws {
        let delete = DeleteQuery(
            target: TableRef("users"),
            filter: .equal(.column(ColumnRef(column: "id")), .literal(.int(1))),
            returning: [ProjectionItem(.column(ColumnRef(column: "name")))]
        )

        #expect(delete.returning?.count == 1)
    }
}

// MARK: - SQL/PGQ Graph Definition Tests

@Suite("SQL/PGQ Graph Definition Tests")
struct SQLPGQGraphDefinitionTests {

    @Test("CreateGraphStatement basic")
    func testCreateGraphBasic() throws {
        let create = CreateGraphStatement(
            graphName: "social",
            vertexTables: [
                VertexTableDefinition(
                    tableName: "persons",
                    keyColumns: ["id"]
                )
            ],
            edgeTables: [
                EdgeTableDefinition(
                    tableName: "friendships",
                    keyColumns: ["person1_id", "person2_id"],
                    sourceVertex: VertexReference(
                        tableName: "persons",
                        keyColumns: [(source: "person1_id", target: "id")]
                    ),
                    destinationVertex: VertexReference(
                        tableName: "persons",
                        keyColumns: [(source: "person2_id", target: "id")]
                    )
                )
            ]
        )

        #expect(create.graphName == "social")
        #expect(create.vertexTables.count == 1)
        #expect(create.edgeTables.count == 1)
    }

    @Test("CreateGraphStatement with IF NOT EXISTS")
    func testCreateGraphIfNotExists() throws {
        let create = CreateGraphStatement(
            graphName: "social",
            ifNotExists: true,
            vertexTables: [VertexTableDefinition(tableName: "persons", keyColumns: ["id"])],
            edgeTables: []
        )

        #expect(create.ifNotExists == true)
    }

    @Test("VertexTableDefinition with label")
    func testVertexWithLabel() throws {
        let vertex = VertexTableDefinition(
            tableName: "persons",
            keyColumns: ["id"],
            labelExpression: .single("Person")
        )

        if case .single(let label) = vertex.labelExpression {
            #expect(label == "Person")
        } else {
            Issue.record("Expected single label")
        }
    }

    @Test("VertexTableDefinition with dynamic label")
    func testVertexWithDynamicLabel() throws {
        let vertex = VertexTableDefinition(
            tableName: "entities",
            keyColumns: ["id"],
            labelExpression: .column("type")
        )

        if case .column(let col) = vertex.labelExpression {
            #expect(col == "type")
        } else {
            Issue.record("Expected column label")
        }
    }

    @Test("VertexTableDefinition with alias")
    func testVertexWithAlias() throws {
        let vertex = VertexTableDefinition(
            tableName: "persons",
            alias: "p",
            keyColumns: ["id"]
        )

        #expect(vertex.alias == "p")
    }

    @Test("VertexTableDefinition with properties spec")
    func testVertexWithProperties() throws {
        let vertexAll = VertexTableDefinition(
            tableName: "persons",
            keyColumns: ["id"],
            propertiesSpec: .all
        )
        #expect(vertexAll.propertiesSpec == .all)

        let vertexNone = VertexTableDefinition(
            tableName: "persons",
            keyColumns: ["id"],
            propertiesSpec: PropertiesSpec.none
        )
        #expect(vertexNone.propertiesSpec == PropertiesSpec.none)

        let vertexCols = VertexTableDefinition(
            tableName: "persons",
            keyColumns: ["id"],
            propertiesSpec: .columns(["name", "age"])
        )
        if case .columns(let cols) = vertexCols.propertiesSpec {
            #expect(cols == ["name", "age"])
        }

        let vertexExcept = VertexTableDefinition(
            tableName: "persons",
            keyColumns: ["id"],
            propertiesSpec: .allExcept(["password"])
        )
        if case .allExcept(let cols) = vertexExcept.propertiesSpec {
            #expect(cols == ["password"])
        }
    }

    @Test("EdgeTableDefinition with label")
    func testEdgeWithLabel() throws {
        let edge = EdgeTableDefinition(
            tableName: "friendships",
            keyColumns: ["id"],
            sourceVertex: VertexReference(tableName: "persons", keyColumns: [(source: "person1_id", target: "id")]),
            destinationVertex: VertexReference(tableName: "persons", keyColumns: [(source: "person2_id", target: "id")]),
            labelExpression: .single("FRIEND")
        )

        if case .single(let label) = edge.labelExpression {
            #expect(label == "FRIEND")
        }
    }

    @Test("LabelExpression combinations")
    func testLabelExpressionCombinations() throws {
        let orExpr = LabelExpression.or([.single("Person"), .single("Employee")])
        if case .or(let exprs) = orExpr {
            #expect(exprs.count == 2)
        }

        let andExpr = LabelExpression.and([.single("Active"), .single("Premium")])
        if case .and(let exprs) = andExpr {
            #expect(exprs.count == 2)
        }
    }

    @Test("VertexReference equality")
    func testVertexReferenceEquality() throws {
        let ref1 = VertexReference(tableName: "persons", keyColumns: [(source: "id", target: "id")])
        let ref2 = VertexReference(tableName: "persons", keyColumns: [(source: "id", target: "id")])
        let ref3 = VertexReference(tableName: "users", keyColumns: [(source: "id", target: "id")])

        #expect(ref1 == ref2)
        #expect(ref1 != ref3)
    }
}

// MARK: - SPARQL Update Statement Tests

@Suite("SPARQL Update Statement Tests")
struct SPARQLUpdateStatementTests {

    @Test("InsertDataQuery")
    func testInsertData() throws {
        let triple = TriplePattern(
            subject: .iri("http://example.org/alice"),
            predicate: .iri("http://xmlns.com/foaf/0.1/name"),
            object: .literal(.string("Alice"))
        )
        let insert = InsertDataQuery(quads: [Quad(triple: triple)])

        #expect(insert.quads.count == 1)
    }

    @Test("DeleteDataQuery")
    func testDeleteData() throws {
        let triple = TriplePattern(
            subject: .iri("http://example.org/alice"),
            predicate: .iri("http://xmlns.com/foaf/0.1/name"),
            object: .literal(.string("Alice"))
        )
        let delete = DeleteDataQuery(quads: [Quad(triple: triple)])

        #expect(delete.quads.count == 1)
    }

    @Test("DeleteInsertQuery")
    func testDeleteInsert() throws {
        let deleteTriple = TriplePattern(
            subject: .variable("s"),
            predicate: .iri("http://xmlns.com/foaf/0.1/age"),
            object: .variable("oldAge")
        )
        let insertTriple = TriplePattern(
            subject: .variable("s"),
            predicate: .iri("http://xmlns.com/foaf/0.1/age"),
            object: .literal(.int(31))
        )
        let wherePattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("s"),
                predicate: .iri("http://xmlns.com/foaf/0.1/name"),
                object: .literal(.string("Alice"))
            )
        ])

        let query = DeleteInsertQuery(
            deletePattern: [Quad(triple: deleteTriple)],
            insertPattern: [Quad(triple: insertTriple)],
            wherePattern: wherePattern
        )

        #expect(query.deletePattern?.count == 1)
        #expect(query.insertPattern?.count == 1)
    }

    @Test("Quad with graph")
    func testQuadWithGraph() throws {
        let triple = TriplePattern(
            subject: .variable("s"),
            predicate: .variable("p"),
            object: .variable("o")
        )
        let quad = Quad(
            graph: .iri("http://example.org/graph1"),
            triple: triple
        )

        #expect(quad.graph != nil)
    }

    @Test("GraphRef")
    func testGraphRef() throws {
        let ref1 = GraphRef(iri: "http://example.org/default")
        #expect(ref1.isNamed == false)

        let ref2 = GraphRef(iri: "http://example.org/named", isNamed: true)
        #expect(ref2.isNamed == true)
    }

    @Test("LoadQuery")
    func testLoadQuery() throws {
        let load = LoadQuery(
            source: "http://example.org/data.ttl",
            destination: "http://example.org/graph1",
            silent: true
        )

        #expect(load.source == "http://example.org/data.ttl")
        #expect(load.destination == "http://example.org/graph1")
        #expect(load.silent == true)
    }

    @Test("ClearQuery targets")
    func testClearQueryTargets() throws {
        let clearGraph = ClearQuery(target: .graph("http://example.org/graph1"))
        if case .graph(let iri) = clearGraph.target {
            #expect(iri == "http://example.org/graph1")
        }

        let clearDefault = ClearQuery(target: .default)
        #expect(clearDefault.target == .default)

        let clearNamed = ClearQuery(target: .named)
        #expect(clearNamed.target == .named)

        let clearAll = ClearQuery(target: .all, silent: true)
        #expect(clearAll.target == .all)
        #expect(clearAll.silent == true)
    }
}

// MARK: - SPARQL Query Form Tests

@Suite("SPARQL Query Form Tests")
struct SPARQLQueryFormTests {

    @Test("ConstructQuery")
    func testConstructQuery() throws {
        let template = [
            TriplePattern(
                subject: .variable("s"),
                predicate: .iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                object: .iri("http://example.org/Person")
            )
        ]
        let pattern = GraphPattern.basic([
            TriplePattern(
                subject: .variable("s"),
                predicate: .iri("http://xmlns.com/foaf/0.1/name"),
                object: .variable("name")
            )
        ])

        let construct = ConstructQuery(
            template: template,
            pattern: pattern,
            orderBy: [SortKey(.variable(Variable("name")), direction: .ascending)],
            limit: 100,
            offset: 10
        )

        #expect(construct.template.count == 1)
        #expect(construct.limit == 100)
        #expect(construct.offset == 10)
        #expect(construct.orderBy?.count == 1)
    }

    @Test("AskQuery")
    func testAskQuery() throws {
        let pattern = GraphPattern.basic([
            TriplePattern(
                subject: .iri("http://example.org/alice"),
                predicate: .iri("http://xmlns.com/foaf/0.1/knows"),
                object: .iri("http://example.org/bob")
            )
        ])

        let ask = AskQuery(pattern: pattern)
        if case .basic(let triples) = ask.pattern {
            #expect(triples.count == 1)
        }
    }

    @Test("DescribeQuery with resources")
    func testDescribeQuery() throws {
        let describe = DescribeQuery(
            resources: [
                .iri("http://example.org/alice"),
                .variable("person")
            ],
            pattern: GraphPattern.basic([
                TriplePattern(
                    subject: .variable("person"),
                    predicate: .iri("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                    object: .iri("http://example.org/Person")
                )
            ])
        )

        #expect(describe.resources.count == 2)
        #expect(describe.pattern != nil)
    }

    @Test("DescribeQuery without pattern")
    func testDescribeQueryNoPattern() throws {
        let describe = DescribeQuery(
            resources: [.iri("http://example.org/alice")]
        )

        #expect(describe.pattern == nil)
    }
}

// MARK: - QueryStatement Analysis Tests

@Suite("QueryStatement Analysis Tests")
struct QueryStatementAnalysisTests {

    @Test("isReadOnly")
    func testIsReadOnly() throws {
        let select = QueryStatement.select(SelectQuery(projection: .all, source: .table(TableRef("users"))))
        #expect(select.isReadOnly == true)

        let ask = QueryStatement.ask(AskQuery(pattern: .basic([])))
        #expect(ask.isReadOnly == true)

        let construct = QueryStatement.construct(ConstructQuery(template: [], pattern: .basic([])))
        #expect(construct.isReadOnly == true)

        let describe = QueryStatement.describe(DescribeQuery(resources: []))
        #expect(describe.isReadOnly == true)

        let insert = QueryStatement.insert(InsertQuery(target: TableRef("users"), source: .defaultValues))
        #expect(insert.isReadOnly == false)
    }

    @Test("isModification")
    func testIsModification() throws {
        let insert = QueryStatement.insert(InsertQuery(target: TableRef("users"), source: .defaultValues))
        #expect(insert.isModification == true)

        let update = QueryStatement.update(UpdateQuery(target: TableRef("users"), assignments: []))
        #expect(update.isModification == true)

        let delete = QueryStatement.delete(DeleteQuery(target: TableRef("users")))
        #expect(delete.isModification == true)

        let insertData = QueryStatement.insertData(InsertDataQuery(quads: []))
        #expect(insertData.isModification == true)

        let deleteData = QueryStatement.deleteData(DeleteDataQuery(quads: []))
        #expect(deleteData.isModification == true)

        let deleteInsert = QueryStatement.deleteInsert(DeleteInsertQuery(
            deletePattern: nil,
            insertPattern: nil,
            wherePattern: .basic([])
        ))
        #expect(deleteInsert.isModification == true)

        let load = QueryStatement.load(LoadQuery(source: "http://example.org/data"))
        #expect(load.isModification == true)

        let clear = QueryStatement.clear(ClearQuery(target: .all))
        #expect(clear.isModification == true)

        let select = QueryStatement.select(SelectQuery(projection: .all, source: .table(TableRef("users"))))
        #expect(select.isModification == false)
    }

    @Test("isSchemaDefinition")
    func testIsSchemaDefinition() throws {
        let createGraph = QueryStatement.createGraph(CreateGraphStatement(
            graphName: "test",
            vertexTables: [],
            edgeTables: []
        ))
        #expect(createGraph.isSchemaDefinition == true)

        let dropGraph = QueryStatement.dropGraph("test")
        #expect(dropGraph.isSchemaDefinition == true)

        let createSPARQLGraph = QueryStatement.createSPARQLGraph("http://example.org/graph", silent: false)
        #expect(createSPARQLGraph.isSchemaDefinition == true)

        let dropSPARQLGraph = QueryStatement.dropSPARQLGraph("http://example.org/graph", silent: true)
        #expect(dropSPARQLGraph.isSchemaDefinition == true)

        let select = QueryStatement.select(SelectQuery(projection: .all, source: .table(TableRef("users"))))
        #expect(select.isSchemaDefinition == false)
    }
}

// MARK: - OnConflictAction Tests

@Suite("OnConflictAction Tests")
struct OnConflictActionTests {

    @Test("OnConflictAction equality")
    func testOnConflictActionEquality() throws {
        let doNothing1 = OnConflictAction.doNothing
        let doNothing2 = OnConflictAction.doNothing
        #expect(doNothing1 == doNothing2)

        let doUpdate1 = OnConflictAction.doUpdate(
            assignments: [("name", .literal(.string("test")))],
            where: nil
        )
        let doUpdate2 = OnConflictAction.doUpdate(
            assignments: [("name", .literal(.string("test")))],
            where: nil
        )
        #expect(doUpdate1 == doUpdate2)

        let doUpdate3 = OnConflictAction.doUpdate(
            assignments: [("name", .literal(.string("different")))],
            where: nil
        )
        #expect(doUpdate1 != doUpdate3)

        #expect(doNothing1 != doUpdate1)
    }

    @Test("OnConflictAction hashable")
    func testOnConflictActionHashable() throws {
        var set = Set<OnConflictAction>()
        set.insert(.doNothing)
        set.insert(.doUpdate(assignments: [("x", .literal(.int(1)))], where: nil))

        #expect(set.count == 2)
    }
}
