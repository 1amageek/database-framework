/// CreateGraphTests.swift
/// Comprehensive tests for SQL/PGQ CREATE PROPERTY GRAPH types

import Testing
@testable import QueryAST

// MARK: - CreateGraphStatement Builder Tests

@Suite("CreateGraphStatement Builder Tests")
struct CreateGraphStatementBuilderTests {

    @Test("CreateGraphStatement.simple builder")
    func testSimpleBuilder() throws {
        let create = CreateGraphStatement.simple(
            name: "social",
            vertexTable: "persons",
            vertexKey: "id",
            edgeTable: "friendships",
            sourceKey: "person1_id",
            targetKey: "person2_id"
        )

        #expect(create.graphName == "social")
        #expect(create.vertexTables.count == 1)
        #expect(create.vertexTables[0].tableName == "persons")
        #expect(create.edgeTables.count == 1)
        #expect(create.edgeTables[0].tableName == "friendships")
    }

    @Test("CreateGraphStatement.ifNotExists modifier")
    func testIfNotExistsModifier() throws {
        let create = CreateGraphStatement(
            graphName: "test",
            vertexTables: [],
            edgeTables: []
        ).ifNotExists()

        #expect(create.ifNotExists == true)
    }

    @Test("CreateGraphStatement.builder fluent API")
    func testBuilderFluentAPI() throws {
        let create = CreateGraphStatement.builder(name: "social")
            .ifNotExists()
            .vertex(table: "persons", key: "id", label: "Person")
            .vertex(table: "companies", key: "id", label: "Company")
            .edge(
                table: "works_at",
                sourceTable: "persons",
                sourceKey: "person_id",
                destinationTable: "companies",
                destinationKey: "company_id",
                label: "WORKS_AT"
            )
            .build()

        #expect(create.graphName == "social")
        #expect(create.ifNotExists == true)
        #expect(create.vertexTables.count == 2)
        #expect(create.edgeTables.count == 1)
    }

    @Test("GraphSchemaBuilder vertex with definition")
    func testBuilderVertexWithDefinition() throws {
        let vertexDef = VertexTableDefinition(
            tableName: "custom",
            alias: "c",
            keyColumns: ["id"],
            labelExpression: .single("Custom"),
            propertiesSpec: .all
        )

        let create = CreateGraphStatement.builder(name: "test")
            .vertex(vertexDef)
            .build()

        #expect(create.vertexTables.count == 1)
        #expect(create.vertexTables[0].alias == "c")
    }

    @Test("GraphSchemaBuilder edge with definition")
    func testBuilderEdgeWithDefinition() throws {
        let edgeDef = EdgeTableDefinition(
            tableName: "relationships",
            keyColumns: ["id"],
            sourceVertex: VertexReference.simple(table: "persons", sourceKey: "from_id", targetKey: "id"),
            destinationVertex: VertexReference.simple(table: "persons", sourceKey: "to_id", targetKey: "id")
        )

        let create = CreateGraphStatement.builder(name: "test")
            .vertex(table: "persons", key: "id")
            .edge(edgeDef)
            .build()

        #expect(create.edgeTables.count == 1)
    }
}

// MARK: - VertexTableDefinition Builder Tests

@Suite("VertexTableDefinition Builder Tests")
struct VertexTableDefinitionBuilderTests {

    @Test("VertexTableDefinition.labeled builder")
    func testLabeledBuilder() throws {
        let vertex = VertexTableDefinition.labeled(
            table: "persons",
            key: "id",
            label: "Person"
        )

        #expect(vertex.tableName == "persons")
        #expect(vertex.keyColumns == ["id"])
        if case .single(let label) = vertex.labelExpression {
            #expect(label == "Person")
        }
    }

    @Test("VertexTableDefinition.dynamicLabel builder")
    func testDynamicLabelBuilder() throws {
        let vertex = VertexTableDefinition.dynamicLabel(
            table: "entities",
            key: "id",
            labelColumn: "entity_type"
        )

        if case .column(let col) = vertex.labelExpression {
            #expect(col == "entity_type")
        }
    }

    @Test("VertexTableDefinition.as modifier")
    func testAsModifier() throws {
        let vertex = VertexTableDefinition(tableName: "persons", keyColumns: ["id"])
            .as("p")

        #expect(vertex.alias == "p")
        #expect(vertex.tableName == "persons")
    }

    @Test("VertexTableDefinition.properties modifier")
    func testPropertiesModifier() throws {
        let vertex = VertexTableDefinition(tableName: "persons", keyColumns: ["id"])
            .properties(.columns(["name", "age"]))

        if case .columns(let cols) = vertex.propertiesSpec {
            #expect(cols == ["name", "age"])
        }
    }

    @Test("VertexTableDefinition chained modifiers")
    func testChainedModifiers() throws {
        let vertex = VertexTableDefinition.labeled(table: "persons", key: "id", label: "Person")
            .as("p")
            .properties(.allExcept(["password"]))

        #expect(vertex.alias == "p")
        if case .allExcept(let excluded) = vertex.propertiesSpec {
            #expect(excluded == ["password"])
        }
    }
}

// MARK: - EdgeTableDefinition Builder Tests

@Suite("EdgeTableDefinition Builder Tests")
struct EdgeTableDefinitionBuilderTests {

    @Test("EdgeTableDefinition.labeled builder")
    func testLabeledBuilder() throws {
        let edge = EdgeTableDefinition.labeled(
            table: "friendships",
            key: ["id"],
            from: VertexReference.simple(table: "persons", sourceKey: "person1_id", targetKey: "id"),
            to: VertexReference.simple(table: "persons", sourceKey: "person2_id", targetKey: "id"),
            label: "FRIEND"
        )

        #expect(edge.tableName == "friendships")
        if case .single(let label) = edge.labelExpression {
            #expect(label == "FRIEND")
        }
    }

    @Test("EdgeTableDefinition.as modifier")
    func testAsModifier() throws {
        let edge = EdgeTableDefinition(
            tableName: "edges",
            keyColumns: ["id"],
            sourceVertex: VertexReference(tableName: "v", keyColumns: []),
            destinationVertex: VertexReference(tableName: "v", keyColumns: [])
        ).as("e")

        #expect(edge.alias == "e")
    }

    @Test("EdgeTableDefinition.properties modifier")
    func testPropertiesModifier() throws {
        let edge = EdgeTableDefinition(
            tableName: "edges",
            keyColumns: ["id"],
            sourceVertex: VertexReference(tableName: "v", keyColumns: []),
            destinationVertex: VertexReference(tableName: "v", keyColumns: [])
        ).properties(PropertiesSpec.none)

        #expect(edge.propertiesSpec == PropertiesSpec.none)
    }
}

// MARK: - VertexReference Builder Tests

@Suite("VertexReference Builder Tests")
struct VertexReferenceBuilderTests {

    @Test("VertexReference.simple builder")
    func testSimpleBuilder() throws {
        let ref = VertexReference.simple(
            table: "persons",
            sourceKey: "person_id",
            targetKey: "id"
        )

        #expect(ref.tableName == "persons")
        #expect(ref.keyColumns.count == 1)
        #expect(ref.keyColumns[0].source == "person_id")
        #expect(ref.keyColumns[0].target == "id")
    }

    @Test("VertexReference.composite builder")
    func testCompositeBuilder() throws {
        let ref = VertexReference.composite(
            table: "persons",
            keys: [
                KeyColumnMapping(source: "tenant_id", target: "tenant_id"),
                KeyColumnMapping(source: "person_id", target: "id")
            ]
        )

        #expect(ref.keyColumns.count == 2)
    }
}

// MARK: - SQL Generation Tests

@Suite("CreateGraph SQL Generation Tests")
struct CreateGraphSQLGenerationTests {

    @Test("CreateGraphStatement toSQL basic")
    func testBasicToSQL() throws {
        let create = CreateGraphStatement(
            graphName: "social",
            vertexTables: [
                VertexTableDefinition(tableName: "persons", keyColumns: ["id"])
            ],
            edgeTables: []
        )

        let sql = create.toSQL()
        #expect(sql.contains("CREATE PROPERTY GRAPH social"))
        #expect(sql.contains("VERTEX TABLES"))
        #expect(sql.contains("persons"))
    }

    @Test("CreateGraphStatement toSQL with IF NOT EXISTS")
    func testToSQLWithIfNotExists() throws {
        let create = CreateGraphStatement(
            graphName: "test",
            ifNotExists: true,
            vertexTables: [VertexTableDefinition(tableName: "v", keyColumns: ["id"])],
            edgeTables: []
        )

        let sql = create.toSQL()
        #expect(sql.contains("IF NOT EXISTS"))
    }

    @Test("VertexTableDefinition toSQL")
    func testVertexToSQL() throws {
        let vertex = VertexTableDefinition(
            tableName: "persons",
            alias: "p",
            keyColumns: ["id"],
            labelExpression: .single("Person"),
            propertiesSpec: .all
        )

        let sql = vertex.toSQL()
        #expect(sql.contains("persons AS p"))
        #expect(sql.contains("KEY (id)"))
        #expect(sql.contains("LABEL Person"))
        #expect(sql.contains("PROPERTIES ALL COLUMNS"))
    }

    @Test("EdgeTableDefinition toSQL")
    func testEdgeToSQL() throws {
        let edge = EdgeTableDefinition(
            tableName: "friendships",
            keyColumns: ["id"],
            sourceVertex: VertexReference(
                tableName: "persons",
                keyColumns: [KeyColumnMapping(source: "person1_id", target: "id")]
            ),
            destinationVertex: VertexReference(
                tableName: "persons",
                keyColumns: [KeyColumnMapping(source: "person2_id", target: "id")]
            ),
            labelExpression: .single("FRIEND")
        )

        let sql = edge.toSQL()
        #expect(sql.contains("friendships"))
        #expect(sql.contains("KEY (id)"))
        #expect(sql.contains("SOURCE KEY"))
        #expect(sql.contains("DESTINATION KEY"))
        #expect(sql.contains("REFERENCES persons"))
        #expect(sql.contains("LABEL FRIEND"))
    }

    @Test("LabelExpression toSQL")
    func testLabelExpressionToSQL() throws {
        let single = LabelExpression.single("Person")
        #expect(single.toSQL() == "Person")

        let column = LabelExpression.column("type")
        #expect(column.toSQL() == "(type)")

        let orExpr = LabelExpression.or([.single("A"), .single("B")])
        #expect(orExpr.toSQL().contains("|"))

        let andExpr = LabelExpression.and([.single("X"), .single("Y")])
        #expect(andExpr.toSQL().contains("&"))
    }

    @Test("PropertiesSpec toSQL")
    func testPropertiesSpecToSQL() throws {
        #expect(PropertiesSpec.all.toSQL() == "PROPERTIES ALL COLUMNS")
        #expect(PropertiesSpec.none.toSQL() == "NO PROPERTIES")
        #expect(PropertiesSpec.columns(["a", "b"]).toSQL() == "PROPERTIES (a, b)")
        #expect(PropertiesSpec.allExcept(["x"]).toSQL() == "PROPERTIES ALL COLUMNS EXCEPT (x)")
    }
}

// MARK: - Graph Schema Validation Tests

@Suite("Graph Schema Validation Tests")
struct GraphSchemaValidationTests {

    @Test("validate - valid schema")
    func testValidSchema() throws {
        let create = CreateGraphStatement(
            graphName: "social",
            vertexTables: [
                VertexTableDefinition(tableName: "persons", keyColumns: ["id"])
            ],
            edgeTables: [
                EdgeTableDefinition(
                    tableName: "friendships",
                    keyColumns: ["id"],
                    sourceVertex: VertexReference(tableName: "persons", keyColumns: [KeyColumnMapping(source: "p1", target: "id")]),
                    destinationVertex: VertexReference(tableName: "persons", keyColumns: [KeyColumnMapping(source: "p2", target: "id")])
                )
            ]
        )

        let errors = create.validate()
        #expect(errors.isEmpty)
    }

    @Test("validate - duplicate vertex table")
    func testDuplicateVertexTable() throws {
        let create = CreateGraphStatement(
            graphName: "test",
            vertexTables: [
                VertexTableDefinition(tableName: "persons", keyColumns: ["id"]),
                VertexTableDefinition(tableName: "persons", keyColumns: ["id"])
            ],
            edgeTables: []
        )

        let errors = create.validate()
        #expect(errors.contains(.duplicateVertexTable("persons")))
    }

    @Test("validate - duplicate vertex alias")
    func testDuplicateVertexAlias() throws {
        let create = CreateGraphStatement(
            graphName: "test",
            vertexTables: [
                VertexTableDefinition(tableName: "table1", alias: "v", keyColumns: ["id"]),
                VertexTableDefinition(tableName: "table2", alias: "v", keyColumns: ["id"])
            ],
            edgeTables: []
        )

        let errors = create.validate()
        #expect(errors.contains(.duplicateVertexTable("v")))
    }

    @Test("validate - duplicate edge table")
    func testDuplicateEdgeTable() throws {
        let create = CreateGraphStatement(
            graphName: "test",
            vertexTables: [
                VertexTableDefinition(tableName: "v", keyColumns: ["id"])
            ],
            edgeTables: [
                EdgeTableDefinition(
                    tableName: "edges",
                    keyColumns: ["id"],
                    sourceVertex: VertexReference(tableName: "v", keyColumns: []),
                    destinationVertex: VertexReference(tableName: "v", keyColumns: [])
                ),
                EdgeTableDefinition(
                    tableName: "edges",
                    keyColumns: ["id"],
                    sourceVertex: VertexReference(tableName: "v", keyColumns: []),
                    destinationVertex: VertexReference(tableName: "v", keyColumns: [])
                )
            ]
        )

        let errors = create.validate()
        #expect(errors.contains(.duplicateEdgeTable("edges")))
    }

    @Test("validate - invalid vertex reference in edge")
    func testInvalidVertexReference() throws {
        let create = CreateGraphStatement(
            graphName: "test",
            vertexTables: [
                VertexTableDefinition(tableName: "persons", keyColumns: ["id"])
            ],
            edgeTables: [
                EdgeTableDefinition(
                    tableName: "edges",
                    keyColumns: ["id"],
                    sourceVertex: VertexReference(tableName: "persons", keyColumns: []),
                    destinationVertex: VertexReference(tableName: "nonexistent", keyColumns: [])
                )
            ]
        )

        let errors = create.validate()
        #expect(errors.contains(where: {
            if case .invalidVertexReference(_, let vertex) = $0 {
                return vertex == "nonexistent"
            }
            return false
        }))
    }

    @Test("validate - empty key columns")
    func testEmptyKeyColumns() throws {
        let create = CreateGraphStatement(
            graphName: "test",
            vertexTables: [
                VertexTableDefinition(tableName: "persons", keyColumns: [])
            ],
            edgeTables: []
        )

        let errors = create.validate()
        #expect(errors.contains(.emptyKeyColumns(table: "persons")))
    }
}

// MARK: - GraphSchemaError Tests

@Suite("GraphSchemaError Tests")
struct GraphSchemaErrorTests {

    @Test("GraphSchemaError equality")
    func testEquality() throws {
        let err1 = GraphSchemaError.duplicateVertexTable("a")
        let err2 = GraphSchemaError.duplicateVertexTable("a")
        let err3 = GraphSchemaError.duplicateVertexTable("b")

        #expect(err1 == err2)
        #expect(err1 != err3)
    }

    @Test("All error types")
    func testAllErrorTypes() throws {
        let errors: [GraphSchemaError] = [
            .duplicateVertexTable("v"),
            .duplicateEdgeTable("e"),
            .invalidVertexReference(edge: "e", vertex: "v"),
            .emptyKeyColumns(table: "t"),
            .keyColumnMismatch(edge: "e", expectedCount: 2, actualCount: 1)
        ]

        // Just ensure they can be created and compared
        for error in errors {
            #expect(error == error)
        }
    }
}
