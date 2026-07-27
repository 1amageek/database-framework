import QueryAST
import DatabaseKit
import Testing

@Suite("SQL property graph definition parser")
struct SQLParserGraphDefinitionTests {
    @Test("CREATE PROPERTY GRAPH preserves the complete graph schema")
    func createPropertyGraph() throws {
        let statement = try SQLParser().parse("""
            CREATE PROPERTY GRAPH IF NOT EXISTS social
            VERTEX TABLES (
                persons AS person KEY (id, tenant_id)
                    LABEL Person
                    PROPERTIES ALL COLUMNS EXCEPT (secret),
                companies KEY (id, tenant_id)
                    LABEL (kind)
                    PROPERTIES (name)
            )
            EDGE TABLES (
                works_at AS relationship KEY (id)
                    SOURCE KEY (person_id, tenant_id)
                        REFERENCES persons (id, tenant_id)
                    DESTINATION KEY (company_id, tenant_id)
                        REFERENCES companies (id, tenant_id)
                    LABEL (WORKS_AT | CONTRACTOR)
                    NO PROPERTIES
            )
            """)

        guard case .createGraph(let graph) = statement else {
            Issue.record("Expected CREATE PROPERTY GRAPH")
            return
        }
        #expect(graph.graphName == "social")
        #expect(graph.ifNotExists)
        #expect(graph.vertexTables.count == 2)
        #expect(graph.vertexTables[0].alias == "person")
        #expect(graph.vertexTables[0].keyColumns == ["id", "tenant_id"])
        #expect(graph.vertexTables[0].propertiesSpec == .allExcept(["secret"]))
        #expect(graph.vertexTables[1].labelExpression == .column("kind"))
        #expect(graph.vertexTables[1].propertiesSpec == .columns(["name"]))

        #expect(graph.edgeTables.count == 1)
        let edge = graph.edgeTables[0]
        #expect(edge.alias == "relationship")
        #expect(edge.sourceVertex.tableName == "persons")
        #expect(edge.sourceVertex.keyColumns == [
            KeyColumnMapping(source: "person_id", target: "id"),
            KeyColumnMapping(source: "tenant_id", target: "tenant_id"),
        ])
        #expect(edge.destinationVertex.tableName == "companies")
        #expect(edge.labelExpression == .or([
            .single("WORKS_AT"),
            .single("CONTRACTOR"),
        ]))
        #expect(edge.propertiesSpec == PropertiesSpec.none)
    }

    @Test("DROP PROPERTY GRAPH produces the canonical statement")
    func dropPropertyGraph() throws {
        let statement = try SQLParser().parse("DROP PROPERTY GRAPH social;")
        #expect(statement == .dropGraph("social"))
    }

    @Test("Invalid graph definitions fail instead of producing partial schemas")
    func invalidGraphDefinitionsFail() {
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse(
                "CREATE PROPERTY GRAPH empty VERTEX TABLES ()"
            )
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("""
                CREATE PROPERTY GRAPH broken
                VERTEX TABLES (person KEY (id))
                EDGE TABLES (
                    follows KEY (id)
                    SOURCE KEY (source_id, tenant_id) REFERENCES person (id)
                    DESTINATION KEY (target_id) REFERENCES person (id)
                )
                """)
        }
        #expect(throws: SQLParser.ParseError.self) {
            _ = try SQLParser().parse("""
                CREATE PROPERTY GRAPH broken
                VERTEX TABLES (person KEY (id))
                EDGE TABLES ()
                """)
        }
    }

    @Test("Graph definition collections use the shared structural ledger")
    func graphDefinitionCollectionLimit() {
        #expect(throws: QueryStructuralValidationError.self) {
            _ = try SQLParser(
                structuralLimits: QueryStructuralLimits(
                    maximumCollectionElements: 2
                )
            ).parse("""
                CREATE PROPERTY GRAPH social
                VERTEX TABLES (person KEY (id, tenant_id))
                """)
        }
    }
}
