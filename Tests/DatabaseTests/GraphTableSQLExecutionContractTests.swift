import Database
import DatabaseKit
import DatabaseTypes
import GraphIndex
import StorageKit
import Testing

@Persistable(type: "GraphTableSQLEdge")
private struct GraphTableSQLEdge {
    #Directory<GraphTableSQLEdge>("graph_table_sql_edges")
    #Index(
        .graph(
            name: "graph_table_sql_index",
            definition: .property(
                source: \GraphTableSQLEdge.source,
                label: .field(\GraphTableSQLEdge.label),
                target: \GraphTableSQLEdge.target,
                graph: nil,
                strategy: .tripleStore
            ),
            includedFields: [\GraphTableSQLEdge.weight]
        )
    )

    var id: String = ""
    var source: String = ""
    var label: String = ""
    var target: String = ""
    var weight: Int64 = 0
}

@Suite("GRAPH_TABLE SQL execution contract")
struct GraphTableSQLExecutionContractTests {
    @Test("SQL text reaches the canonical property-graph executor")
    func sqlTextReachesCanonicalPropertyGraphExecutor() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        try context.insert(
            GraphTableSQLEdge(
                id: "knows-edge",
                source: "alice",
                label: "KNOWS",
                target: "bob",
                weight: 7
            )
        )
        try context.insert(
            GraphTableSQLEdge(
                id: "blocks-edge",
                source: "alice",
                label: "BLOCKS",
                target: "carol",
                weight: 11
            )
        )
        try await context.save()

        let response = try await context.executeSQL(
            """
            SELECT *
            FROM GRAPH_TABLE(
                graph_table_sql_index,
                MATCH (source)-[relation:KNOWS]->(target)
                COLUMNS (
                    source.id AS source_id,
                    target.id AS target_id,
                    relation.weight AS relationship_weight
                )
            )
            """
        )

        let row = try #require(response.rows.first)
        #expect(response.rows.count == 1)
        #expect(row.fields["source_id"] == .string("alice"))
        #expect(row.fields["target_id"] == .string("bob"))
        #expect(row.fields["relationship_weight"] == .int64(7))
    }

    private func makeContainer() async throws -> DBContainer {
        let provider = GraphIndexMaintainerProvider()
        var runtime = try EntityRuntimeDefinition(GraphTableSQLEdge.self)
        try runtime.register(provider)
        return try await DBContainer.open(
            for: try Schema(
                entities: [try GraphTableSQLEdge.schemaEntity]
            ),
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseRuntimeConfiguration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "graph-table-sql-contract-tests",
                    revision: 1
                ),
                indexMaintainerProviderDescriptors: [
                    .init(describing: provider)
                ],
                graphTableSourceExecutor: GraphTableReadExecutors
                    .sourceExecutor,
                sparqlSourceExecutor: SPARQLReadExecutors.sourceExecutor(
                    functionRegistry: .empty
                ),
                entityRuntimes: [runtime.registration()]
            ),
            security: .testingDisabled
        )
    }
}
