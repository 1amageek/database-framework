import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine
@testable import GraphIndex

@Persistable
private struct PartitionedRDFStatement {
    #Directory<PartitionedRDFStatement>(
        "sparql",
        "partitioned",
        \PartitionedRDFStatement.tenantID
    )
    #Index(
        .graph(
            name: "partitioned_rdf",
            definition: .rdf(
                subject: \PartitionedRDFStatement.subject,
                predicate: \PartitionedRDFStatement.predicate,
                object: \PartitionedRDFStatement.object, graph: nil)))

    var id: String = ""
    var tenantID: String = ""
    var subject: RDFTerm = .iri(.xsdString)
    var predicate: RDFTerm = .iri(.xsdString)
    var object: RDFTerm = .iri(.xsdString)
}

@Suite("SPARQL missing partition semantics")
struct SPARQLMissingPartitionSemanticsTests {
    @Test("A missing partition remains an empty RDF dataset")
    func missingPartitionExecutesSPARQLAlgebra() async throws {
        let container = try await DBContainer.open(
            for: try Schema(
                entities: [try PartitionedRDFStatement.schemaEntity]
            ),
            configuration: .testing(
                storageEngine: InMemoryEngine()
            ),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        PartitionedRDFStatement.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let query = try container.testBaseContext()
            .sparql(PartitionedRDFStatement.self)
            .partition(
                PartitionedRDFStatement.fields.tenantID,
                equals: "missing"
            )
            .defaultIndex()

        let emptyPatternResult = try await query.execute()
        #expect(emptyPatternResult.count == 1)
        #expect(emptyPatternResult.first?.isEmpty == true)

        let triplePatternQuery = query.where(
            "?subject",
            "?predicate",
            "?object"
        )
        let triplePatternResult = try await triplePatternQuery.execute()
        #expect(triplePatternResult.isEmpty)

        let aggregateResult = try await triplePatternQuery
            .groupBy([])
            .countAll(as: "count")
            .execute()
        #expect(aggregateResult.count == 1)
        #expect(aggregateResult.firstNumericAggregate("count") == 0)

        let partitions = try await container.withTestBaseOperation {
            try await container.partitionCatalogPage(
                entity: PartitionedRDFStatement.persistableType,
                limit: 1
            )
        }
        #expect(partitions.entries.isEmpty)
    }
}
