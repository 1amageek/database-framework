import DatabaseKit
import DatabaseEngine
import StorageKit

/// Enumerates canonical RDF dataset indexes that can expose a named graph.
public struct RDFDatasetSourcePlanner: Sendable {
    public static func plan(
        namedGraph: RDFGraphName,
        queryContext: IndexQueryContext,
        transaction: any TransactionAccess
    ) async throws -> [RDFDatasetSource] {
        var sources: [RDFDatasetSource] = []

        for entity in queryContext.schema.entities {
            for descriptor in entity.indexDescriptors {
                guard let selection = try RDFDatasetIndexSelection(
                    descriptor: descriptor
                ) else {
                    continue
                }
                let metadata = selection.metadata
                switch metadata.graphMapping {
                case .defaultGraph:
                    continue
                case .entityField:
                    break
                case .fixed(let fixedGraph):
                    guard fixedGraph == namedGraph.term else { continue }
                }

                guard let readableIndex = try await queryContext.readableIndex(
                    named: descriptor.name,
                    kindIdentifier: descriptor.kindIdentifier,
                    forEntityName: entity.name,
                    partitions: FieldObject(),
                    transaction: transaction
                ) else {
                    continue
                }
                sources.append(
                    RDFDatasetSource(
                        entityName: entity.name,
                        indexName: descriptor.name,
                        indexSubspace: readableIndex.subspace,
                        coverage: try metadata.graphMapping.sourceCoverage,
                        storedFieldNames: selection.storedFieldNames
                    )
                )
            }
        }

        return sources
    }
}
