import DatabaseKit
import DatabaseEngine

/// Enumerates canonical RDF dataset indexes that can expose a named graph.
public struct RDFDatasetSourcePlanner: Sendable {
    public static func plan(
        namedGraph: RDFGraphName,
        queryContext: IndexQueryContext
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
                switch metadata.graphScope {
                case .defaultGraph:
                    continue
                case .entityField:
                    break
                case .fixed(let fixedGraph):
                    guard fixedGraph == namedGraph.term else { continue }
                }

                let typeSubspace = try await queryContext.indexSubspace(
                    forEntityName: entity.name
                )
                sources.append(
                    RDFDatasetSource(
                        entityName: entity.name,
                        indexName: descriptor.name,
                        indexSubspace: typeSubspace.subspace(descriptor.name),
                        coverage: try metadata.graphScope.sourceCoverage,
                        storedFieldNames: selection.storedFieldNames
                    )
                )
            }
        }

        return sources
    }
}
