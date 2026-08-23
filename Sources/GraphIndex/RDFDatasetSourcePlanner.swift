import DatabaseEngine
import DatabaseKit
import StorageKit

/// Enumerates canonical RDF dataset indexes that can expose a named graph.
public struct RDFDatasetSourcePlanner: Sendable {
    /// Authorizes the complete logical dataset before a transaction resolves
    /// any namespace or observes whether a physical index exists.
    static func preflightAuthorization(
        namedGraph: RDFGraphName,
        queryContext: IndexQueryContext,
        authorization: IndexReadAuthorization
    ) throws {
        for candidate in try candidates(
            namedGraph: namedGraph,
            queryContext: queryContext
        ) {
            try queryContext.authorizeIndexRead(
                entityName: candidate.entityName,
                descriptor: candidate.descriptor,
                authorization: authorization
            )
        }
    }

    public static func plan(
        namedGraph: RDFGraphName,
        queryContext: IndexQueryContext,
        authorization: IndexReadAuthorization,
        transaction: any TransactionReadAccess
    ) async throws -> [RDFDatasetSource] {
        let candidates = try candidates(
            namedGraph: namedGraph,
            queryContext: queryContext
        )
        for candidate in candidates {
            try queryContext.authorizeIndexRead(
                entityName: candidate.entityName,
                descriptor: candidate.descriptor,
                authorization: authorization
            )
        }

        var sources: [RDFDatasetSource] = []
        for candidate in candidates {
            guard let readableIndex = try await queryContext.readableIndex(
                named: candidate.descriptor.name,
                indexType: candidate.descriptor.type,
                forEntityName: candidate.entityName,
                partitions: FieldObject(),
                authorization: authorization,
                transaction: transaction
            ) else {
                continue
            }
            sources.append(
                RDFDatasetSource(
                    entityName: candidate.entityName,
                    indexName: candidate.descriptor.name,
                    indexSubspace: readableIndex.subspace,
                    coverage: try candidate.selection.metadata.graphMapping
                        .sourceCoverage,
                    includedFieldNames: candidate.selection.includedFieldNames
                )
            )
        }

        return sources
    }

    package static func withPlannedSources<Result: Sendable>(
        namedGraph: RDFGraphName,
        queryContext: IndexQueryContext,
        authorization: IndexReadAuthorization,
        snapshot: any IndexQuerySnapshotAccess,
        _ operation: @Sendable @escaping (
            [RDFDatasetSource],
            any IndexQueryReadAccess
        ) async throws -> Result
    ) async throws -> Result {
        let candidates = try candidates(
            namedGraph: namedGraph,
            queryContext: queryContext
        )
        let requests = candidates.map { candidate in
            IndexReadRequest(
                indexName: candidate.descriptor.name,
                indexType: candidate.descriptor.type,
                entityName: candidate.entityName,
                authorization: authorization
            )
        }
        return try await snapshot.withReadableIndexes(requests) {
            readableIndexes, access in
            var sources: [RDFDatasetSource] = []
            sources.reserveCapacity(readableIndexes.count)
            for index in candidates.indices {
                guard let readableIndex = readableIndexes[index] else {
                    continue
                }
                let candidate = candidates[index]
                sources.append(
                    RDFDatasetSource(
                        entityName: candidate.entityName,
                        indexName: candidate.descriptor.name,
                        indexSubspace: readableIndex.subspace,
                        coverage: try candidate.selection.metadata.graphMapping
                            .sourceCoverage,
                        includedFieldNames: candidate.selection
                            .includedFieldNames
                    )
                )
            }
            return try await operation(sources, access)
        }
    }

    private struct Candidate: Sendable {
        let entityName: String
        let descriptor: IndexDescriptor
        let selection: RDFDatasetIndexSelection
    }

    private static func candidates(
        namedGraph: RDFGraphName,
        queryContext: IndexQueryContext
    ) throws -> [Candidate] {
        var result: [Candidate] = []
        for entity in queryContext.schema.entities {
            for descriptor in entity.indexDescriptors {
                guard let selection = try RDFDatasetIndexSelection(
                    descriptor: descriptor
                ) else {
                    continue
                }
                switch selection.metadata.graphMapping {
                case .defaultGraph:
                    continue
                case .entityField:
                    break
                case .fixed(let fixedGraph):
                    guard fixedGraph == namedGraph.term else { continue }
                }
                result.append(
                    Candidate(
                        entityName: entity.name,
                        descriptor: descriptor,
                        selection: selection
                    )
                )
            }
        }
        return result
    }
}
