#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseKit
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import GraphIndex
import StorageKit

public struct SchemaDatabaseGraphSourceResolver: DatabaseGraphSourceResolving {
    private struct OwnedIndex {
        let entity: Schema.Entity
        let descriptor: IndexDescriptor
    }

    private let container: DBContainer

    public init(container: DBContainer) {
        self.container = container
    }

    public func resolve(
        _ source: GraphAlgorithmOperation.Source,
        transaction: any TransactionAccess
    ) async throws -> ResolvedDatabaseGraphSource {
        let candidates = container.schema.entities.flatMap { entity in
            entity.indexDescriptors
                .filter { $0.name == source.index }
                .map { OwnedIndex(entity: entity, descriptor: $0) }
        }
        guard !candidates.isEmpty else {
            throw DatabaseGraphAlgorithmError.sourceIndexNotFound(source.index)
        }
        guard candidates.count == 1, let owned = candidates.first else {
            throw DatabaseGraphAlgorithmError.sourceIndexHasNoUniqueOwner(source.index)
        }

        let queryContext = IndexQueryContext(context: container.newContext())
        guard let readableIndex = try await queryContext.readableIndex(
            named: owned.descriptor.name,
            kindIdentifier: owned.descriptor.kindIdentifier,
            forEntityName: owned.entity.name,
            partitions: source.partitions,
            transaction: transaction
        ) else {
            throw DatabaseGraphAlgorithmError.sourcePartitionNotFound(
                index: source.index,
                entity: owned.entity.name
            )
        }
        let indexSubspace = readableIndex.subspace

        if owned.descriptor.kindIdentifier == "graph" {
            return try propertyGraphSource(
                source,
                owned: owned,
                indexSubspace: indexSubspace,
                metadata: PropertyGraphIndexMetadata(
                    canonical: owned.descriptor.kind
                )
            )
        }
        if let selection = try RDFDatasetIndexSelection(
            descriptor: owned.descriptor
        ) {
            return try rdfSource(
                source,
                owned: owned,
                indexSubspace: indexSubspace,
                metadata: selection.metadata
            )
        }
        throw DatabaseGraphAlgorithmError.unsupportedSourceIndex(
            index: source.index,
            kind: owned.descriptor.kindIdentifier
        )
    }

    private func propertyGraphSource(
        _ source: GraphAlgorithmOperation.Source,
        owned: OwnedIndex,
        indexSubspace: StorageKit.Subspace,
        metadata: PropertyGraphIndexMetadata
    ) throws -> ResolvedDatabaseGraphSource {
        let graphTarget: ResolvedDatabaseGraphSource.PropertyGraphTarget
        switch source.graph {
        case .all:
            graphTarget = .all
        case .defaultGraph:
            graphTarget = .defaultGraph
        case .named(.identifier(let name)):
            guard metadata.namespaceFieldName != nil else {
                throw DatabaseGraphAlgorithmError
                    .propertyGraphSourceDoesNotCoverNamedGraph(
                        index: source.index
                    )
            }
            graphTarget = .named(name)
        case .named(let term):
            throw DatabaseGraphAlgorithmError.expectedPropertyGraphIdentifier(term)
        }
        let edgeLabel: String?
        switch source.edgeLabel {
        case nil:
            edgeLabel = nil
        case .identifier(let value):
            edgeLabel = value
        case .some(let term):
            throw DatabaseGraphAlgorithmError.expectedPropertyGraphIdentifier(term)
        }
        return ResolvedDatabaseGraphSource(
            entityName: owned.entity.name,
            indexName: source.index,
            indexSubspace: indexSubspace,
            storedFieldNames: owned.descriptor.storedFieldNames,
            layout: .propertyGraph(
                ResolvedDatabaseGraphSource.PropertyGraphLayout(
                    strategy: metadata.declarativeStrategy,
                    graphTarget: graphTarget,
                    edgeLabel: edgeLabel
                )
            )
        )
    }

    private func rdfSource(
        _ source: GraphAlgorithmOperation.Source,
        owned: OwnedIndex,
        indexSubspace: StorageKit.Subspace,
        metadata: RDFDatasetIndexMetadata
    ) throws -> ResolvedDatabaseGraphSource {
        let graphTarget: ResolvedDatabaseGraphSource.RDFGraphTarget
        switch source.graph {
        case .all:
            graphTarget = .all
        case .defaultGraph:
            graphTarget = .defaultGraph
        case .named(.rdf(let graph)):
            graphTarget = .named(graph)
        case .named(let term):
            throw DatabaseGraphAlgorithmError.expectedRDFTerm(term)
        }
        try validateRDFCoverage(
            metadata.graphMapping,
            requestedGraphTarget: graphTarget,
            indexName: source.index
        )
        let predicate: RDFTerm?
        switch source.edgeLabel {
        case nil:
            predicate = nil
        case .rdf(.iri(let value)):
            predicate = .iri(value)
        case .some(let term):
            throw DatabaseGraphAlgorithmError.invalidRDFPredicate(term)
        }
        return ResolvedDatabaseGraphSource(
            entityName: owned.entity.name,
            indexName: source.index,
            indexSubspace: indexSubspace,
            storedFieldNames: owned.descriptor.storedFieldNames,
            layout: .rdf(
                try ResolvedDatabaseGraphSource.RDFLayout(
                    graphTarget: graphTarget,
                    predicate: predicate
                )
            )
        )
    }

    private func validateRDFCoverage(
        _ coverage: RDFDatasetGraphMapping,
        requestedGraphTarget: ResolvedDatabaseGraphSource.RDFGraphTarget,
        indexName: String
    ) throws {
        switch requestedGraphTarget {
        case .all:
            return
        case .defaultGraph:
            switch coverage {
            case .defaultGraph, .entityField:
                return
            case .fixed:
                throw DatabaseGraphAlgorithmError
                    .rdfSourceDoesNotCoverDefaultGraph(index: indexName)
            }
        case .named(let requestedGraph):
            switch coverage {
            case .entityField:
                return
            case .fixed(let fixedGraph) where fixedGraph == requestedGraph:
                return
            case .defaultGraph, .fixed:
                throw DatabaseGraphAlgorithmError
                    .rdfSourceDoesNotCoverNamedGraph(
                        index: indexName,
                        graph: requestedGraph
                    )
            }
        }
    }
}
#endif
