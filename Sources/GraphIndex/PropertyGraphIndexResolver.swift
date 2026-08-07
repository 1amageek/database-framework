import DatabaseKit
import DatabaseEngine
import StorageKit

/// Declarative property-graph index selected from one entity's schema.
///
/// Selection is synchronous and exact. Physical subspace resolution remains
/// asynchronous because partitioned entity stores may require I/O.
package struct DeclaredPropertyGraphIndex: Sendable {
    package let entityName: String
    package let indexName: String
    package let metadata: PropertyGraphIndexMetadata
    package let storedFieldNames: [String]
}

/// A declared property-graph index bound to its physical index subspace.
package struct ResolvedPropertyGraphIndex: Sendable {
    package let declaration: DeclaredPropertyGraphIndex
    package let indexSubspace: Subspace

    package var indexName: String { declaration.indexName }
    package var metadata: PropertyGraphIndexMetadata { declaration.metadata }
    package var storedFieldNames: [String] { declaration.storedFieldNames }

    package func scanner(
        snapshot: GraphReadSnapshot,
        graphTarget: GraphScanTarget = .all
    ) -> GraphEdgeScanner {
        GraphEdgeScanner(
            indexSubspace: indexSubspace,
            strategy: metadata.strategy,
            graphTarget: graphTarget,
            snapshot: snapshot
        )
    }
}

package struct PropertyGraphIndexSignature: Sendable, Hashable {
    package let sourceFieldName: String
    package let labelFieldName: String
    package let targetFieldName: String
    package let namespaceFieldName: String?

    package init(
        sourceFieldName: String,
        labelFieldName: String,
        targetFieldName: String,
        namespaceFieldName: String? = nil
    ) {
        self.sourceFieldName = sourceFieldName
        self.labelFieldName = labelFieldName
        self.targetFieldName = targetFieldName
        self.namespaceFieldName = namespaceFieldName
    }
}

package enum PropertyGraphIndexResolver {
    package static func unique<T: Persistable>(
        for type: T.Type,
        in context: IndexQueryContext
    ) throws -> DeclaredPropertyGraphIndex {
        let candidates = try declarations(for: type, in: context)
        return try requireUnique(
            candidates,
            entityName: T.persistableType,
            selector: "the default property-graph index"
        )
    }

    package static func exact<T: Persistable>(
        named indexName: String,
        for type: T.Type,
        in context: IndexQueryContext
    ) throws -> DeclaredPropertyGraphIndex {
        let candidates = try declarations(for: type, in: context)
            .filter { $0.indexName == indexName }
        return try requireUnique(
            candidates,
            entityName: T.persistableType,
            selector: "index named \(indexName)"
        )
    }

    package static func exact<T: Persistable>(
        signature: PropertyGraphIndexSignature,
        for type: T.Type,
        in context: IndexQueryContext
    ) throws -> DeclaredPropertyGraphIndex {
        let candidates = try declarations(for: type, in: context).filter { candidate in
            let metadata = candidate.metadata
            return metadata.sourceFieldName == signature.sourceFieldName
                && metadata.labelFieldName == signature.labelFieldName
                && metadata.targetFieldName == signature.targetFieldName
                && metadata.namespaceFieldName == signature.namespaceFieldName
        }
        return try requireUnique(
            candidates,
            entityName: T.persistableType,
            selector: "property-graph field signature"
        )
    }

    package static func resolve<T: Persistable>(
        _ declaration: DeclaredPropertyGraphIndex,
        for type: T.Type,
        in context: IndexQueryContext,
        transaction: any TransactionAccess
    ) async throws -> ResolvedPropertyGraphIndex? {
        guard declaration.entityName == T.persistableType else {
            throw PropertyGraphIndexResolutionError.entityOwnershipMismatch(
                indexName: declaration.indexName,
                expectedEntity: T.persistableType,
                actualEntity: declaration.entityName
            )
        }
        guard let readableIndex = try await context.readableIndex(
            named: declaration.indexName,
            kindIdentifier: "graph",
            for: type,
            transaction: transaction
        ) else {
            return nil
        }
        return ResolvedPropertyGraphIndex(
            declaration: declaration,
            indexSubspace: readableIndex.subspace
        )
    }

    private static func declarations<T: Persistable>(
        for type: T.Type,
        in context: IndexQueryContext
    ) throws -> [DeclaredPropertyGraphIndex] {
        try context.findIndexes(
            for: type,
            kindIdentifier: "graph"
        ).map { descriptor in
            DeclaredPropertyGraphIndex(
                entityName: T.persistableType,
                indexName: descriptor.name,
                metadata: try PropertyGraphIndexMetadata(canonical: descriptor.kind),
                storedFieldNames: descriptor.storedFieldNames
            )
        }
    }

    private static func requireUnique(
        _ candidates: [DeclaredPropertyGraphIndex],
        entityName: String,
        selector: String
    ) throws -> DeclaredPropertyGraphIndex {
        guard let candidate = candidates.first else {
            throw PropertyGraphIndexResolutionError.notFound(
                entityName: entityName,
                selector: selector
            )
        }
        guard candidates.count == 1 else {
            throw PropertyGraphIndexResolutionError.ambiguous(
                entityName: entityName,
                selector: selector,
                indexNames: candidates.map { $0.indexName }.sorted()
            )
        }
        return candidate
    }
}

public enum PropertyGraphIndexResolutionError: Error, Sendable, CustomStringConvertible {
    case notFound(entityName: String, selector: String)
    case ambiguous(entityName: String, selector: String, indexNames: [String])
    case entityOwnershipMismatch(
        indexName: String,
        expectedEntity: String,
        actualEntity: String
    )

    public var description: String {
        switch self {
        case .notFound(let entityName, let selector):
            return "No \(selector) is declared by entity \(entityName)"
        case .ambiguous(let entityName, let selector, let indexNames):
            return "Entity \(entityName) has ambiguous \(selector): \(indexNames.joined(separator: ", "))"
        case .entityOwnershipMismatch(let indexName, let expectedEntity, let actualEntity):
            return "Index \(indexName) belongs to \(actualEntity), not \(expectedEntity)"
        }
    }
}
