import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for RDF quad indexes.
public struct RDFQuadIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "rdf_quad"
    public let runtimeRequirements: IndexRuntimeRequirements = .graphQueries

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        guard index.kind.identifier == kindIdentifier else {
            throw IndexMaintainerProviderError.kindMismatch(
                registered: kindIdentifier,
                actual: index.kind.identifier
            )
        }
        guard index.kind.metadata.isEmpty else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "metadata"
            )
        }
        let fields = index.kind.fieldNames
        guard fields.count == 3 || fields.count == 4 else {
            throw IndexMaintainerProviderError.invalidMetadata(
                kindIdentifier: kindIdentifier,
                key: "fieldNames"
            )
        }

        return try RDFQuadIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            subjectField: fields[0],
            predicateField: fields[1],
            objectField: fields[2],
            graphField: fields.count == 4 ? fields[3] : nil
        )
    }
}
