import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for RDF quad indexes.
public struct RDFQuadIndexMaintainerProvider: IndexMaintainerProvider {
    public let indexType: IndexType = .graph(.rdf)
    public let runtimeRequirements: IndexRuntimeRequirements = .graphQueries

    public init() {}

    public func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Item> {
        guard
            case .graph(
                .rdf(let subject, let predicate, let object, let graph), _
            ) = index.definition
        else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: indexType,
                actual: index.type
            )
        }

        return try RDFQuadIndexMaintainer<Item>(
            index: index,
            subspace: subspace,
            idExpression: idExpression,
            subjectField: subject.name,
            predicateField: predicate.name,
            objectField: object.name,
            graphField: graph?.name
        )
    }
}
