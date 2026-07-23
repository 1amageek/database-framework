import Core
import DatabaseEngine
import Graph
import StorageKit

/// Canonical runtime provider for OWL class RDF projections.
public struct OWLClassRDFIndexMaintainerProvider: IndexMaintainerProvider {
    public let kindIdentifier = "owl_class_rdf"
    public let runtimeRequirements: IndexRuntimeRequirements = .graphQueries

    public init() {}

    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) throws -> any IndexMaintainer<Item> {
        let metadata = try OWLClassRDFIndexMetadata(canonical: index.kind)
        guard let root = Item.self as? any OWLClassEntity.Type else {
            throw OWLClassRDFIndexError.rootDoesNotConform(
                typeName: Item.persistableType
            )
        }
        guard metadata.individualIRIBase == root.ontologyIndividualIRIBase,
              metadata.graph == root.ontologyGraph else {
            throw OWLClassRDFIndexError.projectionConfigurationMismatch(
                typeName: Item.persistableType
            )
        }
        return OWLClassRDFIndexMaintainer<Item>(subspace: subspace)
    }
}
