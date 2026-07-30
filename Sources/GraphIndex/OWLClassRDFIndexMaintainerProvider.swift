import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for OWL class RDF projections.
public struct OWLClassRDFIndexMaintainerProvider<Root: OWLClassEntity>:
    EntityIndexMaintainerProvider {
    public typealias Model = Root
    public let kindIdentifier = "owl_class_rdf"
    public let runtimeRequirements: IndexRuntimeRequirements = .graphQueries

    public init() {}

    public func makeIndexMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<Root> {
        let metadata = try OWLClassRDFIndexMetadata(canonical: index.kind)
        guard metadata.individualIRIBase == Root.ontologyIndividualIRIBase,
              metadata.graph == Root.ontologyGraph else {
            throw OWLClassRDFIndexError.projectionConfigurationMismatch(
                typeName: Root.persistableType
            )
        }
        return OWLClassRDFIndexMaintainer<Root>(subspace: subspace)
    }
}
