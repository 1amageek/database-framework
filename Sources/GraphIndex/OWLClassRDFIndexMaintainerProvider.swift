import DatabaseKit
import DatabaseEngine
import StorageKit

/// Canonical runtime provider for OWL class RDF projections.
public struct OWLClassRDFIndexMaintainerProvider:
    CanonicalEntityIndexMaintainerProvider {
    public let kindIdentifier = "owl_class_rdf"
    public let runtimeRequirements: IndexRuntimeRequirements = .graphQueries

    private let entityName: String
    private let classIRI: String
    private let individualIRIBase: String
    private let graph: RDFGraphName?
    private let properties: [OWLDataPropertyDescriptor]

    public init(entity: Schema.Entity) throws {
        guard case .owlClass(let classIRI, let properties) = entity.ontology else {
            throw OWLClassRDFIndexError.missingOWLClassBinding(entity: entity.name)
        }
        guard let descriptor = entity.indexDescriptors.first(where: {
            $0.kindIdentifier == "owl_class_rdf"
        }) else {
            throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                kindIdentifier: "owl_class_rdf",
                indexName: entity.name + "_owl_rdf"
            )
        }
        let metadata = try OWLClassRDFIndexMetadata(canonical: descriptor.kind)
        self.entityName = entity.name
        self.classIRI = classIRI
        self.individualIRIBase = metadata.individualIRIBase
        self.graph = metadata.graph
        self.properties = properties
    }

    public func makeIndexMaintainer(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<PersistedModel> {
        let metadata = try OWLClassRDFIndexMetadata(canonical: index.kind)
        guard metadata.individualIRIBase == individualIRIBase,
              metadata.graph == graph,
              index.itemTypes == Set([entityName]) else {
            throw OWLClassRDFIndexError.projectionConfigurationMismatch(
                typeName: entityName
            )
        }
        return OWLClassRDFIndexMaintainer(
            subspace: subspace,
            entityName: entityName,
            classIRI: classIRI,
            individualIRIBase: individualIRIBase,
            graph: graph,
            properties: properties
        )
    }
}
