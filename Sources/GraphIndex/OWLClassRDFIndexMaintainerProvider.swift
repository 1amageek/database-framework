import DatabaseEngine
import DatabaseKit
import StorageKit

/// Canonical runtime provider for OWL class RDF projections.
public struct OWLClassRDFIndexMaintainerProvider:
    CanonicalEntityIndexMaintainerProvider {
    public let indexType: IndexType = .graph(.ontologyProjection)
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
                $0.type == .graph(.ontologyProjection)
            }) else {
            throw IndexMaintainerProviderRegistryError.providerNotRegistered(
                indexType: .graph(.ontologyProjection),
                indexName: entity.name + "_owl_rdf"
            )
        }
        guard
            case .graph(
                .ontologyProjection(let individualIRIBase, let graph), _
            ) = descriptor.declaration.definition
        else {
            throw IndexMaintainerProviderError.typeMismatch(
                registered: .graph(.ontologyProjection),
                actual: descriptor.type
            )
        }
        self.entityName = entity.name
        self.classIRI = classIRI
        self.individualIRIBase = individualIRIBase
        self.graph = graph
        self.properties = properties
    }

    public func makeIndexMaintainer(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        wallClock: any WallClock
    ) throws -> any IndexMaintainer<PersistedModel> {
        guard
            case .graph(
                .ontologyProjection(
                    let declaredIndividualIRIBase,
                    let declaredGraph
                ), _
            ) = index.definition,
            declaredIndividualIRIBase == individualIRIBase,
            declaredGraph == graph,
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
