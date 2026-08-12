import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire
import DatabaseKit
import GraphIndex
import StorageKit

public struct ResolvedDatabaseGraphSource: Sendable {
    public enum Layout: Sendable {
        case propertyGraph(PropertyGraphLayout)
        case rdf(RDFLayout)
    }

    public enum PropertyGraphTarget: Sendable, Equatable {
        case all
        case defaultGraph
        case named(String)
    }

    public struct PropertyGraphLayout: Sendable {
        public let strategy: PropertyGraphIndexStrategy
        public let graphTarget: PropertyGraphTarget
        public let edgeLabel: String?

        public init(
            strategy: PropertyGraphIndexStrategy,
            graphTarget: PropertyGraphTarget,
            edgeLabel: String?
        ) {
            self.strategy = strategy
            self.graphTarget = graphTarget
            self.edgeLabel = edgeLabel
        }

        package var scannerGraphTarget: GraphScanTarget {
            switch graphTarget {
            case .all:
                return .all
            case .defaultGraph:
                return .defaultGraph
            case .named(let name):
                return .named(.identifier(name))
            }
        }

        package var scannerEdgeLabel: GraphIdentity? {
            edgeLabel.map(GraphIdentity.identifier)
        }
    }

    public enum RDFGraphTarget: Sendable, Equatable {
        case all
        case defaultGraph
        case named(RDFTerm)
    }

    public struct RDFLayout: Sendable {
        public let graphTarget: RDFGraphTarget
        public let predicate: RDFTerm?
        package let scannerGraphTarget: GraphScanTarget
        package let scannerEdgeLabel: GraphIdentity?

        public init(
            graphTarget: RDFGraphTarget,
            predicate: RDFTerm?
        ) throws {
            switch graphTarget {
            case .all:
                self.scannerGraphTarget = .all
            case .defaultGraph:
                self.scannerGraphTarget = .defaultGraph
            case .named(let graph):
                do {
                    _ = try RDFGraphName(graph)
                } catch {
                    throw DatabaseGraphAlgorithmError.invalidRDFGraphName(.rdf(graph))
                }
                self.scannerGraphTarget = .named(try .rdf(graph))
            }
            if let predicate {
                guard case .iri = predicate else {
                    throw DatabaseGraphAlgorithmError.invalidRDFPredicate(.rdf(predicate))
                }
                self.scannerEdgeLabel = try .rdf(predicate)
            } else {
                self.scannerEdgeLabel = nil
            }
            self.graphTarget = graphTarget
            self.predicate = predicate
        }
    }

    public let entityName: String
    public let indexName: String
    public let indexSubspace: Subspace
    public let storedFieldNames: [String]
    public let layout: Layout

    public init(
        entityName: String,
        indexName: String,
        indexSubspace: Subspace,
        storedFieldNames: [String],
        layout: Layout
    ) {
        self.entityName = entityName
        self.indexName = indexName
        self.indexSubspace = indexSubspace
        self.storedFieldNames = storedFieldNames
        self.layout = layout
    }

    package var strategy: GraphIndexStrategy {
        switch layout {
        case .propertyGraph(let layout):
            return layout.strategy.storageStrategy
        case .rdf:
            return .quadStore
        }
    }

    package var graphTarget: GraphScanTarget {
        switch layout {
        case .propertyGraph(let layout):
            return layout.scannerGraphTarget
        case .rdf(let layout):
            return layout.scannerGraphTarget
        }
    }

    package var edgeLabel: GraphIdentity? {
        switch layout {
        case .propertyGraph(let layout):
            return layout.scannerEdgeLabel
        case .rdf(let layout):
            return layout.scannerEdgeLabel
        }
    }

    public func encodeVertex(_ term: GraphAlgorithmOperation.Term) throws -> GraphIdentity {
        switch (layout, term) {
        case (.propertyGraph, .identifier(let value)):
            return .identifier(value)
        case (.rdf, .rdf(let value)):
            return try .rdf(value)
        case (.propertyGraph, _):
            throw DatabaseGraphAlgorithmError.expectedPropertyGraphIdentifier(term)
        case (.rdf, _):
            throw DatabaseGraphAlgorithmError.expectedRDFTerm(term)
        }
    }

    public func decodeVertex(_ value: GraphIdentity) throws -> GraphAlgorithmOperation.Term {
        switch (layout, value.representation) {
        case (.propertyGraph, .propertyGraph):
            guard let identifier = value.identifier else {
                throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                    "property-graph identity lost its identifier"
                )
            }
            return .identifier(identifier)
        case (.rdf, .rdf):
            guard let term = try value.decodeRDFTerm() else {
                throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                    "RDF identity lost its canonical bytes"
                )
            }
            return .rdf(term)
        case (.propertyGraph, .rdf):
            guard let term = try value.decodeRDFTerm() else {
                throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                    "RDF identity lost its canonical bytes"
                )
            }
            throw DatabaseGraphAlgorithmError.expectedPropertyGraphIdentifier(
                .rdf(term)
            )
        case (.rdf, .propertyGraph):
            guard let identifier = value.identifier else {
                throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                    "property-graph identity lost its identifier"
                )
            }
            throw DatabaseGraphAlgorithmError.expectedRDFTerm(
                .identifier(identifier)
            )
        }
    }

    public func decodeEdgeLabel(_ value: GraphIdentity) throws -> GraphAlgorithmOperation.Term {
        try decodeVertex(value)
    }
}
#endif
