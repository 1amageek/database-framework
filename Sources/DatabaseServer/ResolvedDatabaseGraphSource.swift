import DatabaseValue
import DatabaseWire
import Graph
import GraphIndex
import StorageKit

public struct ResolvedDatabaseGraphSource: Sendable {
    public enum Layout: Sendable {
        case propertyGraph(PropertyGraphLayout)
        case rdf(RDFLayout)
    }

    public enum PropertyGraphScope: Sendable, Equatable {
        case all
        case defaultGraph
        case named(String)
    }

    public struct PropertyGraphLayout: Sendable {
        public let strategy: PropertyGraphIndexStrategy
        public let scope: PropertyGraphScope
        public let edgeLabel: String?

        public init(
            strategy: PropertyGraphIndexStrategy,
            scope: PropertyGraphScope,
            edgeLabel: String?
        ) {
            self.strategy = strategy
            self.scope = scope
            self.edgeLabel = edgeLabel
        }

        package var scannerScope: GraphScanScope {
            switch scope {
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

    public enum RDFScope: Sendable, Equatable {
        case all
        case defaultGraph
        case named(DatabaseRDFTerm)
    }

    public struct RDFLayout: Sendable {
        public let scope: RDFScope
        public let predicate: DatabaseRDFTerm?
        package let scannerScope: GraphScanScope
        package let scannerEdgeLabel: GraphIdentity?

        public init(
            scope: RDFScope,
            predicate: DatabaseRDFTerm?
        ) throws {
            switch scope {
            case .all:
                self.scannerScope = .all
            case .defaultGraph:
                self.scannerScope = .defaultGraph
            case .named(let graph):
                guard graph.isRDFGraphName else {
                    throw DatabaseGraphAlgorithmError.invalidRDFGraphName(.rdf(graph))
                }
                self.scannerScope = .named(try .rdf(graph))
            }
            if let predicate {
                guard predicate.isRDFPredicate else {
                    throw DatabaseGraphAlgorithmError.invalidRDFPredicate(.rdf(predicate))
                }
                self.scannerEdgeLabel = try .rdf(predicate)
            } else {
                self.scannerEdgeLabel = nil
            }
            self.scope = scope
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

    package var scope: GraphScanScope {
        switch layout {
        case .propertyGraph(let layout):
            return layout.scannerScope
        case .rdf(let layout):
            return layout.scannerScope
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

    public func encodeVertex(_ term: DatabaseGraphTerm) throws -> GraphIdentity {
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

    public func decodeVertex(_ value: GraphIdentity) throws -> DatabaseGraphTerm {
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

    public func decodeEdgeLabel(_ value: GraphIdentity) throws -> DatabaseGraphTerm {
        try decodeVertex(value)
    }
}
