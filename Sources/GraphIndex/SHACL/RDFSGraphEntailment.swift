import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import OntologyIndex
import StorageKit

/// Computes the finite RDFS closure needed to validate one selected data graph.
public struct RDFSGraphEntailment: SHACLEntailmentContext, Sendable {
    private let storage: RDFSGraphEntailmentStorage

    public var ontologyContext: OntologyContext {
        storage.ontologyContext
    }

    public static func resolve(
        executor: SPARQLQueryExecutor,
        dataGraph: SHACLDataGraphTarget,
        transaction: any TransactionReadAccess,
        budget: SHACLValidationWorkBudget
    ) async throws -> Self {
        let graphTarget: RDFGraphScanTarget
        switch dataGraph {
        case .defaultGraph:
            graphTarget = .defaultGraph
        case .named(let graph):
            graphTarget = .named(graph)
        }
        let scan = try await executor.scanDatasetInTransaction(
            graphTarget: graphTarget,
            transaction: transaction,
            workMeter: budget.workMeter
        )
        guard scan.workMeter === budget.workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        var builder = try RDFSGraphEntailmentStorage.Builder(
            workMeter: budget.workMeter
        )
        for index in 0..<scan.count {
            try scan.withQuad(at: index) { quad in
                try builder.ingest(quad, budget: budget)
            }
        }
        return RDFSGraphEntailment(
            storage: try builder.finish(budget: budget)
        )
    }

    public init(
        quads: [RDFQuad],
        budget: SHACLValidationWorkBudget
    ) throws {
        var builder = try RDFSGraphEntailmentStorage.Builder(
            workMeter: budget.workMeter
        )
        for quad in quads {
            try builder.ingest(quad, budget: budget)
        }
        self.storage = try builder.finish(budget: budget)
    }

    private init(storage: RDFSGraphEntailmentStorage) {
        self.storage = storage
    }

    public func subClasses(of classIRI: String) -> Set<String> {
        storage.classSubClosure.values(for: classIRI)
    }

    public func equivalentClasses(of classIRI: String) -> Set<String> {
        let superClasses = storage.classSuperClosure.values(for: classIRI)
        return Set(superClasses.filter { candidate in
            storage.classSuperClosure.contains(
                classIRI,
                for: candidate
            )
        })
    }

    public func instances(of classIRI: String) throws -> Set<RDFTerm> {
        storage.instancesByClass.values(for: classIRI)
    }

    public func contains(_ node: RDFTerm, in classIRI: String) -> Bool {
        storage.typesByNode.contains(classIRI, for: node)
    }

    public func subsumes(
        superClass: String,
        subClass: String
    ) -> Bool {
        superClass == subClass
            || storage.classSuperClosure.contains(
                superClass,
                for: subClass
            )
    }

    public func subProperties(of propertyIRI: String) -> Set<String> {
        storage.propertySubClosure.values(for: propertyIRI)
    }
}
