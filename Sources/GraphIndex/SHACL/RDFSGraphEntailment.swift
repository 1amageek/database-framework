import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import OntologyIndex
import StorageKit

/// Computes the finite RDFS closure needed to validate one selected data graph.
public struct RDFSGraphEntailment: SHACLEntailmentContext, Sendable {
    public let ontologyContext: OntologyContext

    private let classSuperClosure: [String: Set<String>]
    private let classSubClosure: [String: Set<String>]
    private let propertySubClosure: [String: Set<String>]
    private let instancesByClass: [String: Set<RDFTerm>]
    private let typesByNode: [RDFTerm: Set<String>]

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
        var builder = Builder()
        for row in scan {
            try builder.ingest(row.quad, budget: budget)
        }
        return try builder.finish(budget: budget)
    }

    public init(
        quads: [RDFQuad],
        budget: SHACLValidationWorkBudget
    ) throws {
        var builder = Builder()
        for quad in quads {
            try builder.ingest(quad, budget: budget)
        }
        self = try builder.finish(budget: budget)
    }

    private struct Builder {
        var directClassSupers: [String: Set<String>] = [:]
        var directPropertySupers: [String: Set<String>] = [:]
        var domains: [String: Set<String>] = [:]
        var ranges: [String: Set<String>] = [:]
        var directTypes: [RDFTerm: Set<String>] = [:]
        var assertions: [(RDFTerm, String, RDFTerm)] = []

        mutating func ingest(
            _ quad: borrowing RDFQuad,
            budget: SHACLValidationWorkBudget
        ) throws {
            try budget.consume(at: .validation)
            let subject = quad.subject.term
            let predicate = quad.predicate.rawValue
            switch predicate {
            case Vocabulary.rdfType:
                if case .iri(let classIRI) = quad.object {
                    directTypes[subject, default: []].insert(
                        classIRI.rawValue
                    )
                }
            case Vocabulary.subClassOf:
                if case .iri(let subClass) = subject,
                   case .iri(let superClass) = quad.object {
                    directClassSupers[subClass.rawValue, default: []]
                        .insert(superClass.rawValue)
                }
            case Vocabulary.subPropertyOf:
                if case .iri(let subProperty) = subject,
                   case .iri(let superProperty) = quad.object {
                    directPropertySupers[subProperty.rawValue, default: []]
                        .insert(superProperty.rawValue)
                }
            case Vocabulary.domain:
                if case .iri(let property) = subject,
                   case .iri(let domain) = quad.object {
                    domains[property.rawValue, default: []]
                        .insert(domain.rawValue)
                }
            case Vocabulary.range:
                if case .iri(let property) = subject,
                   case .iri(let range) = quad.object {
                    ranges[property.rawValue, default: []]
                        .insert(range.rawValue)
                }
            default:
                assertions.append((subject, predicate, quad.object))
            }
        }

        consuming func finish(
            budget: SHACLValidationWorkBudget
        ) throws -> RDFSGraphEntailment {
            let classSuperClosure = try RDFSGraphEntailment.transitiveClosure(
                directClassSupers,
                budget: budget
            )
            let propertySuperClosure = try RDFSGraphEntailment.transitiveClosure(
                directPropertySupers,
                budget: budget
            )
            let classSubClosure = RDFSGraphEntailment.inverted(
                classSuperClosure
            )
            let propertySubClosure = RDFSGraphEntailment.inverted(
                propertySuperClosure
            )

            var entailedTypes = directTypes
            for (subject, property, object) in assertions {
                try budget.consume(at: .validation)
                var entailedProperties: Set<String> = [property]
                entailedProperties.formUnion(
                    propertySuperClosure[property] ?? []
                )
                for entailedProperty in entailedProperties {
                    entailedTypes[subject, default: []].formUnion(
                        domains[entailedProperty] ?? []
                    )
                    if object.isRDFSubject {
                        entailedTypes[object, default: []].formUnion(
                            ranges[entailedProperty] ?? []
                        )
                    }
                }
            }

            var closedTypes: [RDFTerm: Set<String>] = [:]
            closedTypes.reserveCapacity(entailedTypes.count)
            for (node, direct) in entailedTypes {
                try budget.consume(at: .validation)
                var closure = direct
                for classIRI in direct {
                    closure.formUnion(classSuperClosure[classIRI] ?? [])
                }
                closedTypes[node] = closure
            }

            var instancesByClass: [String: Set<RDFTerm>] = [:]
            for (node, types) in closedTypes {
                for type in types {
                    try budget.consume(at: .deduplication)
                    instancesByClass[type, default: []].insert(node)
                }
            }

            var roleHierarchy = RoleHierarchy()
            for (subProperty, superProperties) in directPropertySupers {
                for superProperty in superProperties {
                    try budget.consume(at: .validation)
                    roleHierarchy.addSubRole(
                        sub: subProperty,
                        super: superProperty
                    )
                }
            }
            roleHierarchy.ensureClosuresComputed()

            return RDFSGraphEntailment(
                ontologyContext: OntologyContext(
                    roleHierarchy: roleHierarchy
                ),
                classSuperClosure: classSuperClosure,
                classSubClosure: classSubClosure,
                propertySubClosure: propertySubClosure,
                instancesByClass: instancesByClass,
                typesByNode: closedTypes
            )
        }
    }

    private init(
        ontologyContext: OntologyContext,
        classSuperClosure: [String: Set<String>],
        classSubClosure: [String: Set<String>],
        propertySubClosure: [String: Set<String>],
        instancesByClass: [String: Set<RDFTerm>],
        typesByNode: [RDFTerm: Set<String>]
    ) {
        self.ontologyContext = ontologyContext
        self.classSuperClosure = classSuperClosure
        self.classSubClosure = classSubClosure
        self.propertySubClosure = propertySubClosure
        self.instancesByClass = instancesByClass
        self.typesByNode = typesByNode
    }

    public func subClasses(of classIRI: String) -> Set<String> {
        classSubClosure[classIRI] ?? []
    }

    public func equivalentClasses(of classIRI: String) -> Set<String> {
        let superClasses = classSuperClosure[classIRI] ?? []
        return Set(superClasses.filter { candidate in
            classSuperClosure[candidate]?.contains(classIRI) == true
        })
    }

    public func instances(of classIRI: String) throws -> Set<RDFTerm> {
        instancesByClass[classIRI] ?? []
    }

    public func contains(_ node: RDFTerm, in classIRI: String) -> Bool {
        typesByNode[node]?.contains(classIRI) == true
    }

    public func subsumes(
        superClass: String,
        subClass: String
    ) -> Bool {
        superClass == subClass
            || classSuperClosure[subClass]?.contains(superClass) == true
    }

    public func subProperties(of propertyIRI: String) -> Set<String> {
        propertySubClosure[propertyIRI] ?? []
    }

    private static func transitiveClosure(
        _ direct: [String: Set<String>],
        budget: SHACLValidationWorkBudget
    ) throws -> [String: Set<String>] {
        var universe = Set(direct.keys)
        for values in direct.values {
            universe.formUnion(values)
        }
        var result: [String: Set<String>] = [:]
        for origin in universe {
            var visited = Set<String>()
            var queue = Array(direct[origin] ?? []).sorted()
            var cursor = 0
            while cursor < queue.count {
                try budget.consume(at: .validation)
                let current = queue[cursor]
                cursor += 1
                guard visited.insert(current).inserted else { continue }
                queue.append(contentsOf: (direct[current] ?? []).sorted())
            }
            visited.remove(origin)
            result[origin] = visited
        }
        return result
    }

    private static func inverted(
        _ closure: [String: Set<String>]
    ) -> [String: Set<String>] {
        var result: [String: Set<String>] = [:]
        for (source, targets) in closure {
            for target in targets {
                result[target, default: []].insert(source)
            }
        }
        return result
    }

    private enum Vocabulary {
        static let rdfType =
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        static let subClassOf =
            "http://www.w3.org/2000/01/rdf-schema#subClassOf"
        static let subPropertyOf =
            "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
        static let domain =
            "http://www.w3.org/2000/01/rdf-schema#domain"
        static let range =
            "http://www.w3.org/2000/01/rdf-schema#range"
    }
}

private extension RDFTerm {
    var isRDFSubject: Bool {
        switch self {
        case .iri, .blankNode:
            return true
        case .literal, .tripleTerm:
            return false
        }
    }
}
