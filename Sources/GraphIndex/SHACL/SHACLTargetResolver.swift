// SHACLTargetResolver.swift
// GraphIndex - Resolve SHACL targets to focus nodes
//
// Converts SHACL target declarations into SPARQL queries
// and executes them against the graph index.
//
// Reference: W3C SHACL §2.1.3 (Targets)
// https://www.w3.org/TR/shacl/#targets

import StorageKit
import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Resolves SHACL targets to focus nodes using SPARQL queries
///
/// Each target type maps to a specific SPARQL pattern:
/// - `sh:targetNode` → direct set inclusion
/// - `sh:targetClass` → `{ ?node rdf:type <class> }`
/// - `sh:targetSubjectsOf` → `{ ?node <predicate> ?o }`
/// - `sh:targetObjectsOf` → `{ ?s <predicate> ?node }`
public struct SHACLTargetResolver: Sendable {

    private let executor: SPARQLQueryExecutor
    private let transaction: any TransactionReadAccess
    private let dataGraph: SHACLDataGraphTarget
    private let entailmentContext: (any SHACLEntailmentContext)?
    private let budget: SHACLValidationWorkBudget

    public init(
        executor: SPARQLQueryExecutor,
        transaction: any TransactionReadAccess,
        dataGraph: SHACLDataGraphTarget,
        entailmentContext: (any SHACLEntailmentContext)? = nil,
        budget: SHACLValidationWorkBudget
    ) {
        self.executor = executor
        self.transaction = transaction
        self.dataGraph = dataGraph
        self.entailmentContext = entailmentContext
        self.budget = budget
    }

    /// Resolve all targets to a set of focus RDF nodes.
    ///
    /// - Parameters:
    ///   - targets: The target declarations from a shape
    ///   - shapeIdentifier: The shape identifier used by implicit class targets
    /// - Returns: Set of focus RDF terms
    func resolve(
        _ targets: [SHACLTarget],
        shapeIdentifier: RDFTerm?
    ) async throws -> SHACLRetainedTerms {
        var focusNodes = try SHACLRetainedTermSetBuilder(
            workMeter: budget.workMeter,
            expectedCount: targets.count
        )

        for target in targets {
            try budget.consume(at: .projection)
            let nodes = try await resolveTarget(
                target,
                shapeIdentifier: shapeIdentifier
            )
            try focusNodes.formUnion(nodes)
        }

        return try focusNodes.finish()
    }

    // MARK: - Private

    private func resolveTarget(
        _ target: SHACLTarget,
        shapeIdentifier: RDFTerm?
    ) async throws -> SHACLRetainedTerms {
        switch target {
        case .node(let node):
            // Direct node — no query needed
            var nodes = try SHACLRetainedTermSetBuilder(
                workMeter: budget.workMeter,
                expectedCount: 1
            )
            try nodes.insert(node)
            return try nodes.finish()

        case .class_(let classIRI):
            return try await queryInstances(of: classIRI)

        case .subjectsOf(let predicateIRI):
            return try await queryEntailedSubjects(
                predicate: predicateIRI
            )

        case .objectsOf(let predicateIRI):
            return try await queryEntailedObjects(
                predicate: predicateIRI
            )

        case .implicitClass:
            // An IRI shape identifier is treated as an rdfs:Class.
            guard case .iri(let iri) = shapeIdentifier else {
                throw SHACLTargetResolutionError
                    .implicitClassRequiresIRI(shapeIdentifier)
            }
            return try await queryInstances(of: iri.rawValue)
        }
    }

    private func queryInstances(
        of classIRI: String
    ) async throws -> SHACLRetainedTerms {
        var entailedClasses: Set<String> = [classIRI]
        if let entailmentContext {
            entailedClasses.formUnion(
                entailmentContext.subClasses(of: classIRI)
            )
            entailedClasses.formUnion(
                entailmentContext.equivalentClasses(of: classIRI)
            )
        }
        try budget.consume(
            UInt64(entailedClasses.count),
            at: .projection
        )

        var instances = try SHACLRetainedTermSetBuilder(
            workMeter: budget.workMeter
        )
        for entailedClass in entailedClasses.sorted() {
            try instances.formUnion(
                try await querySubjects(
                    predicate: "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                    object: entailedClass
                )
            )
        }
        if let entailmentContext {
            for individual in try entailmentContext.instances(
                of: classIRI
            ).sorted() {
                try budget.consume(at: .deduplication)
                try instances.insert(individual)
            }
        }
        return try instances.finish()
    }

    private func queryEntailedSubjects(
        predicate: String
    ) async throws -> SHACLRetainedTerms {
        var predicates: Set<String> = [predicate]
        if let entailmentContext {
            predicates.formUnion(
                entailmentContext.subProperties(of: predicate)
            )
        }
        var nodes = try SHACLRetainedTermSetBuilder(
            workMeter: budget.workMeter
        )
        for entailedPredicate in predicates.sorted() {
            try nodes.formUnion(
                try await querySubjects(predicate: entailedPredicate)
            )
        }
        return try nodes.finish()
    }

    private func queryEntailedObjects(
        predicate: String
    ) async throws -> SHACLRetainedTerms {
        var predicates: Set<String> = [predicate]
        if let entailmentContext {
            predicates.formUnion(
                entailmentContext.subProperties(of: predicate)
            )
        }
        var nodes = try SHACLRetainedTermSetBuilder(
            workMeter: budget.workMeter
        )
        for entailedPredicate in predicates.sorted() {
            try nodes.formUnion(
                try await queryObjects(predicate: entailedPredicate)
            )
        }
        return try nodes.finish()
    }

    /// Query subjects matching { ?node <predicate> <object>? }
    private func querySubjects(
        predicate: String,
        object: String? = nil
    ) async throws -> SHACLRetainedTerms {
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .variable("?node"),
                predicate: .value(
                    .rdfTerm(.iri(try RDFIRI(predicate)))
                ),
                object: try object.map {
                    .value(.rdfTerm(.iri(try RDFIRI($0))))
                } ?? .wildcard
            )
        ])

        let bindings = try await executor.executeRetainedInTransaction(
            pattern: dataGraph.apply(to: pattern),
            transaction: transaction,
            limit: nil,
            offset: 0,
            workMeter: budget.workMeter
        )
        try budget.consume(UInt64(bindings.count), at: .deduplication)

        var nodes = try SHACLRetainedTermSetBuilder(
            workMeter: budget.workMeter,
            expectedCount: bindings.count
        )
        for index in 0..<bindings.count {
            try bindings.withBinding(
                at: index,
                workMeter: budget.workMeter
            ) { binding in
                if let value = binding["?node"],
                   case .rdfTerm(let term) = value {
                    try nodes.insert(term)
                }
            }
        }
        return try nodes.finish()
    }

    /// Query objects matching { ?s <predicate> ?node }
    private func queryObjects(
        predicate: String
    ) async throws -> SHACLRetainedTerms {
        let pattern = ExecutionPattern.basic([
            ExecutionTriple(
                subject: .wildcard,
                predicate: .value(
                    .rdfTerm(.iri(try RDFIRI(predicate)))
                ),
                object: .variable("?node")
            )
        ])

        let bindings = try await executor.executeRetainedInTransaction(
            pattern: dataGraph.apply(to: pattern),
            transaction: transaction,
            limit: nil,
            offset: 0,
            workMeter: budget.workMeter
        )
        try budget.consume(UInt64(bindings.count), at: .deduplication)

        var nodes = try SHACLRetainedTermSetBuilder(
            workMeter: budget.workMeter,
            expectedCount: bindings.count
        )
        for index in 0..<bindings.count {
            try bindings.withBinding(
                at: index,
                workMeter: budget.workMeter
            ) { binding in
                if let value = binding["?node"],
                   case .rdfTerm(let term) = value {
                    try nodes.insert(term)
                }
            }
        }
        return try nodes.finish()
    }
}
