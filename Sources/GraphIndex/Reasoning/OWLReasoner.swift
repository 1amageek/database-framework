// OWLReasoner.swift
// GraphIndex - Main OWL DL reasoning interface
//
// Provides a unified interface for OWL DL reasoning operations.
//
// Reference: OWL 2 Web Ontology Language Direct Semantics
// https://www.w3.org/TR/owl2-direct-semantics/

import Foundation
import Graph
import Synchronization

/// OWL DL Reasoner
///
/// Main interface for OWL DL reasoning operations including:
/// - Ontology consistency checking
/// - Class satisfiability
/// - Subsumption (class hierarchy)
/// - Instance classification
/// - Property reasoning
///
/// **Thread Safety**: This class is thread-safe using Mutex for state protection.
///
/// **Example**:
/// ```swift
/// // Create reasoner
/// let reasoner = OWLReasoner(ontology: ontology)
///
/// // Validate ontology
/// let isValid = reasoner.validateOWLDL()
///
/// // Check consistency
/// let isConsistent = try await reasoner.isConsistent()
///
/// // Classification queries
/// let superClasses = reasoner.superClasses(of: "ex:Employee")
/// let instances = reasoner.instances(of: .named("ex:Person"))
/// ```
public final class OWLReasoner: Sendable {

    // MARK: - Types

    /// Reasoning configuration
    public struct Configuration: Sendable {
        /// Maximum expansion steps for tableaux
        public var maxExpansionSteps: Int

        /// Enable incremental reasoning
        public var enableIncrementalReasoning: Bool

        /// Cache classification results
        public var cacheClassification: Bool

        /// Timeout for reasoning operations (seconds)
        public var timeout: TimeInterval

        public init(
            maxExpansionSteps: Int = 10000,
            enableIncrementalReasoning: Bool = true,
            cacheClassification: Bool = true,
            timeout: TimeInterval = 60.0
        ) {
            self.maxExpansionSteps = maxExpansionSteps
            self.enableIncrementalReasoning = enableIncrementalReasoning
            self.cacheClassification = cacheClassification
            self.timeout = timeout
        }

        public static let `default` = Configuration()
    }

    /// Reasoning result with explanation
    public struct ReasoningResult<T: Sendable>: Sendable {
        public let value: T
        public let inferred: Bool
        public let explanation: [String]
        public let statistics: Statistics

        public init(value: T, inferred: Bool = false, explanation: [String] = [], statistics: Statistics = Statistics()) {
            self.value = value
            self.inferred = inferred
            self.explanation = explanation
            self.statistics = statistics
        }
    }

    /// Reasoning statistics
    public struct Statistics: Sendable {
        public var satisfiabilityChecks: Int = 0
        public var subsumptionChecks: Int = 0
        public var instanceChecks: Int = 0
        public var totalReasoningTime: TimeInterval = 0
        public var cacheHits: Int = 0
        public var cacheMisses: Int = 0

        public init(
            satisfiabilityChecks: Int = 0,
            subsumptionChecks: Int = 0,
            instanceChecks: Int = 0,
            totalReasoningTime: TimeInterval = 0,
            cacheHits: Int = 0,
            cacheMisses: Int = 0
        ) {
            self.satisfiabilityChecks = satisfiabilityChecks
            self.subsumptionChecks = subsumptionChecks
            self.instanceChecks = instanceChecks
            self.totalReasoningTime = totalReasoningTime
            self.cacheHits = cacheHits
            self.cacheMisses = cacheMisses
        }
    }

    // MARK: - Cache Key Types

    /// Hashable cache key for subsumption queries
    private struct SubsumptionPair: Hashable, Sendable {
        let subClass: OWLClassExpression
        let superClass: OWLClassExpression
    }

    // MARK: - State

    private struct State: Sendable {
        var isClassified: Bool = false
        var classHierarchy: ClassHierarchy
        var roleHierarchy: RoleHierarchy
        var statistics: Statistics = Statistics()

        // Caches — keyed by canonicalized OWLClassExpression for stable identity
        var satisfiabilityCache: [OWLClassExpression: Bool] = [:]
        var subsumptionCache: [SubsumptionPair: Bool] = [:]
        var instanceCache: [String: Set<String>] = [:]
    }

    // MARK: - Properties

    private let ontology: OWLOntology
    private let ontologyIndex: OntologyIndex
    private let configuration: Configuration
    private let state: Mutex<State>
    private let tableauxReasoner: TableauxReasoner
    private let regularityChecker: OWLDLRegularityChecker
    private let datatypeValidator: OWLDatatypeValidator

    // MARK: - Initialization

    /// Initialize reasoner with ontology
    ///
    /// - Parameters:
    ///   - ontology: The OWL ontology to reason over
    ///   - configuration: Reasoning configuration
    public init(ontology: OWLOntology, configuration: Configuration = .default) {
        self.ontology = ontology
        self.configuration = configuration

        // Build O(1) index once
        let index = ontology.buildIndex()
        self.ontologyIndex = index

        let classHierarchy = ClassHierarchy(ontology: ontology, index: index)
        let roleHierarchy = RoleHierarchy(ontology: ontology, index: index)

        self.state = Mutex(State(
            classHierarchy: classHierarchy,
            roleHierarchy: roleHierarchy
        ))

        self.tableauxReasoner = TableauxReasoner(
            ontology: ontology,
            index: index,
            configuration: TableauxReasoner.Configuration(
                maxExpansionSteps: configuration.maxExpansionSteps,
                timeout: configuration.timeout
            )
        )
        self.regularityChecker = OWLDLRegularityChecker()
        self.datatypeValidator = OWLDatatypeValidator()
    }

    // MARK: - Validation

    /// Validate ontology conforms to OWL DL
    ///
    /// - Returns: Tuple of (isValid, violations)
    public func validateOWLDL() -> (isValid: Bool, violations: [OWLDLRegularityChecker.Violation]) {
        var checker = regularityChecker
        let violations = checker.check(ontology)
        return (violations.isEmpty, violations)
    }

    /// Validate ontology structure
    ///
    /// - Returns: Array of validation errors
    public func validateStructure() -> [OWLOntology.ValidationError] {
        ontology.validate()
    }

    // MARK: - Consistency

    /// Check if the ontology is consistent
    ///
    /// An ontology is consistent if there exists a model
    /// that satisfies all axioms.
    ///
    /// - Returns: ReasoningResult with consistency status
    public func isConsistent() -> ReasoningResult<Bool> {
        let startTime = Date()

        // Check if owl:Thing is satisfiable
        let result = tableauxReasoner.checkSatisfiability(.thing)

        let elapsed = Date().timeIntervalSince(startTime)

        state.withLock { s in
            s.statistics.satisfiabilityChecks += 1
            s.statistics.totalReasoningTime += elapsed
        }

        return ReasoningResult(
            value: result.isSatisfiable,
            inferred: true,
            explanation: result.clash.map { [$0.description] } ?? [],
            statistics: state.withLock { $0.statistics }
        )
    }

    // MARK: - Satisfiability

    /// Check if a class expression is satisfiable
    ///
    /// - Parameter classExpr: The class expression to check
    /// - Returns: ReasoningResult with satisfiability status
    public func isSatisfiable(_ classExpr: OWLClassExpression) -> ReasoningResult<Bool> {
        let cacheKey = classExpr.canonicalized()

        // Check cache
        if configuration.cacheClassification {
            if let cached = state.withLock({ $0.satisfiabilityCache[cacheKey] }) {
                state.withLock { $0.statistics.cacheHits += 1 }
                return ReasoningResult(
                    value: cached,
                    inferred: true,
                    statistics: state.withLock { $0.statistics }
                )
            }
            state.withLock { $0.statistics.cacheMisses += 1 }
        }

        let startTime = Date()
        let result = tableauxReasoner.checkSatisfiability(classExpr)
        let elapsed = Date().timeIntervalSince(startTime)

        state.withLock { s in
            s.statistics.satisfiabilityChecks += 1
            s.statistics.totalReasoningTime += elapsed
            if configuration.cacheClassification {
                s.satisfiabilityCache[cacheKey] = result.isSatisfiable
            }
        }

        return ReasoningResult(
            value: result.isSatisfiable,
            inferred: true,
            explanation: result.clash.map { [$0.description] } ?? [],
            statistics: state.withLock { $0.statistics }
        )
    }

    // MARK: - Subsumption

    /// Check if superClass subsumes subClass
    ///
    /// - Parameters:
    ///   - superClass: The potential superclass
    ///   - subClass: The potential subclass
    /// - Returns: ReasoningResult with subsumption status
    public func subsumes(superClass: OWLClassExpression, subClass: OWLClassExpression) -> ReasoningResult<Bool> {
        let cacheKey = SubsumptionPair(
            subClass: subClass.canonicalized(),
            superClass: superClass.canonicalized()
        )

        // Check cache
        if configuration.cacheClassification {
            if let cached = state.withLock({ $0.subsumptionCache[cacheKey] }) {
                state.withLock { $0.statistics.cacheHits += 1 }
                return ReasoningResult(value: cached, inferred: true)
            }
            state.withLock { $0.statistics.cacheMisses += 1 }
        }

        let startTime = Date()
        let result = tableauxReasoner.subsumes(superClass: superClass, subClass: subClass)
        let elapsed = Date().timeIntervalSince(startTime)

        state.withLock { s in
            s.statistics.subsumptionChecks += 1
            s.statistics.totalReasoningTime += elapsed
            if configuration.cacheClassification {
                s.subsumptionCache[cacheKey] = result
            }
        }

        return ReasoningResult(
            value: result,
            inferred: true,
            statistics: state.withLock { $0.statistics }
        )
    }

    /// Check if two classes are equivalent
    public func areEquivalent(_ class1: OWLClassExpression, _ class2: OWLClassExpression) -> ReasoningResult<Bool> {
        let sub1 = subsumes(superClass: class1, subClass: class2)
        let sub2 = subsumes(superClass: class2, subClass: class1)

        return ReasoningResult(
            value: sub1.value && sub2.value,
            inferred: true,
            statistics: state.withLock { $0.statistics }
        )
    }

    /// Check if two classes are disjoint
    public func areDisjoint(_ class1: OWLClassExpression, _ class2: OWLClassExpression) -> ReasoningResult<Bool> {
        let result = tableauxReasoner.areDisjoint(class1, class2)
        return ReasoningResult(value: result, inferred: true)
    }

    // MARK: - Classification

    /// Classify the ontology (compute complete class hierarchy)
    ///
    /// - Returns: The computed class hierarchy
    public func classify() -> ClassHierarchy {
        let hierarchy = tableauxReasoner.classify()

        state.withLock { s in
            s.classHierarchy = hierarchy
            s.isClassified = true
        }

        return hierarchy
    }

    /// Get super-classes of a class
    ///
    /// - Parameters:
    ///   - classIRI: The class IRI
    ///   - direct: If true, only return direct super-classes
    /// - Returns: Set of super-class IRIs
    public func superClasses(of classIRI: String, direct: Bool = false) -> Set<String> {
        state.withLock { s in
            if direct {
                return s.classHierarchy.directSuperClasses(of: classIRI)
            } else {
                var h = s.classHierarchy
                return h.superClasses(of: classIRI)
            }
        }
    }

    /// Get sub-classes of a class
    ///
    /// - Parameters:
    ///   - classIRI: The class IRI
    ///   - direct: If true, only return direct sub-classes
    /// - Returns: Set of sub-class IRIs
    public func subClasses(of classIRI: String, direct: Bool = false) -> Set<String> {
        state.withLock { s in
            if direct {
                return s.classHierarchy.directSubClasses(of: classIRI)
            } else {
                var h = s.classHierarchy
                return h.subClasses(of: classIRI)
            }
        }
    }

    /// Get equivalent classes
    ///
    /// - Parameter classIRI: The class IRI
    /// - Returns: Set of equivalent class IRIs
    public func equivalentClasses(of classIRI: String) -> Set<String> {
        state.withLock { s in
            s.classHierarchy.equivalentClasses(of: classIRI)
        }
    }

    /// Get disjoint classes
    ///
    /// - Parameter classIRI: The class IRI
    /// - Returns: Set of disjoint class IRIs
    public func disjointClasses(of classIRI: String) -> Set<String> {
        state.withLock { s in
            s.classHierarchy.disjointClasses(of: classIRI)
        }
    }

    // MARK: - Instance Reasoning

    /// Check if an individual is an instance of a class
    ///
    /// - Parameters:
    ///   - individual: The individual IRI
    ///   - classExpr: The class expression
    /// - Returns: ReasoningResult with instance check result
    public func isInstanceOf(individual: String, classExpr: OWLClassExpression) -> ReasoningResult<Bool> {
        let startTime = Date()
        let result = tableauxReasoner.isInstanceOf(individual: individual, classExpr: classExpr)
        let elapsed = Date().timeIntervalSince(startTime)

        state.withLock { s in
            s.statistics.instanceChecks += 1
            s.statistics.totalReasoningTime += elapsed
        }

        return ReasoningResult(
            value: result,
            inferred: true,
            statistics: state.withLock { $0.statistics }
        )
    }

    /// Get all instances of a class
    ///
    /// - Parameters:
    ///   - classExpr: The class expression
    ///   - direct: If true, only return direct instances
    /// - Returns: Set of individual IRIs
    public func instances(of classExpr: OWLClassExpression, direct: Bool = false) -> Set<String> {
        tableauxReasoner.instances(of: classExpr)
    }

    /// Get all types of an individual
    ///
    /// Optimized: uses ClassHierarchy for subClassOf inference,
    /// Tableaux only for Defined Classes (equivalentClasses).
    ///
    /// - Parameters:
    ///   - individual: The individual IRI
    ///   - direct: If true, only return most specific types
    /// - Returns: Set of class IRIs
    public func types(of individual: String, direct: Bool = false) -> Set<String> {
        // 1. Cache check
        if let cached = state.withLock({ $0.instanceCache[individual] }) {
            return cached
        }

        var result = Set<String>()

        // 2. Collect asserted types (O(1) lookup via OntologyIndex)
        var assertedClasses = Set<String>()
        for type in ontologyIndex.classAssertionsByIndividual[individual] ?? [] {
            if case .named(let iri) = type {
                assertedClasses.insert(iri)
            }
        }
        result.formUnion(assertedClasses)

        // 3. Expand via ClassHierarchy transitive closure (pre-computed)
        state.withLock { s in
            for cls in assertedClasses {
                result.formUnion(s.classHierarchy.superClasses(of: cls))
            }
        }

        // 4. Check Defined Classes only via Tableaux
        let individualType = buildIndividualType(for: individual)
        let definedClasses: [(String, OWLClassExpression)] = state.withLock { s in
            ontologyIndex.classSignature.compactMap { cls in
                guard let def = s.classHierarchy.definition(of: cls) else { return nil }
                return (cls, def)
            }
        }

        if let indType = individualType {
            for (className, _) in definedClasses where !result.contains(className) {
                if tableauxReasoner.subsumes(superClass: .named(className), subClass: indType) {
                    result.insert(className)
                    state.withLock { s in
                        result.formUnion(s.classHierarchy.superClasses(of: className))
                    }
                }
            }
        }

        // 5. owl:Thing is always a type
        result.insert("owl:Thing")
        result.remove("owl:Nothing")

        // 6. Cache result
        state.withLock { $0.instanceCache[individual] = result }

        return result
    }

    /// Build the individual's type as a single OWLClassExpression
    /// by collecting all ABox assertions once via OntologyIndex (O(1) lookup).
    private func buildIndividualType(for individual: String) -> OWLClassExpression? {
        var types: [OWLClassExpression] = []

        for type in ontologyIndex.classAssertionsByIndividual[individual] ?? [] {
            types.append(type)
        }
        for (property, object) in ontologyIndex.objectPropertyAssertionsBySubject[individual] ?? [] {
            types.append(.hasValue(property: property, individual: object))
        }
        for (property, value) in ontologyIndex.dataPropertyAssertionsBySubject[individual] ?? [] {
            types.append(.dataHasValue(property: property, literal: value))
        }

        guard !types.isEmpty else { return nil }
        return types.count == 1 ? types[0] : .intersection(types)
    }

    // MARK: - Property Reasoning

    /// Get super-properties of a property
    ///
    /// - Parameters:
    ///   - propertyIRI: The property IRI
    ///   - direct: If true, only return direct super-properties
    /// - Returns: Set of property IRIs
    public func superProperties(of propertyIRI: String, direct: Bool = false) -> Set<String> {
        state.withLock { s in
            if direct {
                return s.roleHierarchy.directSuperRoles(of: propertyIRI)
            } else {
                var h = s.roleHierarchy
                return h.superRoles(of: propertyIRI)
            }
        }
    }

    /// Get sub-properties of a property
    ///
    /// - Parameters:
    ///   - propertyIRI: The property IRI
    ///   - direct: If true, only return direct sub-properties
    /// - Returns: Set of property IRIs
    public func subProperties(of propertyIRI: String, direct: Bool = false) -> Set<String> {
        state.withLock { s in
            if direct {
                return s.roleHierarchy.directSubRoles(of: propertyIRI)
            } else {
                var h = s.roleHierarchy
                return h.subRoles(of: propertyIRI)
            }
        }
    }

    /// Get inverse property
    ///
    /// - Parameter propertyIRI: The property IRI
    /// - Returns: Inverse property IRI or nil
    public func inverseProperty(of propertyIRI: String) -> String? {
        state.withLock { s in
            s.roleHierarchy.inverse(of: propertyIRI)
        }
    }

    /// Check if property is transitive
    public func isTransitive(_ propertyIRI: String) -> Bool {
        state.withLock { s in
            s.roleHierarchy.isTransitive(propertyIRI)
        }
    }

    /// Check if property is symmetric
    public func isSymmetric(_ propertyIRI: String) -> Bool {
        state.withLock { s in
            s.roleHierarchy.isSymmetric(propertyIRI)
        }
    }

    /// Check if property is functional
    public func isFunctional(_ propertyIRI: String) -> Bool {
        state.withLock { s in
            s.roleHierarchy.isFunctional(propertyIRI)
        }
    }

    // MARK: - Statistics

    /// Get current reasoning statistics
    public var statistics: Statistics {
        state.withLock { $0.statistics }
    }

    /// Clear all caches
    public func clearCaches() {
        state.withLock { s in
            s.satisfiabilityCache.removeAll()
            s.subsumptionCache.removeAll()
            s.instanceCache.removeAll()
            s.statistics.cacheHits = 0
            s.statistics.cacheMisses = 0
        }
    }

    // MARK: - Ontology Access

    /// The ontology being reasoned over
    public var reasoningOntology: OWLOntology {
        ontology
    }

    /// Check if ontology has been classified
    public var isClassified: Bool {
        state.withLock { $0.isClassified }
    }
}

// MARK: - Query Extension Support

extension OWLReasoner {
    /// Create a reasoning-enabled query for graph traversal
    ///
    /// This enables SPARQL-like queries with OWL DL inference.
    ///
    /// - Parameter individual: Starting individual IRI
    /// - Returns: Set of individuals reachable via inferred relationships
    public func reachableIndividuals(
        from individual: String,
        via property: String,
        includeInferred: Bool = true
    ) -> Set<String> {
        var result = Set<String>()

        // Direct assertions via OntologyIndex (O(1) lookup)
        for (prop, obj) in ontologyIndex.objectPropertyAssertionsBySubject[individual] ?? [] {
            if prop == property {
                result.insert(obj)
            }
        }

        if includeInferred {
            // Add inverse property assertions
            if let inverse = inverseProperty(of: property) {
                for (prop, subj) in ontologyIndex.objectPropertyAssertionsByObject[individual] ?? [] {
                    if prop == inverse {
                        result.insert(subj)
                    }
                }
            }

            // Add symmetric property assertions
            if isSymmetric(property) {
                for (prop, subj) in ontologyIndex.objectPropertyAssertionsByObject[individual] ?? [] {
                    if prop == property {
                        result.insert(subj)
                    }
                }
            }

            // Add sub-property assertions
            for subProp in subProperties(of: property) {
                for (prop, obj) in ontologyIndex.objectPropertyAssertionsBySubject[individual] ?? [] {
                    if prop == subProp {
                        result.insert(obj)
                    }
                }
            }

            // Handle transitive closure
            if isTransitive(property) {
                var frontier = result
                var visited = Set<String>([individual])

                while !frontier.isEmpty {
                    var newFrontier = Set<String>()
                    for node in frontier {
                        if visited.contains(node) { continue }
                        visited.insert(node)

                        for (prop, obj) in ontologyIndex.objectPropertyAssertionsBySubject[node] ?? [] {
                            if prop == property && !visited.contains(obj) {
                                result.insert(obj)
                                newFrontier.insert(obj)
                            }
                        }
                    }
                    frontier = newFrontier
                }
            }
        }

        return result
    }
}
