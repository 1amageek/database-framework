// IncrementalReasoner.swift
// GraphIndex - Incremental OWL 2 RL Reasoning
//
// Implements the DRed (Delete and Re-derive) algorithm for incremental reasoning.
// When triples are added or deleted, only affected inferences are updated.
//
// Reference: Gupta, A., Mumick, I.S. (1995). "Maintenance of Materialized Views"
// Reference: Staudt, M., Jarke, M. (1996). "Incremental Maintenance of Externally Materialized Views"

import Foundation
import FoundationDB

/// Incremental reasoner using DRed algorithm
///
/// Provides efficient incremental updates to materialized inferences when
/// axioms/triples are added or deleted. Uses provenance tracking to determine
/// which inferences are affected by a change.
///
/// **DRed Algorithm Overview**:
/// 1. On insertion: Forward-chain to derive new inferences
/// 2. On deletion:
///    a. Find all inferences that depend on the deleted triple
///    b. Mark them as tentatively deleted
///    c. Attempt re-derivation via alternative paths
///    d. Permanently delete if no alternative derivation exists
///
/// **Example**:
/// ```swift
/// let reasoner = IncrementalReasoner(
///     ontologyStore: store,
///     materializer: materializer,
///     configuration: .default
/// )
///
/// // When a triple is added
/// let changes = try await reasoner.addTriple(
///     ("ex:Alice", "rdf:type", "ex:Employee"),
///     ontologyIRI: "http://example.org/org",
///     transaction: transaction
/// )
///
/// // When a triple is deleted
/// let deletionChanges = try await reasoner.deleteTriple(
///     ("ex:Alice", "rdf:type", "ex:Employee"),
///     ontologyIRI: "http://example.org/org",
///     transaction: transaction
/// )
/// ```
public final class IncrementalReasoner: Sendable {

    // MARK: - Configuration

    /// Configuration for incremental reasoning
    public struct Configuration: Sendable {
        /// Maximum depth for cascading deletions
        public let maxCascadeDepth: Int

        /// Maximum number of re-derivation attempts per triple
        public let maxRederivationAttempts: Int

        /// Whether to batch dependency updates
        public let batchDependencyUpdates: Bool

        /// Batch size for dependency updates
        public let dependencyBatchSize: Int

        /// Default configuration
        public static let `default` = Configuration(
            maxCascadeDepth: 100,
            maxRederivationAttempts: 10,
            batchDependencyUpdates: true,
            dependencyBatchSize: 100
        )

        public init(
            maxCascadeDepth: Int = 100,
            maxRederivationAttempts: Int = 10,
            batchDependencyUpdates: Bool = true,
            dependencyBatchSize: Int = 100
        ) {
            self.maxCascadeDepth = maxCascadeDepth
            self.maxRederivationAttempts = maxRederivationAttempts
            self.batchDependencyUpdates = batchDependencyUpdates
            self.dependencyBatchSize = dependencyBatchSize
        }
    }

    // MARK: - Properties

    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let ontologyStore: OntologyStore
    private let materializer: OWL2RLMaterializer
    private let configuration: Configuration
    private let dependencySubspace: Subspace

    // MARK: - Initialization

    public init(
        database: any DatabaseProtocol,
        ontologyStore: OntologyStore,
        materializer: OWL2RLMaterializer,
        dependencySubspace: Subspace,
        configuration: Configuration = .default
    ) {
        self.database = database
        self.ontologyStore = ontologyStore
        self.materializer = materializer
        self.dependencySubspace = dependencySubspace
        self.configuration = configuration
    }

    // MARK: - Public API

    /// Add a triple and compute new inferences
    ///
    /// - Parameters:
    ///   - triple: The triple being added
    ///   - ontologyIRI: IRI of the ontology to use
    ///   - transaction: Active transaction
    /// - Returns: Changes resulting from the addition
    public func addTriple(
        _ triple: (subject: String, predicate: String, object: String),
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> InferenceChanges {
        var changes = InferenceChanges()
        let startTime = Date()

        // Forward-chain materialization
        let result = try await materializer.materializeOnWrite(
            triple: triple,
            ontologyIRI: ontologyIRI,
            transaction: transaction
        )

        // Store inferred triples and dependencies
        for (inferredTriple, provenance) in result.inferred {
            // Store the inferred triple
            try await storeInferredTriple(inferredTriple, provenance: provenance, transaction: transaction)

            // Store dependencies for DRed
            try await storeDependencies(
                consequent: inferredTriple,
                antecedents: provenance.antecedents,
                transaction: transaction
            )

            changes.addedInferences.append(InferredTriple(triple: inferredTriple, provenance: provenance))
        }

        // Track affected classes
        if triple.predicate == WellKnownIRI.rdfType {
            changes.affectedClasses.insert(triple.object)
        }
        if triple.predicate == WellKnownIRI.rdfsSubClassOf {
            changes.affectedClasses.insert(triple.subject)
            changes.affectedClasses.insert(triple.object)
        }

        changes.statistics.processingTime = Date().timeIntervalSince(startTime)
        changes.statistics.triplesProcessed = 1
        changes.statistics.inferencesAdded = changes.addedInferences.count

        return changes
    }

    /// Delete a triple and remove dependent inferences (DRed algorithm)
    ///
    /// - Parameters:
    ///   - triple: The triple being deleted
    ///   - ontologyIRI: IRI of the ontology to use
    ///   - transaction: Active transaction
    /// - Returns: Changes resulting from the deletion
    public func deleteTriple(
        _ triple: (subject: String, predicate: String, object: String),
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> InferenceChanges {
        var changes = InferenceChanges()
        let startTime = Date()
        let tripleKey = TripleKey(triple.subject, triple.predicate, triple.object)

        // Phase 1: Find all dependent inferences
        let dependents = try await getTransitiveDependents(of: tripleKey, transaction: transaction)
        changes.statistics.cascadingChecks = dependents.count

        // Phase 2: Mark all dependents as tentatively deleted
        var tentativelyDeleted: Set<TripleKey> = []
        for dependent in dependents {
            try await markTentativelyDeleted(dependent, transaction: transaction)
            tentativelyDeleted.insert(dependent)
        }

        // Phase 3: Attempt re-derivation for each tentatively deleted triple
        var permanentlyDeleted: Set<TripleKey> = []
        var rederived: Set<TripleKey> = []

        for tentative in tentativelyDeleted {
            let canRederive = try await attemptRederivation(
                triple: tentative,
                excluding: tripleKey,
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

            if canRederive {
                // Triple can be re-derived via alternative path
                try await markValid(tentative, transaction: transaction)
                rederived.insert(tentative)
            } else {
                // Triple must be permanently deleted
                try await deleteInferredTriple(tentative, transaction: transaction)
                try await deleteDependencies(of: tentative, transaction: transaction)
                permanentlyDeleted.insert(tentative)
            }
        }

        // Delete the original triple's dependencies
        try await deleteDependencies(of: tripleKey, transaction: transaction)

        // Build changes result
        for deleted in permanentlyDeleted {
            changes.removedInferences.append(deleted)
        }

        // Track affected classes
        if triple.predicate == WellKnownIRI.rdfType {
            changes.affectedClasses.insert(triple.object)
        }
        if triple.predicate == WellKnownIRI.rdfsSubClassOf {
            changes.affectedClasses.insert(triple.subject)
            changes.affectedClasses.insert(triple.object)
        }

        changes.statistics.processingTime = Date().timeIntervalSince(startTime)
        changes.statistics.triplesProcessed = 1
        changes.statistics.inferencesRemoved = permanentlyDeleted.count
        changes.statistics.rederivations = rederived.count

        return changes
    }

    /// Add an axiom (class or property definition) and propagate inferences
    ///
    /// - Parameters:
    ///   - axiom: The axiom being added
    ///   - ontologyIRI: IRI of the ontology
    ///   - transaction: Active transaction
    /// - Returns: Changes resulting from the addition
    public func addAxiom(
        _ axiom: IncrementalAxiom,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> InferenceChanges {
        switch axiom {
        case .subClassOf(let subClass, let superClass):
            return try await addTriple(
                (subClass, WellKnownIRI.rdfsSubClassOf, superClass),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .equivalentClasses(let class1, let class2):
            var changes = try await addTriple(
                (class1, WellKnownIRI.owlEquivalentClass, class2),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            let reverseChanges = try await addTriple(
                (class2, WellKnownIRI.owlEquivalentClass, class1),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            changes.merge(reverseChanges)
            return changes

        case .subPropertyOf(let subProp, let superProp):
            return try await addTriple(
                (subProp, WellKnownIRI.rdfsSubPropertyOf, superProp),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .inverseOf(let prop1, let prop2):
            var changes = try await addTriple(
                (prop1, WellKnownIRI.owlInverseOf, prop2),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            let reverseChanges = try await addTriple(
                (prop2, WellKnownIRI.owlInverseOf, prop1),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            changes.merge(reverseChanges)
            return changes

        case .domain(let property, let classIRI):
            return try await addTriple(
                (property, WellKnownIRI.rdfsDomain, classIRI),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .range(let property, let classIRI):
            return try await addTriple(
                (property, WellKnownIRI.rdfsRange, classIRI),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .symmetricProperty(let property):
            return try await addTriple(
                (property, WellKnownIRI.rdfType, WellKnownIRI.owlSymmetricProperty),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .transitiveProperty(let property):
            return try await addTriple(
                (property, WellKnownIRI.rdfType, WellKnownIRI.owlTransitiveProperty),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .classAssertion(let individual, let classIRI):
            return try await addTriple(
                (individual, WellKnownIRI.rdfType, classIRI),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .propertyAssertion(let subject, let property, let object):
            return try await addTriple(
                (subject, property, object),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }

    /// Remove an axiom and propagate deletions
    ///
    /// - Parameters:
    ///   - axiom: The axiom being removed
    ///   - ontologyIRI: IRI of the ontology
    ///   - transaction: Active transaction
    /// - Returns: Changes resulting from the removal
    public func removeAxiom(
        _ axiom: IncrementalAxiom,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> InferenceChanges {
        switch axiom {
        case .subClassOf(let subClass, let superClass):
            return try await deleteTriple(
                (subClass, WellKnownIRI.rdfsSubClassOf, superClass),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .equivalentClasses(let class1, let class2):
            var changes = try await deleteTriple(
                (class1, WellKnownIRI.owlEquivalentClass, class2),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            let reverseChanges = try await deleteTriple(
                (class2, WellKnownIRI.owlEquivalentClass, class1),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            changes.merge(reverseChanges)
            return changes

        case .subPropertyOf(let subProp, let superProp):
            return try await deleteTriple(
                (subProp, WellKnownIRI.rdfsSubPropertyOf, superProp),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .inverseOf(let prop1, let prop2):
            var changes = try await deleteTriple(
                (prop1, WellKnownIRI.owlInverseOf, prop2),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            let reverseChanges = try await deleteTriple(
                (prop2, WellKnownIRI.owlInverseOf, prop1),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
            changes.merge(reverseChanges)
            return changes

        case .domain(let property, let classIRI):
            return try await deleteTriple(
                (property, WellKnownIRI.rdfsDomain, classIRI),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .range(let property, let classIRI):
            return try await deleteTriple(
                (property, WellKnownIRI.rdfsRange, classIRI),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .symmetricProperty(let property):
            return try await deleteTriple(
                (property, WellKnownIRI.rdfType, WellKnownIRI.owlSymmetricProperty),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .transitiveProperty(let property):
            return try await deleteTriple(
                (property, WellKnownIRI.rdfType, WellKnownIRI.owlTransitiveProperty),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .classAssertion(let individual, let classIRI):
            return try await deleteTriple(
                (individual, WellKnownIRI.rdfType, classIRI),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )

        case .propertyAssertion(let subject, let property, let object):
            return try await deleteTriple(
                (subject, property, object),
                ontologyIRI: ontologyIRI,
                transaction: transaction
            )
        }
    }

    // MARK: - Dependency Storage

    /// Store dependencies for an inferred triple
    private func storeDependencies(
        consequent: TripleKey,
        antecedents: [TripleKey],
        transaction: any TransactionProtocol
    ) async throws {
        // Key structure: [dependency]/[dependents]/[antecedent]/[consequent]
        let dependentsSubspace = dependencySubspace.subspace(Int64(0))
        // Key structure: [dependency]/[dependencies]/[consequent]/[antecedent]
        let dependenciesSubspace = dependencySubspace.subspace(Int64(1))

        for antecedent in antecedents {
            // Store antecedent -> consequent mapping
            let dependentKey = dependentsSubspace
                .subspace(antecedent.subject)
                .subspace(antecedent.predicate)
                .subspace(antecedent.object)
                .subspace(consequent.subject)
                .subspace(consequent.predicate)
                .pack(Tuple([consequent.object]))

            transaction.setValue([], for: dependentKey)

            // Store consequent -> antecedent mapping (for re-derivation)
            let dependencyKey = dependenciesSubspace
                .subspace(consequent.subject)
                .subspace(consequent.predicate)
                .subspace(consequent.object)
                .subspace(antecedent.subject)
                .subspace(antecedent.predicate)
                .pack(Tuple([antecedent.object]))

            transaction.setValue([], for: dependencyKey)
        }
    }

    /// Get all triples that depend on the given triple
    private func getDependents(
        of triple: TripleKey,
        transaction: any TransactionProtocol
    ) async throws -> [TripleKey] {
        let dependentsSubspace = dependencySubspace.subspace(Int64(0))
        let prefix = dependentsSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .subspace(triple.object)

        let (beginKey, endKey) = prefix.range()

        var results: [TripleKey] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            let elements = try prefix.unpack(key)
            guard elements.count >= 3,
                  let s = elements[0] as? String,
                  let p = elements[1] as? String,
                  let o = elements[2] as? String else {
                continue
            }
            results.append(TripleKey(s, p, o))
        }

        return results
    }

    /// Get all transitive dependents (cascade)
    private func getTransitiveDependents(
        of triple: TripleKey,
        transaction: any TransactionProtocol
    ) async throws -> Set<TripleKey> {
        var visited: Set<TripleKey> = []
        var queue: [TripleKey] = try await getDependents(of: triple, transaction: transaction)
        var depth = 0

        while !queue.isEmpty && depth < configuration.maxCascadeDepth {
            let current = queue.removeFirst()
            if visited.contains(current) { continue }
            visited.insert(current)

            let dependents = try await getDependents(of: current, transaction: transaction)
            queue.append(contentsOf: dependents)
            depth += 1
        }

        return visited
    }

    /// Delete dependencies of a triple
    private func deleteDependencies(
        of triple: TripleKey,
        transaction: any TransactionProtocol
    ) async throws {
        let dependentsSubspace = dependencySubspace.subspace(Int64(0))
        let dependenciesSubspace = dependencySubspace.subspace(Int64(1))

        // Clear dependents (where this triple is antecedent)
        let dependentsPrefix = dependentsSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .subspace(triple.object)

        let (dependentsBegin, dependentsEnd) = dependentsPrefix.range()
        transaction.clearRange(beginKey: dependentsBegin, endKey: dependentsEnd)

        // Clear dependencies (where this triple is consequent)
        let dependenciesPrefix = dependenciesSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .subspace(triple.object)

        let (dependenciesBegin, dependenciesEnd) = dependenciesPrefix.range()
        transaction.clearRange(beginKey: dependenciesBegin, endKey: dependenciesEnd)
    }

    // MARK: - Inferred Triple Storage

    /// Store an inferred triple
    private func storeInferredTriple(
        _ triple: TripleKey,
        provenance: InferenceProvenance,
        transaction: any TransactionProtocol
    ) async throws {
        // Key structure: [inferred]/[s]/[p]/[o]
        let inferredSubspace = dependencySubspace.subspace(Int64(2))
        let key = inferredSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .pack(Tuple([triple.object]))

        let value = try provenance.encode()
        transaction.setValue(Array(value), for: key)
    }

    /// Delete an inferred triple
    private func deleteInferredTriple(
        _ triple: TripleKey,
        transaction: any TransactionProtocol
    ) async throws {
        let inferredSubspace = dependencySubspace.subspace(Int64(2))
        let key = inferredSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .pack(Tuple([triple.object]))

        transaction.clear(key: key)
    }

    /// Mark a triple as tentatively deleted
    private func markTentativelyDeleted(
        _ triple: TripleKey,
        transaction: any TransactionProtocol
    ) async throws {
        // Update provenance to mark as invalid
        let inferredSubspace = dependencySubspace.subspace(Int64(2))
        let key = inferredSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .pack(Tuple([triple.object]))

        if let value = try await transaction.getValue(for: key, snapshot: true) {
            var provenance = try InferenceProvenance.decode(from: Data(value))
            provenance.isValid = false
            let newValue = try provenance.encode()
            transaction.setValue(Array(newValue), for: key)
        }
    }

    /// Mark a triple as valid (after re-derivation)
    private func markValid(
        _ triple: TripleKey,
        transaction: any TransactionProtocol
    ) async throws {
        let inferredSubspace = dependencySubspace.subspace(Int64(2))
        let key = inferredSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .pack(Tuple([triple.object]))

        if let value = try await transaction.getValue(for: key, snapshot: true) {
            var provenance = try InferenceProvenance.decode(from: Data(value))
            provenance.isValid = true
            let newValue = try provenance.encode()
            transaction.setValue(Array(newValue), for: key)
        }
    }

    // MARK: - Re-derivation

    /// Attempt to re-derive a triple via alternative paths
    private func attemptRederivation(
        triple: TripleKey,
        excluding: TripleKey,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Bool {
        // Get the original dependencies of this triple
        let dependenciesSubspace = dependencySubspace.subspace(Int64(1))
        let prefix = dependenciesSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .subspace(triple.object)

        let (beginKey, endKey) = prefix.range()

        // Collect all antecedents that are NOT the excluded triple
        var alternativeAntecedents: [TripleKey] = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            let elements = try prefix.unpack(key)
            guard elements.count >= 3,
                  let s = elements[0] as? String,
                  let p = elements[1] as? String,
                  let o = elements[2] as? String else {
                continue
            }

            let antecedent = TripleKey(s, p, o)
            if antecedent != excluding {
                alternativeAntecedents.append(antecedent)
            }
        }

        // If no alternative antecedents, cannot re-derive
        if alternativeAntecedents.isEmpty {
            return false
        }

        // Check if all alternative antecedents still exist
        // This is a simplified check - full re-derivation would re-run the rule
        for antecedent in alternativeAntecedents {
            let exists = try await tripleExists(antecedent, transaction: transaction)
            if !exists {
                return false
            }
        }

        return true
    }

    /// Check if a triple exists (either explicit or inferred)
    private func tripleExists(
        _ triple: TripleKey,
        transaction: any TransactionProtocol
    ) async throws -> Bool {
        let inferredSubspace = dependencySubspace.subspace(Int64(2))
        let key = inferredSubspace
            .subspace(triple.subject)
            .subspace(triple.predicate)
            .pack(Tuple([triple.object]))

        if let value = try await transaction.getValue(for: key, snapshot: true) {
            let provenance = try InferenceProvenance.decode(from: Data(value))
            return provenance.isValid
        }
        return false
    }
}

// MARK: - Incremental Axiom Types

/// Simplified axiom types for incremental reasoning
///
/// This is a lightweight representation optimized for the DRed algorithm's
/// dependency tracking and incremental updates. It uses simple IRI strings
/// instead of complex class expressions.
///
/// **Relationship to `Graph.OWLAxiom`**:
/// - `Graph.OWLAxiom` (in database-kit) provides full OWL DL support with
///   `OWLClassExpression` for complex class descriptions (intersections,
///   unions, restrictions, etc.)
/// - `IncrementalAxiom` is a flattened representation where all classes
///   and properties are identified by their IRIs only
///
/// **When to use which**:
/// - Use `Graph.OWLAxiom` for ontology definition and complex reasoning
/// - Use `IncrementalAxiom` for efficient incremental updates via DRed
///
/// **Conversion**: When working with `Graph.OWLAxiom`, extract the relevant
/// IRIs and create corresponding `IncrementalAxiom` instances. Complex
/// class expressions should be decomposed into multiple simple axioms.
///
/// **Example**:
/// ```swift
/// // Add a simple subclass axiom
/// let axiom = IncrementalAxiom.subClassOf(
///     subClass: "http://example.org/Employee",
///     superClass: "http://example.org/Person"
/// )
/// let changes = try await reasoner.addAxiom(axiom, ontologyIRI: ontologyIRI, transaction: tx)
/// ```
public enum IncrementalAxiom: Sendable, Hashable {
    case subClassOf(subClass: String, superClass: String)
    case equivalentClasses(class1: String, class2: String)
    case subPropertyOf(subProperty: String, superProperty: String)
    case inverseOf(property1: String, property2: String)
    case domain(property: String, classIRI: String)
    case range(property: String, classIRI: String)
    case symmetricProperty(property: String)
    case transitiveProperty(property: String)
    case classAssertion(individual: String, classIRI: String)
    case propertyAssertion(subject: String, property: String, object: String)
}

// MARK: - Inference Changes

/// Result of an incremental reasoning operation
public struct InferenceChanges: Sendable {
    /// Newly inferred triples
    public var addedInferences: [InferredTriple]

    /// Triples that were removed (permanently deleted)
    public var removedInferences: [TripleKey]

    /// Classes affected by the change
    public var affectedClasses: Set<String>

    /// Statistics about the operation
    public var statistics: IncrementalStatistics

    public init(
        addedInferences: [InferredTriple] = [],
        removedInferences: [TripleKey] = [],
        affectedClasses: Set<String> = [],
        statistics: IncrementalStatistics = IncrementalStatistics()
    ) {
        self.addedInferences = addedInferences
        self.removedInferences = removedInferences
        self.affectedClasses = affectedClasses
        self.statistics = statistics
    }

    /// Merge another set of changes into this one
    public mutating func merge(_ other: InferenceChanges) {
        addedInferences.append(contentsOf: other.addedInferences)
        removedInferences.append(contentsOf: other.removedInferences)
        affectedClasses.formUnion(other.affectedClasses)
        statistics.merge(other.statistics)
    }

    /// Check if there were any changes
    public var isEmpty: Bool {
        addedInferences.isEmpty && removedInferences.isEmpty
    }
}

/// Statistics for incremental reasoning operations
public struct IncrementalStatistics: Sendable {
    /// Number of triples processed
    public var triplesProcessed: Int = 0

    /// Number of inferences added
    public var inferencesAdded: Int = 0

    /// Number of inferences removed
    public var inferencesRemoved: Int = 0

    /// Number of successful re-derivations
    public var rederivations: Int = 0

    /// Number of cascading dependency checks
    public var cascadingChecks: Int = 0

    /// Total processing time
    public var processingTime: TimeInterval = 0

    public init() {}

    /// Merge another set of statistics
    public mutating func merge(_ other: IncrementalStatistics) {
        triplesProcessed += other.triplesProcessed
        inferencesAdded += other.inferencesAdded
        inferencesRemoved += other.inferencesRemoved
        rederivations += other.rederivations
        cascadingChecks += other.cascadingChecks
        processingTime += other.processingTime
    }
}
