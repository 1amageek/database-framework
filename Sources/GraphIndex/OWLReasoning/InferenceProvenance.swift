// InferenceProvenance.swift
// GraphIndex - Inference provenance tracking
//
// Tracks the derivation history of inferred triples for DRed maintenance.
//
// Reference: Gupta, A., Mumick, I.S. (1995). "Maintenance of Materialized Views: Problems, Techniques, and Applications"

import Foundation

/// Key identifying a triple (subject, predicate, object)
///
/// Used for tracking dependencies between inferred triples.
public struct TripleKey: Codable, Sendable, Hashable {
    public let subject: String
    public let predicate: String
    public let object: String

    public init(subject: String, predicate: String, object: String) {
        self.subject = subject
        self.predicate = predicate
        self.object = object
    }

    public init(_ s: String, _ p: String, _ o: String) {
        self.subject = s
        self.predicate = p
        self.object = o
    }
}

extension TripleKey: CustomStringConvertible {
    public var description: String {
        "(\(subject), \(predicate), \(object))"
    }
}

/// Provenance information for an inferred triple
///
/// Tracks how a triple was derived, enabling:
/// - Explanation generation (why was this inferred?)
/// - DRed maintenance (what needs re-evaluation on deletion?)
/// - Trust/confidence propagation
///
/// **DRed Algorithm**:
/// When a base triple is deleted:
/// 1. Find all inferred triples that depend on it
/// 2. Mark them as tentatively deleted
/// 3. Attempt re-derivation via alternative paths
/// 4. Permanently delete if no alternative derivation exists
///
/// **Example**:
/// ```swift
/// let provenance = InferenceProvenance(
///     rule: .caxSco,
///     antecedents: [
///         TripleKey("ex:Alice", "rdf:type", "ex:Employee"),
///         TripleKey("ex:Employee", "rdfs:subClassOf", "ex:Person")
///     ]
/// )
/// // Records that ex:Alice rdf:type ex:Person was inferred from cax-sco rule
/// ```
public struct InferenceProvenance: Codable, Sendable, Hashable {

    /// The rule used to derive this triple
    public let rule: OWL2RLRule

    /// Antecedent triples (premises)
    ///
    /// The triples that were matched to fire this rule.
    /// Empty for explicitly asserted triples.
    public let antecedents: [TripleKey]

    /// Timestamp when inference was made
    public let inferredAt: Date

    /// Whether this inference is still valid
    ///
    /// Set to false during DRed deletion phase.
    /// If re-derivation succeeds, set back to true.
    public var isValid: Bool

    /// Optional derivation depth (for explanation ordering)
    public var depth: Int

    public init(
        rule: OWL2RLRule,
        antecedents: [TripleKey],
        inferredAt: Date = Date(),
        isValid: Bool = true,
        depth: Int = 1
    ) {
        self.rule = rule
        self.antecedents = antecedents
        self.inferredAt = inferredAt
        self.isValid = isValid
        self.depth = depth
    }

    /// Create provenance for an explicitly asserted triple
    public static func asserted() -> InferenceProvenance {
        InferenceProvenance(
            rule: .eqRef, // Dummy rule for explicit assertions
            antecedents: [],
            depth: 0
        )
    }

    /// Check if this is an explicit assertion (no derivation)
    public var isExplicit: Bool {
        antecedents.isEmpty && depth == 0
    }
}

// MARK: - Encoding/Decoding

extension InferenceProvenance {
    /// Encode to JSON bytes
    public func encode() throws -> Data {
        try JSONEncoder().encode(self)
    }

    /// Decode from JSON bytes
    public static func decode(from data: Data) throws -> InferenceProvenance {
        try JSONDecoder().decode(InferenceProvenance.self, from: data)
    }
}

/// Result of an inference operation
public struct InferenceResult: Sendable {
    /// Newly inferred triples with provenance
    public var inferred: [(triple: TripleKey, provenance: InferenceProvenance)]

    /// Triples that caused inconsistency
    public var inconsistencies: [InconsistencyReport]

    /// Statistics about the inference process
    public var statistics: InferenceStatistics

    public init(
        inferred: [(triple: TripleKey, provenance: InferenceProvenance)] = [],
        inconsistencies: [InconsistencyReport] = [],
        statistics: InferenceStatistics = InferenceStatistics()
    ) {
        self.inferred = inferred
        self.inconsistencies = inconsistencies
        self.statistics = statistics
    }

    /// Check if inference produced any results
    public var isEmpty: Bool {
        inferred.isEmpty && inconsistencies.isEmpty
    }

    /// Check if inference found inconsistencies
    public var hasInconsistencies: Bool {
        !inconsistencies.isEmpty
    }
}

/// Report of an inconsistency detected during reasoning
public struct InconsistencyReport: Sendable {
    /// The rule that detected the inconsistency
    public let rule: OWL2RLRule

    /// Triples involved in the inconsistency
    public let involvedTriples: [TripleKey]

    /// Human-readable description
    public let description: String

    public init(rule: OWL2RLRule, involvedTriples: [TripleKey], description: String) {
        self.rule = rule
        self.involvedTriples = involvedTriples
        self.description = description
    }
}

/// Statistics about inference operations
public struct InferenceStatistics: Sendable {
    /// Number of rule applications attempted
    public var ruleApplications: Int = 0

    /// Number of new triples inferred
    public var triplesInferred: Int = 0

    /// Number of duplicate inferences (already existed)
    public var duplicateInferences: Int = 0

    /// Number of inconsistencies detected
    public var inconsistenciesDetected: Int = 0

    /// Time spent on inference (seconds)
    public var inferenceTime: TimeInterval = 0

    /// Number of triples examined
    public var triplesExamined: Int = 0

    public init() {}
}

/// Deletion status during DRed algorithm
public enum DeletionStatus: String, Codable, Sendable {
    /// Triple is valid and confirmed
    case valid

    /// Triple is tentatively deleted, pending re-derivation attempt
    case tentativelyDeleted

    /// Triple is permanently deleted (no alternative derivation)
    case deleted

    /// Triple was re-derived via alternative path
    case rederived
}

/// Result of a DRed deletion operation
public struct DRedDeletionResult: Sendable {
    /// Triples that were permanently deleted
    public var permanentlyDeleted: [TripleKey]

    /// Triples that were re-derived via alternative paths
    public var rederived: [TripleKey]

    /// Triples that required cascading deletion checks
    public var cascadingChecks: Int

    /// Time spent on deletion maintenance
    public var maintenanceTime: TimeInterval

    public init(
        permanentlyDeleted: [TripleKey] = [],
        rederived: [TripleKey] = [],
        cascadingChecks: Int = 0,
        maintenanceTime: TimeInterval = 0
    ) {
        self.permanentlyDeleted = permanentlyDeleted
        self.rederived = rederived
        self.cascadingChecks = cascadingChecks
        self.maintenanceTime = maintenanceTime
    }
}

// MARK: - Dependency Graph

/// Dependency tracking for inferred triples
///
/// Maintains a graph of dependencies between triples for efficient DRed.
public struct DependencyGraph: Sendable {
    /// Maps each triple to triples that depend on it
    public var dependents: [TripleKey: Set<TripleKey>]

    /// Maps each triple to triples it depends on
    public var dependencies: [TripleKey: Set<TripleKey>]

    public init() {
        self.dependents = [:]
        self.dependencies = [:]
    }

    /// Add a dependency: consequent depends on antecedent
    public mutating func addDependency(antecedent: TripleKey, consequent: TripleKey) {
        dependents[antecedent, default: []].insert(consequent)
        dependencies[consequent, default: []].insert(antecedent)
    }

    /// Get all triples that depend on the given triple
    public func getDependents(of triple: TripleKey) -> Set<TripleKey> {
        dependents[triple] ?? []
    }

    /// Get all triples that the given triple depends on
    public func getDependencies(of triple: TripleKey) -> Set<TripleKey> {
        dependencies[triple] ?? []
    }

    /// Remove a triple and all its dependency relationships
    public mutating func remove(_ triple: TripleKey) {
        // Remove from dependents of its dependencies
        if let deps = dependencies[triple] {
            for dep in deps {
                dependents[dep]?.remove(triple)
            }
        }

        // Remove from dependencies of its dependents
        if let deps = dependents[triple] {
            for dep in deps {
                dependencies[dep]?.remove(triple)
            }
        }

        dependents.removeValue(forKey: triple)
        dependencies.removeValue(forKey: triple)
    }

    /// Get all transitively dependent triples (cascade)
    public func getTransitiveDependents(of triple: TripleKey) -> Set<TripleKey> {
        var visited: Set<TripleKey> = []
        var queue: [TripleKey] = Array(getDependents(of: triple))

        while !queue.isEmpty {
            let current = queue.removeFirst()
            if visited.contains(current) { continue }
            visited.insert(current)
            queue.append(contentsOf: getDependents(of: current))
        }

        return visited
    }
}
