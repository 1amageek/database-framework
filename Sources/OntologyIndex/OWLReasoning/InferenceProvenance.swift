// InferenceProvenance.swift
// GraphIndex - Inference provenance tracking
//
// Tracks the derivation history of inferred triples.
//
// Reference: Gupta, A., Mumick, I.S. (1995). "Maintenance of Materialized Views: Problems, Techniques, and Applications"

/// Provenance information for an inferred triple
///
/// Tracks how a triple was derived, enabling:
/// - Explanation generation (why was this inferred?)
/// - Trust/confidence propagation
///
/// **Example**:
/// ```swift
/// let provenance = try InferenceProvenance(
///     rule: .caxSco,
///     antecedents: [
///         try ReasoningTriple("ex:Alice", "rdf:type", "ex:Employee"),
///         try ReasoningTriple("ex:Employee", "rdfs:subClassOf", "ex:Person")
///     ]
/// )
/// // Records that ex:Alice rdf:type ex:Person was inferred from cax-sco rule
/// ```
public struct InferenceProvenance: Sendable, Hashable {

    /// The rule used to derive this triple
    public let rule: OWL2RLRule

    /// Antecedent triples (premises)
    ///
    /// The triples that were matched to fire this rule.
    public let antecedents: [ReasoningTriple]

    /// Optional derivation depth (for explanation ordering)
    public var depth: Int

    public init(
        rule: OWL2RLRule,
        antecedents: [ReasoningTriple],
        depth: Int = 1
    ) {
        self.rule = rule
        self.antecedents = antecedents
        self.depth = depth
    }

}

/// Result of an inference operation
public struct InferenceResult: Sendable {
    /// Newly inferred triples with provenance
    public var inferred: [(triple: ReasoningTriple, provenance: InferenceProvenance)]

    /// Triples that caused inconsistency
    public var inconsistencies: [InconsistencyReport]

    /// Statistics about the inference process
    public var statistics: InferenceStatistics

    public init(
        inferred: [(triple: ReasoningTriple, provenance: InferenceProvenance)] = [],
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
    public let involvedTriples: [ReasoningTriple]

    /// Human-readable description
    public let description: String

    public init(rule: OWL2RLRule, involvedTriples: [ReasoningTriple], description: String) {
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
    public var inferenceTime: Double = 0

    /// Number of triples examined
    public var triplesExamined: Int = 0

    public init() {}
}
