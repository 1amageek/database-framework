// TableauxReasoner.swift
// GraphIndex - SHOIN(D) Tableaux algorithm implementation
//
// Implements a sound and complete Tableaux decision procedure for OWL DL (SHOIN(D)).
//
// **Algorithm Features**:
// - Trail-based backtracking for efficient state management
// - Proper blocking for termination guarantee
// - Complete handling of all SHOIN(D) constructors
// - Property chain reasoning
// - Nominal (oneOf) support
//
// **Reference**:
// - Baader, F., et al. (2003). "The Description Logic Handbook"
// - Horrocks, I., & Sattler, U. (2007). "A Tableaux Decision Procedure for SHOIQ"
// - Motik, B., Shearer, R., & Horrocks, I. (2009). "Hypertableau Reasoning for Description Logics"

import Foundation
import Graph

// MARK: - TableauxReasoner

/// SHOIN(D) Tableaux Reasoner
///
/// A sound and complete reasoner for OWL DL that supports:
/// - S: Transitive roles
/// - H: Role hierarchy
/// - O: Nominals (oneOf)
/// - I: Inverse roles
/// - N: Number restrictions (cardinality)
/// - (D): Datatypes
///
/// **Usage**:
/// ```swift
/// let reasoner = TableauxReasoner(ontology: ontology)
///
/// // Check satisfiability
/// let result = reasoner.checkSatisfiability(.named("ex:Person"))
/// print(result.isSatisfiable)
///
/// // Check subsumption
/// let subsumes = reasoner.subsumes(
///     superClass: .named("ex:Person"),
///     subClass: .named("ex:Employee")
/// )
/// ```
public final class TableauxReasoner: @unchecked Sendable {

    // MARK: - Types

    /// Status of satisfiability check
    public enum SatisfiabilityStatus: Sendable {
        /// Expression is satisfiable (model found)
        case satisfiable
        /// Expression is unsatisfiable (clash found, all branches exhausted)
        case unsatisfiable
        /// Result unknown (timeout, resource limit reached)
        case unknown
    }

    /// Result of a satisfiability check
    public struct SatisfiabilityResult: Sendable {
        /// The satisfiability status
        public let status: SatisfiabilityStatus

        /// Convenience property for backward compatibility
        /// Returns true only for definite satisfiability, false otherwise
        public var isSatisfiable: Bool {
            status == .satisfiable
        }

        /// Returns true only for definite unsatisfiability
        public var isUnsatisfiable: Bool {
            status == .unsatisfiable
        }

        /// Returns true if the result is unknown (timeout/resource limit)
        public var isUnknown: Bool {
            status == .unknown
        }

        public let clash: ClashInfo?
        public let statistics: Statistics

        public init(status: SatisfiabilityStatus, clash: ClashInfo? = nil, statistics: Statistics) {
            self.status = status
            self.clash = clash
            self.statistics = statistics
        }

        /// Convenience initializer for backward compatibility
        public init(isSatisfiable: Bool, clash: ClashInfo? = nil, statistics: Statistics) {
            self.status = isSatisfiable ? .satisfiable : .unsatisfiable
            self.clash = clash
            self.statistics = statistics
        }
    }

    /// Reasoning statistics
    public struct Statistics: Sendable {
        public var nodesCreated: Int = 0
        public var edgesCreated: Int = 0
        public var ruleApplications: Int = 0
        public var backtrackCount: Int = 0
        public var maxDepth: Int = 0
        public var expansionSteps: Int = 0

        public init() {}
    }

    /// Configuration for the reasoner
    public struct Configuration: Sendable {
        /// Maximum expansion steps (safety limit)
        public let maxExpansionSteps: Int

        /// Timeout for reasoning operations (nil = no timeout)
        ///
        /// **Important**: Timeout is applied per-operation, not per-reasoner lifetime.
        /// Each call to `checkSatisfiability` calculates a fresh deadline from this value.
        public let timeout: TimeInterval?

        /// Whether to check OWL DL regularity before reasoning
        /// When enabled, the reasoner will validate that the ontology
        /// conforms to OWL DL restrictions for decidability.
        /// Default: true (recommended for safety)
        public let checkRegularity: Bool

        /// Whether to abort on regularity violations
        /// When true, reasoning returns `.unknown` if violations exist.
        /// When false, reasoning continues but may not terminate.
        /// Default: true (recommended for safety)
        public let abortOnRegularityViolations: Bool

        public init(
            maxExpansionSteps: Int = 100000,
            timeout: TimeInterval? = nil,
            checkRegularity: Bool = true,
            abortOnRegularityViolations: Bool = true
        ) {
            self.maxExpansionSteps = maxExpansionSteps
            self.timeout = timeout
            self.checkRegularity = checkRegularity
            self.abortOnRegularityViolations = abortOnRegularityViolations
        }
    }

    // MARK: - Properties

    private let ontology: OWLOntology
    private let roleHierarchy: RoleHierarchy
    private let classHierarchy: ClassHierarchy
    private let configuration: Configuration

    /// TBox constraints in NNF form
    private let tboxConstraints: [OWLClassExpression]

    /// Property chains from RBox
    private let propertyChains: [(chain: [String], implies: String)]

    /// Regularity violations (populated if checkRegularity is enabled)
    ///
    /// - Note: This array is only populated when `configuration.checkRegularity` is true.
    ///   When regularity checking is disabled, this will always be empty.
    public private(set) var regularityViolations: [OWLDLRegularityChecker.Violation] = []

    /// Whether the ontology passes OWL DL regularity check
    ///
    /// - Important: When `configuration.checkRegularity` is false, this property
    ///   always returns `true` because no violations are computed. This does NOT
    ///   guarantee the ontology is actually OWL DL compliant—it simply means
    ///   the check was skipped.
    ///
    /// To ensure an ontology is truly regular, either:
    /// - Use the default configuration (checkRegularity = true), or
    /// - Manually call `OWLOntology.checkOWLDLRegularity()` before creating the reasoner
    public var isRegular: Bool { regularityViolations.isEmpty }

    // MARK: - Initialization

    /// Initialize reasoner with ontology and configuration
    ///
    /// - Parameters:
    ///   - ontology: The OWL ontology to reason over
    ///   - configuration: Reasoner configuration (default: standard settings)
    public init(ontology: OWLOntology, configuration: Configuration = Configuration()) {
        self.ontology = ontology
        self.roleHierarchy = RoleHierarchy(ontology: ontology)
        self.classHierarchy = ClassHierarchy(ontology: ontology)
        self.configuration = configuration

        // Precompute TBox constraints
        self.tboxConstraints = Self.computeTBoxConstraints(from: ontology)

        // Extract property chains
        self.propertyChains = Self.extractPropertyChains(from: ontology, roleHierarchy: roleHierarchy)

        // Perform regularity check if enabled
        if configuration.checkRegularity {
            var checker = OWLDLRegularityChecker()
            self.regularityViolations = checker.check(ontology)
        }
    }

    /// Initialize reasoner with ontology (convenience)
    ///
    /// - Parameters:
    ///   - ontology: The OWL ontology to reason over
    ///   - maxExpansionSteps: Maximum expansion steps (default: 100000)
    public convenience init(ontology: OWLOntology, maxExpansionSteps: Int) {
        self.init(ontology: ontology, configuration: Configuration(maxExpansionSteps: maxExpansionSteps))
    }

    /// Compute TBox constraints in NNF form
    private static func computeTBoxConstraints(from ontology: OWLOntology) -> [OWLClassExpression] {
        var constraints: [OWLClassExpression] = []

        for axiom in ontology.axioms {
            switch axiom {
            case .subClassOf(let sub, let sup):
                // C ⊑ D becomes ¬C ⊔ D
                let constraint = OWLClassExpression.union([
                    OWLClassExpression.complement(sub).toNNF(),
                    sup.toNNF()
                ])
                constraints.append(constraint)

            case .equivalentClasses(let exprs):
                // A ≡ B becomes (A ⊑ B) ∧ (B ⊑ A)
                for i in 0..<exprs.count {
                    for j in 0..<exprs.count where i != j {
                        let constraint = OWLClassExpression.union([
                            OWLClassExpression.complement(exprs[i]).toNNF(),
                            exprs[j].toNNF()
                        ])
                        constraints.append(constraint)
                    }
                }

            default:
                break
            }
        }

        return constraints
    }

    /// Extract property chains from RBox
    private static func extractPropertyChains(
        from ontology: OWLOntology,
        roleHierarchy: RoleHierarchy
    ) -> [(chain: [String], implies: String)] {
        var chains: [(chain: [String], implies: String)] = []

        for axiom in ontology.axioms {
            if case .subPropertyChainOf(let chain, let sup) = axiom {
                chains.append((chain: chain, implies: sup))
            }
        }

        // Also get chains from role hierarchy
        for role in roleHierarchy.allRoles {
            for chain in roleHierarchy.propertyChains(for: role) {
                chains.append((chain: chain, implies: role))
            }
        }

        return chains
    }

    // MARK: - Satisfiability

    /// Check if a class expression is satisfiable
    ///
    /// A class is satisfiable if there exists a model where the class
    /// has at least one instance.
    ///
    /// - Parameter classExpr: The class expression to check
    /// - Returns: SatisfiabilityResult with detailed information
    public func checkSatisfiability(_ classExpr: OWLClassExpression) -> SatisfiabilityResult {
        var stats = Statistics()

        // Check regularity violations before reasoning
        if configuration.abortOnRegularityViolations && !regularityViolations.isEmpty {
            // Return unknown - ontology violates OWL DL and may not terminate
            return SatisfiabilityResult(status: .unknown, clash: nil, statistics: stats)
        }

        // Calculate deadline at operation start time (not at Configuration init)
        // This ensures each checkSatisfiability call gets a fresh deadline
        let operationDeadline = configuration.timeout.map { Date().addingTimeInterval($0) }

        // Create completion graph
        let graph = CompletionGraph(roleHierarchy: roleHierarchy, classHierarchy: classHierarchy)

        // Create root node with query concept in NNF
        let rootID = graph.createNode()
        let nnf = classExpr.toNNF()
        graph.addConcept(nnf, to: rootID)

        // Add TBox constraints to root
        for constraint in tboxConstraints {
            graph.addConcept(constraint, to: rootID)
        }

        stats.nodesCreated = 1

        // Run expansion algorithm
        let result = runExpansion(graph: graph, stats: &stats, deadline: operationDeadline)

        return result
    }

    /// Run the Tableaux expansion algorithm
    ///
    /// - Parameters:
    ///   - graph: The completion graph to expand
    ///   - stats: Statistics to update during expansion
    ///   - deadline: Optional deadline for timeout (calculated at operation start)
    private func runExpansion(graph: CompletionGraph, stats: inout Statistics, deadline: Date?) -> SatisfiabilityResult {

        while stats.expansionSteps < configuration.maxExpansionSteps {
            stats.expansionSteps += 1

            // Check deadline before each expansion step
            if let deadline = deadline, Date() > deadline {
                return SatisfiabilityResult(status: .unknown, clash: nil, statistics: stats)
            }

            // Phase 1: Update blocking
            graph.updateBlocking()

            // Phase 2: Check for clashes
            if let clash = checkForClashes(in: graph) {
                // Try backtracking
                if let (nodeID, choice) = graph.backtrack() {
                    stats.backtrackCount += 1
                    graph.addConcept(choice, to: nodeID)
                    continue
                }

                // No more choices - unsatisfiable
                return SatisfiabilityResult(
                    isSatisfiable: false,
                    clash: clash,
                    statistics: stats
                )
            }

            // Phase 3: Apply deterministic rules until saturation
            var deterministicChanged = true
            while deterministicChanged {
                deterministicChanged = false
                deterministicChanged = applyDeterministicRules(graph: graph, stats: &stats) || deterministicChanged
            }

            // Check for clashes after deterministic rules
            if let clash = checkForClashes(in: graph) {
                if let (nodeID, choice) = graph.backtrack() {
                    stats.backtrackCount += 1
                    graph.addConcept(choice, to: nodeID)
                    continue
                }
                return SatisfiabilityResult(isSatisfiable: false, clash: clash, statistics: stats)
            }

            // Phase 4: Apply generating rules
            let generatingChanged = applyGeneratingRules(graph: graph, stats: &stats)

            // Phase 5: Apply non-deterministic rules
            let nonDetResult = applyNonDeterministicRules(graph: graph, stats: &stats)

            // Phase 6: Apply property chains
            var chainChanged = false
            for (chain, implies) in propertyChains {
                if graph.applyPropertyChain(chain, implies: implies) {
                    chainChanged = true
                }
            }

            // Phase 7: Expand transitive roles
            var transitiveChanged = false
            for role in roleHierarchy.allRoles {
                if roleHierarchy.isTransitive(role) {
                    if graph.expandTransitiveRole(role) {
                        transitiveChanged = true
                    }
                }
            }

            // Check if any progress was made
            if !generatingChanged && !nonDetResult.madeChoice && !chainChanged && !transitiveChanged {
                // No more rules applicable - check completion
                if isComplete(graph: graph) {
                    let graphStats = graph.statistics
                    stats.nodesCreated = graphStats.nodes
                    stats.edgesCreated = graphStats.edges
                    stats.maxDepth = computeMaxDepth(graph: graph)

                    return SatisfiabilityResult(
                        isSatisfiable: true,
                        clash: nil,
                        statistics: stats
                    )
                }
            }
        }

        // Timeout - result unknown (reached expansion limit)
        return SatisfiabilityResult(status: .unknown, clash: nil, statistics: stats)
    }

    /// Apply all deterministic rules
    private func applyDeterministicRules(graph: CompletionGraph, stats: inout Statistics) -> Bool {
        var changed = false

        for nodeID in graph.nodes.keys {
            if graph.isBlocked(nodeID) { continue }

            // ⊓-rule
            if ExpansionRules.applyIntersectionRule(at: nodeID, in: graph) {
                changed = true
                stats.ruleApplications += 1
            }

            // ∀-rule
            if ExpansionRules.applyUniversalRule(at: nodeID, in: graph, roleHierarchy: roleHierarchy) {
                changed = true
                stats.ruleApplications += 1
            }

            // Domain rule
            if ExpansionRules.applyDomainRule(at: nodeID, in: graph, roleHierarchy: roleHierarchy) {
                changed = true
                stats.ruleApplications += 1
            }

            // Range rule
            if ExpansionRules.applyRangeRule(at: nodeID, in: graph, roleHierarchy: roleHierarchy) {
                changed = true
                stats.ruleApplications += 1
            }

            // Self rule
            if ExpansionRules.applySelfRule(at: nodeID, in: graph) {
                changed = true
                stats.ruleApplications += 1
            }

            // ≤-rule (merging)
            if ExpansionRules.applyMaxCardinalityRule(at: nodeID, in: graph) {
                changed = true
                stats.ruleApplications += 1
            }

            // Data existential
            if ExpansionRules.applyDataExistentialRule(at: nodeID, in: graph) {
                changed = true
                stats.ruleApplications += 1
            }
        }

        return changed
    }

    /// Apply generating rules (∃, ≥)
    private func applyGeneratingRules(graph: CompletionGraph, stats: inout Statistics) -> Bool {
        var changed = false

        for nodeID in graph.nodes.keys {
            if graph.isBlocked(nodeID) { continue }

            // ∃-rule
            if ExpansionRules.applyExistentialRule(at: nodeID, in: graph, tboxConstraints: tboxConstraints) {
                changed = true
                stats.ruleApplications += 1
            }

            // ≥-rule
            if ExpansionRules.applyMinCardinalityRule(at: nodeID, in: graph, tboxConstraints: tboxConstraints) {
                changed = true
                stats.ruleApplications += 1
            }
        }

        return changed
    }

    /// Apply non-deterministic rules (⊔, oneOf)
    private func applyNonDeterministicRules(
        graph: CompletionGraph,
        stats: inout Statistics
    ) -> (madeChoice: Bool, clash: ClashInfo?) {

        for nodeID in graph.nodes.keys {
            if graph.isBlocked(nodeID) { continue }

            // ⊔-rule
            let unionResult = ExpansionRules.applyUnionRule(at: nodeID, in: graph)
            if unionResult.applied, let expr = unionResult.unionExpr, let alts = unionResult.alternatives {
                // Create choice point and apply first choice
                _ = graph.createChoicePoint(nodeID: nodeID, expression: expr, alternatives: alts)
                graph.addConcept(alts[0], to: nodeID)
                stats.ruleApplications += 1
                return (true, nil)
            }

            // oneOf rule
            let oneOfResult = ExpansionRules.applyOneOfRule(at: nodeID, in: graph)
            if oneOfResult.needsChoice, let individuals = oneOfResult.alternatives, !individuals.isEmpty {
                // For oneOf, we need to merge with one of the nominals
                // This is a non-deterministic choice
                let nominalExprs = individuals.map { OWLClassExpression.oneOf([$0]) }
                _ = graph.createChoicePoint(
                    nodeID: nodeID,
                    expression: .oneOf(individuals),
                    alternatives: nominalExprs
                )

                // Merge with first nominal
                let firstNominalID = graph.getOrCreateNominal(individuals[0])
                graph.mergeNodes(survivor: firstNominalID, merged: nodeID)
                stats.ruleApplications += 1
                return (true, nil)
            }
        }

        return (false, nil)
    }

    /// Check for clashes in the entire graph
    private func checkForClashes(in graph: CompletionGraph) -> ClashInfo? {
        for nodeID in graph.nodes.keys {
            if let clash = ExpansionRules.detectClash(
                at: nodeID,
                in: graph,
                classHierarchy: classHierarchy,
                roleHierarchy: roleHierarchy
            ) {
                return clash
            }
        }
        return nil
    }

    /// Check if the tableau is complete
    private func isComplete(graph: CompletionGraph) -> Bool {
        for nodeID in graph.nodes.keys {
            if graph.isBlocked(nodeID) { continue }
            guard let node = graph.node(nodeID) else { continue }

            // Check if any rules are still applicable
            for concept in node.concepts {
                switch concept {
                case .intersection(let conjuncts):
                    for c in conjuncts {
                        if !node.concepts.contains(c) { return false }
                    }

                case .union(let disjuncts):
                    let hasAny = disjuncts.contains { node.concepts.contains($0) }
                    if !hasAny && !disjuncts.isEmpty { return false }

                case .someValuesFrom(let role, let filler):
                    let successors = graph.successors(of: nodeID, via: role)
                    let hasWitness = successors.contains { graph.hasConcept(filler, at: $0) }
                    if !hasWitness { return false }

                case .minCardinality(let role, let n, let filler):
                    let successors = graph.successors(of: nodeID, via: role)
                    let count: Int
                    if let f = filler {
                        count = successors.filter { graph.hasConcept(f, at: $0) }.count
                    } else {
                        count = successors.count
                    }
                    if count < n { return false }

                default:
                    break
                }
            }
        }

        return true
    }

    /// Compute maximum depth of nodes in the graph
    private func computeMaxDepth(graph: CompletionGraph) -> Int {
        var maxDepth = 0
        for node in graph.nodes.values {
            maxDepth = max(maxDepth, node.depth)
        }
        return maxDepth
    }

    // MARK: - Convenience Methods

    /// Check if a class expression is satisfiable (convenience)
    public func isSatisfiable(_ classExpr: OWLClassExpression) -> Bool {
        checkSatisfiability(classExpr).isSatisfiable
    }

    // MARK: - Subsumption

    /// Check if superClass subsumes subClass (subClass ⊑ superClass)
    ///
    /// Uses the reduction: C ⊑ D iff C ⊓ ¬D is unsatisfiable
    ///
    /// - Parameters:
    ///   - superClass: The potential superclass
    ///   - subClass: The potential subclass
    /// - Returns: true if subsumption definitely holds, false otherwise
    ///
    /// **Note**: Returns false if the satisfiability check is inconclusive (.unknown).
    /// Only returns true when C ⊓ ¬D is definitively unsatisfiable.
    public func subsumes(superClass: OWLClassExpression, subClass: OWLClassExpression) -> Bool {
        let test = OWLClassExpression.intersection([
            subClass,
            .complement(superClass)
        ])
        let result = checkSatisfiability(test)
        // Only claim subsumption when definitively unsatisfiable.
        // If unknown (timeout/resource limit), conservatively return false.
        return result.isUnsatisfiable
    }

    /// Check if two classes are equivalent
    public func areEquivalent(_ class1: OWLClassExpression, _ class2: OWLClassExpression) -> Bool {
        subsumes(superClass: class1, subClass: class2) &&
        subsumes(superClass: class2, subClass: class1)
    }

    /// Check if two classes are disjoint
    ///
    /// **Note**: Returns false if the satisfiability check is inconclusive (.unknown).
    public func areDisjoint(_ class1: OWLClassExpression, _ class2: OWLClassExpression) -> Bool {
        let test = OWLClassExpression.intersection([class1, class2])
        return checkSatisfiability(test).isUnsatisfiable
    }

    // MARK: - Instance Checking

    /// Check if an individual is an instance of a class
    ///
    /// Collects all ABox assertions for the individual and converts them
    /// to class expressions:
    /// - `classAssertion(ind, C)` → C
    /// - `dataPropertyAssertion(ind, P, v)` → dataHasValue(P, v)
    /// - `objectPropertyAssertion(ind, P, obj)` → hasValue(P, obj)
    ///
    /// Then checks if the conjunction of these expressions is subsumed by `classExpr`.
    public func isInstanceOf(individual: String, classExpr: OWLClassExpression) -> Bool {
        var individualTypes: [OWLClassExpression] = []

        for axiom in ontology.axioms {
            switch axiom {
            case .classAssertion(let ind, let type) where ind == individual:
                individualTypes.append(type)

            case .dataPropertyAssertion(let subject, let property, let value) where subject == individual:
                // Convert data property assertion to dataHasValue class expression
                // This enables Defined Class classification via equivalentClasses with dataHasValue
                individualTypes.append(.dataHasValue(property: property, literal: value))

            case .objectPropertyAssertion(let subject, let property, let object) where subject == individual:
                // Convert object property assertion to hasValue class expression
                individualTypes.append(.hasValue(property: property, individual: object))

            default:
                break
            }
        }

        if individualTypes.isEmpty {
            return false
        }

        // Check if the conjunction of individual types implies the class
        let individualType: OWLClassExpression
        if individualTypes.count == 1 {
            individualType = individualTypes[0]
        } else {
            individualType = .intersection(individualTypes)
        }

        return subsumes(superClass: classExpr, subClass: individualType)
    }

    // MARK: - Classification

    /// Classify all named classes in the ontology
    ///
    /// Computes the complete class hierarchy by checking subsumption
    /// between all pairs of named classes.
    ///
    /// - Returns: Updated ClassHierarchy with inferred relationships
    public func classify() -> ClassHierarchy {
        var result = classHierarchy

        let allClasses = Array(ontology.classSignature)

        for i in 0..<allClasses.count {
            for j in 0..<allClasses.count where i != j {
                let c1 = allClasses[i]
                let c2 = allClasses[j]

                if subsumes(superClass: .named(c1), subClass: .named(c2)) {
                    result.addSubsumption(subClass: c2, superClass: c1)
                }
            }
        }

        return result
    }

    /// Find all instances of a class expression
    public func instances(of classExpr: OWLClassExpression) -> Set<String> {
        var result = Set<String>()

        for individual in ontology.individuals {
            if isInstanceOf(individual: individual.iri, classExpr: classExpr) {
                result.insert(individual.iri)
            }
        }

        return result
    }

    /// Find all types of an individual
    public func types(of individual: String) -> Set<String> {
        var result = Set<String>()

        for class_ in ontology.classSignature {
            if isInstanceOf(individual: individual, classExpr: .named(class_)) {
                result.insert(class_)
            }
        }

        return result
    }
}

// MARK: - Legacy Clash Type (for compatibility)

extension TableauxReasoner {
    /// Legacy Clash enum for backward compatibility
    public enum Clash: Sendable, CustomStringConvertible {
        case complementClash(node: String, class1: String, class2: String)
        case disjointClash(node: String, class1: String, class2: String)
        case cardinalityClash(node: String, role: String, expected: Int, actual: Int)
        case datatypeClash(node: String, property: String, value: String, expected: String)
        case functionalClash(node: String, role: String, values: [String])
        case asymmetricReflexiveClash(node: String, role: String)
        case irreflexiveClash(node: String, role: String)

        public var description: String {
            switch self {
            case .complementClash(let node, let c1, let c2):
                return "Complement clash at \(node): \(c1) and \(c2)"
            case .disjointClash(let node, let c1, let c2):
                return "Disjoint clash at \(node): \(c1) and \(c2)"
            case .cardinalityClash(let node, let role, let expected, let actual):
                return "Cardinality clash at \(node): \(role) expected \(expected), got \(actual)"
            case .datatypeClash(let node, let prop, let value, let expected):
                return "Datatype clash at \(node): \(prop) value \(value) not in \(expected)"
            case .functionalClash(let node, let role, let values):
                return "Functional clash at \(node): \(role) has multiple values \(values)"
            case .asymmetricReflexiveClash(let node, let role):
                return "Asymmetric reflexive clash at \(node): \(role)(x,x)"
            case .irreflexiveClash(let node, let role):
                return "Irreflexive clash at \(node): \(role)(x,x)"
            }
        }
    }
}
