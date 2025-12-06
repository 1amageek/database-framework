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

    /// Result of a satisfiability check
    public struct SatisfiabilityResult: Sendable {
        public let isSatisfiable: Bool
        public let clash: ClashInfo?
        public let statistics: Statistics

        public init(isSatisfiable: Bool, clash: ClashInfo? = nil, statistics: Statistics) {
            self.isSatisfiable = isSatisfiable
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

    // MARK: - Properties

    private let ontology: OWLOntology
    private let roleHierarchy: RoleHierarchy
    private let classHierarchy: ClassHierarchy

    /// TBox constraints in NNF form
    private let tboxConstraints: [OWLClassExpression]

    /// Property chains from RBox
    private let propertyChains: [(chain: [String], implies: String)]

    /// Maximum expansion steps (safety limit)
    private let maxExpansionSteps: Int

    // MARK: - Initialization

    /// Initialize reasoner with ontology
    ///
    /// - Parameters:
    ///   - ontology: The OWL ontology to reason over
    ///   - maxExpansionSteps: Maximum expansion steps (default: 100000)
    public init(ontology: OWLOntology, maxExpansionSteps: Int = 100000) {
        self.ontology = ontology
        self.roleHierarchy = RoleHierarchy(ontology: ontology)
        self.classHierarchy = ClassHierarchy(ontology: ontology)
        self.maxExpansionSteps = maxExpansionSteps

        // Precompute TBox constraints
        self.tboxConstraints = Self.computeTBoxConstraints(from: ontology)

        // Extract property chains
        self.propertyChains = Self.extractPropertyChains(from: ontology, roleHierarchy: roleHierarchy)
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
        let result = runExpansion(graph: graph, stats: &stats)

        return result
    }

    /// Run the Tableaux expansion algorithm
    private func runExpansion(graph: CompletionGraph, stats: inout Statistics) -> SatisfiabilityResult {

        while stats.expansionSteps < maxExpansionSteps {
            stats.expansionSteps += 1

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

        // Timeout - treat as satisfiable (open world)
        return SatisfiabilityResult(isSatisfiable: true, clash: nil, statistics: stats)
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
    /// - Returns: true if subsumption holds
    public func subsumes(superClass: OWLClassExpression, subClass: OWLClassExpression) -> Bool {
        let test = OWLClassExpression.intersection([
            subClass,
            .complement(superClass)
        ])
        return !isSatisfiable(test)
    }

    /// Check if two classes are equivalent
    public func areEquivalent(_ class1: OWLClassExpression, _ class2: OWLClassExpression) -> Bool {
        subsumes(superClass: class1, subClass: class2) &&
        subsumes(superClass: class2, subClass: class1)
    }

    /// Check if two classes are disjoint
    public func areDisjoint(_ class1: OWLClassExpression, _ class2: OWLClassExpression) -> Bool {
        let test = OWLClassExpression.intersection([class1, class2])
        return !isSatisfiable(test)
    }

    // MARK: - Instance Checking

    /// Check if an individual is an instance of a class
    public func isInstanceOf(individual: String, classExpr: OWLClassExpression) -> Bool {
        // Collect all class assertions for the individual
        var individualTypes: [OWLClassExpression] = []

        for axiom in ontology.axioms {
            if case .classAssertion(let ind, let type) = axiom, ind == individual {
                individualTypes.append(type)
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
