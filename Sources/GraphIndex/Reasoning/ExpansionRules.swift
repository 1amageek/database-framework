// ExpansionRules.swift
// GraphIndex - Tableaux expansion rules for SHOIN(D)
//
// Implements all expansion rules for the Tableaux algorithm:
// - Deterministic rules (⊓, ∀, choose, etc.)
// - Non-deterministic rules (⊔)
// - Generating rules (∃, ≥)
// - Number restriction rules (≤)
//
// Reference:
// - Baader, F., et al. (2003). "The Description Logic Handbook", Chapter 2
// - Horrocks, I., & Sattler, U. (2007). "A Tableaux Decision Procedure for SHOIQ"

import Foundation
import Graph

// MARK: - Expansion Rule Protocol

/// Result of applying an expansion rule
public enum RuleApplicationResult: Sendable {
    case applied           // Rule was applied successfully
    case notApplicable     // Rule preconditions not met
    case clash(ClashInfo)  // Rule application resulted in a clash
}

/// Information about a clash (contradiction)
public struct ClashInfo: Sendable, CustomStringConvertible {
    public enum ClashType: Sendable {
        case complement          // C and ¬C
        case disjoint           // Disjoint classes
        case bottom             // owl:Nothing
        case maxCardinality     // Too many role fillers
        case functional         // Multiple values for functional role
        case irreflexive        // R(x,x) for irreflexive R
        case asymmetric         // R(x,y) and R(y,x) for asymmetric R
        case datatype           // Datatype constraint violation
    }

    public let type: ClashType
    public let nodeID: NodeID
    public let details: String

    public var description: String {
        "Clash(\(type)) at \(nodeID): \(details)"
    }
}

// MARK: - Expansion Rules

/// Container for all Tableaux expansion rules
public struct ExpansionRules {

    // MARK: - Rule Application Context

    /// Context for rule application
    struct RuleContext {
        let graph: CompletionGraph
        let roleHierarchy: RoleHierarchy
        let classHierarchy: ClassHierarchy
        let tboxConstraints: [OWLClassExpression]
    }

    // MARK: - Clash Detection

    /// Check for clashes at a node
    static func detectClash(
        at nodeID: NodeID,
        in graph: CompletionGraph,
        classHierarchy: ClassHierarchy,
        roleHierarchy: RoleHierarchy
    ) -> ClashInfo? {
        guard let node = graph.node(nodeID) else { return nil }
        if graph.isBlocked(nodeID) { return nil }

        let concepts = node.concepts

        // 1. Check for owl:Nothing
        if concepts.contains(.nothing) {
            return ClashInfo(type: .bottom, nodeID: nodeID, details: "Contains owl:Nothing")
        }

        // 1b. Check for empty oneOf (semantically equivalent to Nothing)
        for concept in concepts {
            if case .oneOf(let individuals) = concept, individuals.isEmpty {
                return ClashInfo(type: .bottom, nodeID: nodeID, details: "Contains empty oneOf (equivalent to owl:Nothing)")
            }
        }

        // 2. Check complement clashes: C and ¬C
        for concept in concepts {
            // Direct complement
            if case .complement(let inner) = concept {
                if concepts.contains(inner) {
                    return ClashInfo(
                        type: .complement,
                        nodeID: nodeID,
                        details: "\(inner.description) and \(concept.description)"
                    )
                }
            }

            // Named class and its negation
            if case .named(let iri) = concept {
                let negation = OWLClassExpression.complement(.named(iri))
                if concepts.contains(negation) {
                    return ClashInfo(
                        type: .complement,
                        nodeID: nodeID,
                        details: "\(iri) and ¬\(iri)"
                    )
                }
            }
        }

        // 3. Check disjoint classes
        let namedClasses = concepts.compactMap { concept -> String? in
            if case .named(let iri) = concept { return iri }
            return nil
        }

        for i in 0..<namedClasses.count {
            for j in (i+1)..<namedClasses.count {
                if classHierarchy.areDisjoint(namedClasses[i], namedClasses[j]) {
                    return ClashInfo(
                        type: .disjoint,
                        nodeID: nodeID,
                        details: "\(namedClasses[i]) and \(namedClasses[j]) are disjoint"
                    )
                }
            }
        }

        // 4. Check conflicting cardinality constraints
        // ≥n R.C ⊓ ≤m R.C where n > m is immediately unsatisfiable
        var minConstraints: [(role: String, n: Int, filler: OWLClassExpression?)] = []
        var maxConstraints: [(role: String, n: Int, filler: OWLClassExpression?)] = []

        for concept in concepts {
            if case .minCardinality(let role, let n, let filler) = concept {
                minConstraints.append((role, n, filler))
            }
            if case .maxCardinality(let role, let n, let filler) = concept {
                maxConstraints.append((role, n, filler))
            }
        }

        for minC in minConstraints {
            for maxC in maxConstraints {
                // Same role, compatible fillers (or both thing/nil)
                if minC.role == maxC.role {
                    let fillersCompatible = (minC.filler == maxC.filler) ||
                                          (minC.filler == nil || minC.filler == .thing) ||
                                          (maxC.filler == nil || maxC.filler == .thing)
                    if fillersCompatible && minC.n > maxC.n {
                        return ClashInfo(
                            type: .maxCardinality,
                            nodeID: nodeID,
                            details: "≥\(minC.n) \(minC.role) conflicts with ≤\(maxC.n) \(maxC.role)"
                        )
                    }
                }
            }
        }

        // 5. Check functional role violations
        for (role, successors) in node.outgoingEdges {
            if roleHierarchy.isFunctional(role) && successors.count > 1 {
                return ClashInfo(
                    type: .functional,
                    nodeID: nodeID,
                    details: "Functional role \(role) has \(successors.count) fillers"
                )
            }
        }

        // 6. Check irreflexive violation
        for (role, successors) in node.outgoingEdges {
            if roleHierarchy.isIrreflexive(role) && successors.contains(nodeID) {
                return ClashInfo(
                    type: .irreflexive,
                    nodeID: nodeID,
                    details: "Irreflexive role \(role) has self-loop"
                )
            }
        }

        // 7. Check asymmetric violation
        for (role, successors) in node.outgoingEdges {
            if roleHierarchy.isAsymmetric(role) {
                for successor in successors {
                    let reverseSuccessors = graph.successors(of: successor, via: role)
                    if reverseSuccessors.contains(nodeID) {
                        return ClashInfo(
                            type: .asymmetric,
                            nodeID: nodeID,
                            details: "Asymmetric role \(role): both \(nodeID)→\(successor) and \(successor)→\(nodeID)"
                        )
                    }
                }
            }
        }

        return nil
    }

    // MARK: - Deterministic Rules

    /// ⊓-rule: If C ⊓ D ∈ L(x) and {C, D} ⊄ L(x), then L(x) := L(x) ∪ {C, D}
    static func applyIntersectionRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for concept in node.concepts {
            // Skip if already processed
            if node.processedIntersections.contains(concept) { continue }

            if case .intersection(let conjuncts) = concept {
                node.processedIntersections.insert(concept)

                for conjunct in conjuncts {
                    if graph.addConcept(conjunct, to: nodeID) {
                        changed = true
                    }
                }
            }
        }

        return changed
    }

    /// ∀-rule: If ∀R.C ∈ L(x) and (x,y):R ∈ E and C ∉ L(y), then L(y) := L(y) ∪ {C}
    static func applyUniversalRule(
        at nodeID: NodeID,
        in graph: CompletionGraph,
        roleHierarchy: RoleHierarchy
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for concept in node.concepts {
            if node.processedUniversals.contains(concept) { continue }

            if case .allValuesFrom(let role, let filler) = concept {
                // Get all successors via R and its sub-roles
                var allSuccessors = graph.successors(of: nodeID, via: role)
                for subRole in roleHierarchy.directSubRoles(of: role) {
                    allSuccessors.formUnion(graph.successors(of: nodeID, via: subRole))
                }

                for successorID in allSuccessors {
                    if graph.addConcept(filler, to: successorID) {
                        changed = true
                    }
                }

                // Also propagate via inverse role
                if let inverseRole = roleHierarchy.inverse(of: role) {
                    let predecessors = graph.predecessors(of: nodeID, via: inverseRole)
                    for predID in predecessors {
                        if graph.addConcept(filler, to: predID) {
                            changed = true
                        }
                    }
                }

                // Mark as processed only if we've seen all current successors
                // (we might add more successors later)
            }
        }

        return changed
    }

    /// Domain inference: If R(x,y), then domain(R) ∈ L(x)
    static func applyDomainRule(
        at nodeID: NodeID,
        in graph: CompletionGraph,
        roleHierarchy: RoleHierarchy
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for (role, _) in node.outgoingEdges {
            for domain in roleHierarchy.domains(of: role) {
                if graph.addConcept(domain, to: nodeID) {
                    changed = true
                }
            }
        }

        return changed
    }

    /// Range inference: If R(x,y), then range(R) ∈ L(y)
    static func applyRangeRule(
        at nodeID: NodeID,
        in graph: CompletionGraph,
        roleHierarchy: RoleHierarchy
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for (role, _) in node.incomingEdges {
            for range in roleHierarchy.ranges(of: role) {
                if graph.addConcept(range, to: nodeID) {
                    changed = true
                }
            }
        }

        return changed
    }

    /// Self rule: If ∃R.Self ∈ L(x), then add (x,x):R
    static func applySelfRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for concept in node.concepts {
            if case .hasSelf(let role) = concept {
                if graph.addEdge(from: nodeID, role: role, to: nodeID) {
                    changed = true
                }
            }
        }

        return changed
    }

    // MARK: - Non-Deterministic Rules

    /// ⊔-rule: If C ⊔ D ∈ L(x) and {C, D} ∩ L(x) = ∅, then L(x) := L(x) ∪ {C} (or {D})
    /// Returns: (applied, needsChoice, alternatives)
    static func applyUnionRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> (applied: Bool, unionExpr: OWLClassExpression?, alternatives: [OWLClassExpression]?) {
        guard let node = graph.node(nodeID) else {
            return (false, nil, nil)
        }
        if graph.isBlocked(nodeID) { return (false, nil, nil) }

        for concept in node.concepts {
            if node.processedUnions.contains(concept) { continue }

            if case .union(let disjuncts) = concept {
                // Check if any disjunct is already present
                let hasDisjunct = disjuncts.contains { node.concepts.contains($0) }

                if !hasDisjunct && !disjuncts.isEmpty {
                    node.processedUnions.insert(concept)
                    // Return alternatives for choice point creation
                    return (true, concept, disjuncts)
                }
            }
        }

        return (false, nil, nil)
    }

    // MARK: - Generating Rules

    /// ∃-rule: If ∃R.C ∈ L(x) and there is no y with (x,y):R ∈ E and C ∈ L(y),
    /// then create new node y with L(y) = {C} and add (x,y):R
    static func applyExistentialRule(
        at nodeID: NodeID,
        in graph: CompletionGraph,
        tboxConstraints: [OWLClassExpression]
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for concept in node.concepts {
            if node.processedExistentials.contains(concept) { continue }

            switch concept {
            case .someValuesFrom(let role, let filler):
                // Check if witness exists
                let successors = graph.successors(of: nodeID, via: role)
                let hasWitness = successors.contains { graph.hasConcept(filler, at: $0) }

                if !hasWitness {
                    node.processedExistentials.insert(concept)

                    // Create new node
                    let newNodeID = graph.createNode(parent: nodeID)
                    graph.addConcept(filler, to: newNodeID)

                    // Add TBox constraints
                    for constraint in tboxConstraints {
                        graph.addConcept(constraint, to: newNodeID)
                    }

                    // Add edge
                    graph.addEdge(from: nodeID, role: role, to: newNodeID)
                    changed = true
                }

            case .hasValue(let role, let individual):
                // Create/get nominal node
                let nominalID = graph.getOrCreateNominal(individual)
                if graph.addEdge(from: nodeID, role: role, to: nominalID) {
                    changed = true
                }

            default:
                break
            }
        }

        return changed
    }

    /// ≥-rule: If ≥n R.C ∈ L(x) and there are less than n R-successors y with C ∈ L(y),
    /// then create new nodes to meet the requirement
    static func applyMinCardinalityRule(
        at nodeID: NodeID,
        in graph: CompletionGraph,
        tboxConstraints: [OWLClassExpression]
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for concept in node.concepts {
            if case .minCardinality(let role, let n, let filler) = concept {
                let successors = graph.successors(of: nodeID, via: role)
                let qualifiedCount: Int
                // owl:Thing (⊤) is satisfied by every individual, so treat it like no filler
                if let f = filler, f != .thing {
                    qualifiedCount = successors.filter { graph.hasConcept(f, at: $0) }.count
                } else {
                    qualifiedCount = successors.count
                }

                // Create missing successors
                if qualifiedCount < n {
                    for _ in qualifiedCount..<n {
                        let newNodeID = graph.createNode(parent: nodeID)

                        if let f = filler {
                            graph.addConcept(f, to: newNodeID)
                        }

                        // Add TBox constraints
                        for constraint in tboxConstraints {
                            graph.addConcept(constraint, to: newNodeID)
                        }

                        graph.addEdge(from: nodeID, role: role, to: newNodeID)
                        changed = true
                    }
                }
            }
        }

        return changed
    }

    /// ≤-rule: If ≤n R.C ∈ L(x) and there are more than n R-successors y with C ∈ L(y),
    /// then merge some successors
    static func applyMaxCardinalityRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for concept in node.concepts {
            if case .maxCardinality(let role, let n, let filler) = concept {
                let successors = Array(graph.successors(of: nodeID, via: role))
                let qualified: [NodeID]
                // owl:Thing (⊤) is satisfied by every individual, so treat it like no filler
                if let f = filler, f != .thing {
                    qualified = successors.filter { graph.hasConcept(f, at: $0) }
                } else {
                    qualified = successors
                }

                // Merge if too many
                if qualified.count > n {
                    // Choose nodes to merge (prefer non-nominals)
                    let sortedByPriority = qualified.sorted { a, b in
                        // Nominals have lower priority for being merged away
                        (a.isNominalNode ? 1 : 0) < (b.isNominalNode ? 1 : 0)
                    }

                    let survivor = sortedByPriority[0]
                    let toMerge = Array(sortedByPriority.dropFirst(n))

                    for mergeID in toMerge {
                        graph.mergeNodes(survivor: survivor, merged: mergeID)
                        changed = true
                    }
                }
            }
        }

        return changed
    }

    // MARK: - Data Property Rules

    /// Data existential: If ∃P.D ∈ L(x), ensure x has a data value in D
    static func applyDataExistentialRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> Bool {
        guard let node = graph.node(nodeID) else { return false }
        if graph.isBlocked(nodeID) { return false }

        var changed = false

        for concept in node.concepts {
            if case .dataSomeValuesFrom(let property, let dataRange) = concept {
                // Check if we already have a value
                let hasValue = !(node.dataValues[property]?.isEmpty ?? true)

                if !hasValue {
                    // Generate a witness value
                    if let value = generateWitnessValue(for: dataRange) {
                        if graph.addDataValue(value, property: property, to: nodeID) {
                            changed = true
                        }
                    }
                }
            }

            if case .dataHasValue(let property, let value) = concept {
                if graph.addDataValue(value, property: property, to: nodeID) {
                    changed = true
                }
            }
        }

        return changed
    }

    /// Generate a witness value for a data range
    private static func generateWitnessValue(for dataRange: OWLDataRange) -> OWLLiteral? {
        switch dataRange {
        case .datatype(let iri):
            switch iri {
            case "xsd:string": return OWLLiteral(lexicalForm: "witness", datatype: iri)
            case "xsd:integer": return OWLLiteral(lexicalForm: "0", datatype: iri)
            case "xsd:decimal": return OWLLiteral(lexicalForm: "0.0", datatype: iri)
            case "xsd:double": return OWLLiteral(lexicalForm: "0.0", datatype: iri)
            case "xsd:float": return OWLLiteral(lexicalForm: "0.0", datatype: iri)
            case "xsd:boolean": return OWLLiteral(lexicalForm: "true", datatype: iri)
            case "xsd:dateTime": return OWLLiteral(lexicalForm: "2000-01-01T00:00:00", datatype: iri)
            default: return OWLLiteral(lexicalForm: "", datatype: iri)
            }

        case .dataOneOf(let values):
            return values.first

        case .datatypeRestriction(let baseType, _):
            return generateWitnessValue(for: .datatype(baseType))

        default:
            return nil
        }
    }

    // MARK: - oneOf (Nominal) Rule

    /// oneOf rule: If {a₁, ..., aₙ} ∈ L(x), then x must be equal to some aᵢ
    /// This creates a non-deterministic choice
    static func applyOneOfRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> (needsChoice: Bool, alternatives: [String]?) {
        guard let node = graph.node(nodeID) else { return (false, nil) }
        if graph.isBlocked(nodeID) { return (false, nil) }

        // Skip if already a nominal
        if nodeID.isNominalNode { return (false, nil) }

        for concept in node.concepts {
            if case .oneOf(let individuals) = concept {
                // This requires merging x with one of the nominals
                return (true, individuals)
            }
        }

        return (false, nil)
    }
}
