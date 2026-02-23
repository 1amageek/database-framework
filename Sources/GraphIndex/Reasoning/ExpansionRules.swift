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
        case nominal            // Attempted merge of distinct nominals (UNA violation)
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

    // MARK: - Witness Result

    /// Result of witness generation for a data range.
    ///
    /// Eliminates the ambiguity of `OWLLiteral?` where `nil` conflated
    /// "provably unsatisfiable" with "not yet implemented".
    private enum WitnessResult {
        /// A concrete witness value was generated
        case witness(OWLLiteral)
        /// The data range is provably empty (e.g., contradictory facets)
        case unsatisfiable
        /// Cannot determine satisfiability — sound but incomplete
        case unsupported
    }

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
        // Phase 5: O(1) check using pre-maintained complement clash index.
        // complementClashes is non-empty iff a concept and its complement coexist.
        if !node.complementClashes.isEmpty {
            // Report the first clash found for diagnostics
            let clashConcept = node.complementClashes.first!
            return ClashInfo(
                type: .complement,
                nodeID: nodeID,
                details: "\(clashConcept.description) and \(OWLClassExpression.complement(clashConcept).description)"
            )
        }

        // 3. Check disjoint classes
        // Phase 5: Use pre-maintained namedClassIRIs set instead of O(c) compactMap
        let namedClasses = Array(node.namedClassIRIs)

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
                graph.recordProcessedFlag(.intersection, concept: concept, at: nodeID)

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
                    graph.recordProcessedFlag(.union, concept: concept, at: nodeID)
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
                    graph.recordProcessedFlag(.existential, concept: concept, at: nodeID)

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
    /// then merge some successors.
    ///
    /// Returns `.clash` if merging would violate the Unique Name Assumption
    /// (i.e., two distinct nominals must be merged).
    /// Reference: Horrocks & Sattler (2007), Section 5.1
    static func applyMaxCardinalityRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> RuleApplicationResult {
        guard let node = graph.node(nodeID) else { return .notApplicable }
        if graph.isBlocked(nodeID) { return .notApplicable }

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
                    // Choose nodes to merge (nominals survive, non-nominals are merge targets)
                    let sortedByPriority = qualified.sorted { a, b in
                        // Nominals first (survive); non-nominals last (get merged)
                        a.isNominalNode && !b.isNominalNode
                    }

                    let survivor = sortedByPriority[0]
                    let toMerge = Array(sortedByPriority.dropFirst(n))

                    for mergeID in toMerge {
                        let mergeResult = graph.mergeNodes(survivor: survivor, merged: mergeID)
                        if case .nominalClash(let s, let m) = mergeResult {
                            return .clash(ClashInfo(
                                type: .nominal,
                                nodeID: nodeID,
                                details: "Cannot merge distinct nominals \(s) and \(m) — Unique Name Assumption violation (≤\(n) \(role))"
                            ))
                        }
                        changed = true
                    }
                }
            }
        }

        return changed ? .applied : .notApplicable
    }

    // MARK: - Data Property Rules

    /// Data existential: If ∃P.D ∈ L(x), ensure x has a data value in D
    ///
    /// Returns `.clash(.datatype, ...)` when the data range is provably unsatisfiable
    /// (e.g., contradictory facets like minInclusive=10, maxExclusive=5).
    static func applyDataExistentialRule(
        at nodeID: NodeID,
        in graph: CompletionGraph
    ) -> RuleApplicationResult {
        guard let node = graph.node(nodeID) else { return .notApplicable }
        if graph.isBlocked(nodeID) { return .notApplicable }

        var changed = false

        for concept in node.concepts {
            if case .dataSomeValuesFrom(let property, let dataRange) = concept {
                let hasValue = !(node.dataValues[property]?.isEmpty ?? true)

                if !hasValue {
                    switch generateWitnessValue(for: dataRange) {
                    case .witness(let value):
                        if graph.addDataValue(value, property: property, to: nodeID) {
                            changed = true
                        }
                    case .unsatisfiable:
                        return .clash(ClashInfo(
                            type: .datatype,
                            nodeID: nodeID,
                            details: "Unsatisfiable data range for property \(property): \(dataRange)"
                        ))
                    case .unsupported:
                        break  // Sound but incomplete
                    }
                }
            }

            if case .dataHasValue(let property, let value) = concept {
                if graph.addDataValue(value, property: property, to: nodeID) {
                    changed = true
                }
            }
        }

        return changed ? .applied : .notApplicable
    }

    // MARK: - Witness Generation

    /// Canonical witness values for unrestricted XSD datatypes
    private static func canonicalWitness(for iri: String) -> OWLLiteral {
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
    }

    /// Generate a witness value for a data range.
    ///
    /// Handles all 6 `OWLDataRange` cases:
    /// - `.datatype`: canonical value
    /// - `.dataOneOf`: first enumerated value
    /// - `.datatypeRestriction`: facet-aware generation
    /// - `.dataUnionOf`: try each sub-range, return first success
    /// - `.dataIntersectionOf`: generate candidate, validate against full intersection
    /// - `.dataComplementOf`: try canonical values, validate against complement
    ///
    /// Reference: XSD 1.1 Part 2, Section 4.3 — Constraining Facets
    private static func generateWitnessValue(for dataRange: OWLDataRange) -> WitnessResult {
        switch dataRange {
        case .datatype(let iri):
            return .witness(canonicalWitness(for: iri))

        case .dataOneOf(let values):
            if values.isEmpty {
                return .unsatisfiable
            }
            return .witness(values[0])

        case .datatypeRestriction(let baseType, let facets):
            return generateFacetAwareWitness(baseType: baseType, facets: facets)

        case .dataUnionOf(let ranges):
            return generateUnionWitness(ranges: ranges)

        case .dataIntersectionOf(let ranges):
            return generateIntersectionWitness(ranges: ranges, fullRange: dataRange)

        case .dataComplementOf(let inner):
            return generateComplementWitness(inner: inner)
        }
    }

    /// Witness for union: try each sub-range, return first success.
    ///
    /// If all sub-ranges are unsatisfiable, the union is unsatisfiable.
    /// If any sub-range is unsupported and none succeeded, result is unsupported.
    private static func generateUnionWitness(ranges: [OWLDataRange]) -> WitnessResult {
        if ranges.isEmpty { return .unsatisfiable }
        var hasUnsupported = false
        for range in ranges {
            switch generateWitnessValue(for: range) {
            case .witness(let value):
                return .witness(value)
            case .unsatisfiable:
                continue
            case .unsupported:
                hasUnsupported = true
            }
        }
        return hasUnsupported ? .unsupported : .unsatisfiable
    }

    /// Witness for intersection: generate candidates from sub-ranges,
    /// validate each against the full intersection using `OWLDatatypeValidator`.
    private static func generateIntersectionWitness(
        ranges: [OWLDataRange],
        fullRange: OWLDataRange
    ) -> WitnessResult {
        // Empty intersection = universal set (vacuously true)
        if ranges.isEmpty { return .witness(canonicalWitness(for: "xsd:string")) }
        let validator = OWLDatatypeValidator()

        for range in ranges {
            switch generateWitnessValue(for: range) {
            case .witness(let candidate):
                if validator.validate(candidate, against: fullRange) == nil {
                    return .witness(candidate)
                }
            case .unsatisfiable:
                // If any sub-range is unsatisfiable, the intersection is unsatisfiable
                return .unsatisfiable
            case .unsupported:
                continue
            }
        }
        // All candidates failed validation against the full intersection.
        // We cannot prove the intersection is empty, so remain sound.
        return .unsupported
    }

    /// Witness for complement: try diverse canonical values,
    /// return the first that does NOT belong to the inner range.
    private static func generateComplementWitness(inner: OWLDataRange) -> WitnessResult {
        let validator = OWLDatatypeValidator()
        let candidates: [OWLLiteral] = [
            OWLLiteral(lexicalForm: "witness", datatype: "xsd:string"),
            OWLLiteral(lexicalForm: "0", datatype: "xsd:integer"),
            OWLLiteral(lexicalForm: "true", datatype: "xsd:boolean"),
            OWLLiteral(lexicalForm: "0.0", datatype: "xsd:double"),
        ]
        for candidate in candidates {
            // If validation against inner range FAILS, the candidate is in the complement
            if validator.validate(candidate, against: inner) != nil {
                return .witness(candidate)
            }
        }
        return .unsupported
    }

    /// Generate a witness that satisfies facet constraints.
    ///
    /// For numeric types: finds a value within [min, max] bounds.
    /// For string types: generates a string satisfying length constraints.
    /// Post-validates with `OWLDatatypeValidator` for pattern facets.
    private static func generateFacetAwareWitness(
        baseType: String,
        facets: [FacetRestriction]
    ) -> WitnessResult {
        let isNumeric = ["xsd:integer", "xsd:decimal", "xsd:double", "xsd:float"].contains(baseType)

        if isNumeric {
            var lower: Double = -.infinity
            var upper: Double = .infinity
            var lowerInclusive = true
            var upperInclusive = true

            for restriction in facets {
                guard let v = restriction.value.doubleValue else { continue }
                switch restriction.facet {
                case .minInclusive:
                    if v > lower {
                        lower = v; lowerInclusive = true
                    }
                case .minExclusive:
                    if v > lower || (v == lower && lowerInclusive) {
                        lower = v; lowerInclusive = false
                    }
                case .maxInclusive:
                    if v < upper {
                        upper = v; upperInclusive = true
                    }
                case .maxExclusive:
                    if v < upper || (v == upper && upperInclusive) {
                        upper = v; upperInclusive = false
                    }
                default:
                    break
                }
            }

            // Check for contradictory facets (empty range)
            if lower > upper { return .unsatisfiable }
            if lower == upper && (!lowerInclusive || !upperInclusive) { return .unsatisfiable }

            // Integer types: use direct integer arithmetic
            if baseType == "xsd:integer" {
                let intResult = generateIntegerWitness(
                    lower: lower, upper: upper,
                    lowerInclusive: lowerInclusive, upperInclusive: upperInclusive
                )
                // Post-validate against non-numeric facets (pattern, totalDigits, etc.)
                if case .witness(let intLiteral) = intResult {
                    let hasNonNumericFacets = facets.contains { restriction in
                        switch restriction.facet {
                        case .minInclusive, .maxInclusive, .minExclusive, .maxExclusive:
                            return false
                        default:
                            return true
                        }
                    }
                    if hasNonNumericFacets {
                        let validator = OWLDatatypeValidator()
                        if validator.validateFacets(intLiteral, facets: facets) != nil {
                            return .unsupported
                        }
                    }
                }
                return intResult
            }

            // Floating-point types
            var witness: Double
            if lower == -.infinity && upper == .infinity {
                witness = 0.0
            } else if lower == -.infinity {
                witness = upperInclusive ? upper : upper.nextDown
            } else if upper == .infinity {
                witness = lowerInclusive ? lower : lower.nextUp
            } else {
                witness = (lower + upper) / 2.0
            }

            // IEEE 754 adjacent-value adjustment for exclusive bounds
            if !lowerInclusive && witness <= lower {
                witness = lower.nextUp
            }
            if !upperInclusive && witness >= upper {
                witness = upper.nextDown
            }
            // Final validation: NaN or out-of-bounds means unsatisfiable
            if witness.isNaN || witness < lower || witness > upper {
                return .unsatisfiable
            }
            if !lowerInclusive && witness == lower { return .unsatisfiable }
            if !upperInclusive && witness == upper { return .unsatisfiable }

            let numericLiteral = OWLLiteral(lexicalForm: "\(witness)", datatype: baseType)

            // Post-validate against non-numeric facets (pattern, totalDigits, etc.)
            let hasNonNumericFacets = facets.contains { restriction in
                switch restriction.facet {
                case .minInclusive, .maxInclusive, .minExclusive, .maxExclusive:
                    return false
                default:
                    return true
                }
            }
            if hasNonNumericFacets {
                let validator = OWLDatatypeValidator()
                if validator.validateFacets(numericLiteral, facets: facets) != nil {
                    return .unsupported
                }
            }

            return .witness(numericLiteral)
        }

        // String/URI facet support (length + pattern constraints)
        if baseType == "xsd:string" || baseType == "xsd:anyURI" {
            var minLen = 0
            var maxLen = Int.max
            for restriction in facets {
                guard let v = restriction.value.intValue else { continue }
                switch restriction.facet {
                case .minLength: minLen = max(minLen, v)
                case .maxLength: maxLen = min(maxLen, v)
                case .length:    minLen = v; maxLen = v
                default: break
                }
            }
            if minLen > maxLen { return .unsatisfiable }

            let candidateLiteral = OWLLiteral(
                lexicalForm: String(repeating: "a", count: minLen),
                datatype: baseType
            )

            // Post-validate against pattern facets
            let validator = OWLDatatypeValidator()
            if validator.validateFacets(candidateLiteral, facets: facets) != nil {
                // Pattern or other facets rejected our candidate.
                // We cannot prove the range is empty, so return unsupported.
                return .unsupported
            }

            return .witness(candidateLiteral)
        }

        // Non-numeric, non-string: fall back to base type canonical witness
        return .witness(canonicalWitness(for: baseType))
    }

    /// Generate witness for integer ranges using direct integer arithmetic.
    ///
    /// Avoids Double precision loss (correct for values near Int64 boundaries).
    /// Returns the smallest valid integer (deterministic).
    ///
    /// Note: Double can only represent integers exactly up to 2^53.
    /// For values beyond that range, we safely clamp to Int.min/Int.max.
    ///
    /// Reference: XSD 1.1 Part 2, Section 3.4.13 (integer)
    private static func generateIntegerWitness(
        lower: Double, upper: Double,
        lowerInclusive: Bool, upperInclusive: Bool
    ) -> WitnessResult {
        let intMaxAsDouble = Double(Int.max)  // Rounded up in Double representation
        let intMinAsDouble = Double(Int.min)

        // Compute effective lower bound (smallest valid integer)
        let effectiveLower: Int
        if lower == -.infinity || lower < intMinAsDouble {
            effectiveLower = Int.min
        } else if lower > intMaxAsDouble {
            return .unsatisfiable  // No representable integer
        } else if lower >= intMaxAsDouble {
            // lower == Double(Int.max), which is actually Int.max+1 due to rounding.
            // Best approximation: clamp to Int.max (sound for lowerInclusive).
            // For exclusive, this is technically one-off, but unavoidable with Double input.
            effectiveLower = Int.max
        } else {
            // Safe to convert: lower is within (Int.min, ~Int.max)
            let ceiled = lower.rounded(.up)
            let lowerInt = Int(ceiled)
            if lowerInclusive {
                effectiveLower = lowerInt
            } else {
                if lower == ceiled {
                    if lowerInt == Int.max { return .unsatisfiable }
                    effectiveLower = lowerInt + 1
                } else {
                    effectiveLower = lowerInt
                }
            }
        }

        // Compute effective upper bound (largest valid integer)
        let effectiveUpper: Int
        if upper == .infinity || upper > intMaxAsDouble {
            effectiveUpper = Int.max
        } else if upper < intMinAsDouble {
            return .unsatisfiable  // No representable integer
        } else if upper <= intMinAsDouble {
            effectiveUpper = Int.min
        } else {
            let floored = upper.rounded(.down)
            // Guard against overflow: if floored >= intMaxAsDouble, clamp
            let upperInt: Int
            if floored >= intMaxAsDouble {
                upperInt = Int.max
            } else {
                upperInt = Int(floored)
            }
            if upperInclusive {
                effectiveUpper = upperInt
            } else {
                if upper == floored {
                    if upperInt == Int.min { return .unsatisfiable }
                    effectiveUpper = upperInt - 1
                } else {
                    effectiveUpper = upperInt
                }
            }
        }

        if effectiveLower > effectiveUpper {
            return .unsatisfiable
        }

        return .witness(OWLLiteral(lexicalForm: "\(effectiveLower)", datatype: "xsd:integer"))
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
