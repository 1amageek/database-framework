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

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseTypes
import DatabaseKit

// MARK: - Expansion Rule Protocol

/// Result of applying an expansion rule
public enum RuleApplicationResult: Sendable {
    case applied           // Rule was applied successfully
    case notApplicable     // Rule preconditions not met
    case clash(ClashInfo)  // Rule application resulted in a clash
    case failure(OntologyReasoningFailure)
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
    /// Eliminates the ambiguity of `RDFLiteral?` where `nil` conflated
    /// "provably unsatisfiable" with "not yet implemented".
    private enum WitnessResult {
        /// A concrete witness value was generated
        case witness(RDFLiteral)
        /// The data range is provably empty (e.g., contradictory facets)
        case unsatisfiable
        /// Cannot determine satisfiability without a complete witness solver.
        case indeterminate(XSDDiagnostic)
        /// Invalid input, unsupported datatype, or a resource failure.
        case failure(XSDValidationFailure)
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
                for subRole in roleHierarchy.subRolesPrecomputed(of: role) {
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
                    case .indeterminate(let diagnostic):
                        return .failure(.incompleteDatatypeReasoning(diagnostic))
                    case .failure(let failure):
                        return .failure(.datatype(failure))
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

    /// Generate a witness value without converting an indeterminate solver
    /// result into a successful rule application.
    private static func generateWitnessValue(for dataRange: OWLDataRange) -> WitnessResult {
        let validator = OWLDatatypeValidator()
        do {
            _ = try validator.compile(dataRange)
        } catch let failure as XSDValidationFailure {
            return .failure(failure)
        } catch {
            return .indeterminate(XSDDiagnostic(
                code: "unexpectedDatatypeFailure",
                message: String(describing: error)
            ))
        }

        switch dataRange {
        case .datatype(let iri):
            return canonicalWitness(for: iri)

        case .dataOneOf(let values):
            return values.first.map(WitnessResult.witness) ?? .unsatisfiable

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

    private static func generateUnionWitness(ranges: [OWLDataRange]) -> WitnessResult {
        if ranges.isEmpty { return .unsatisfiable }
        var indeterminate: XSDDiagnostic?
        for range in ranges {
            switch generateWitnessValue(for: range) {
            case .witness(let value):
                return .witness(value)
            case .unsatisfiable:
                continue
            case .indeterminate(let diagnostic):
                if indeterminate == nil { indeterminate = diagnostic }
            case .failure(let failure):
                return .failure(failure)
            }
        }
        return indeterminate.map(WitnessResult.indeterminate) ?? .unsatisfiable
    }

    /// Witness for intersection: generate candidates from sub-ranges,
    /// validate each against the full intersection using `OWLDatatypeValidator`.
    private static func generateIntersectionWitness(
        ranges: [OWLDataRange],
        fullRange: OWLDataRange
    ) -> WitnessResult {
        if ranges.isEmpty {
            return canonicalWitness(for: XSDDatatype.string.iri.rawValue)
        }
        let validator = OWLDatatypeValidator()
        let compiled: CompiledOWLDataRange
        do {
            compiled = try validator.compile(fullRange)
        } catch let failure as XSDValidationFailure {
            return .failure(failure)
        } catch {
            return .indeterminate(XSDDiagnostic(
                code: "unexpectedDatatypeFailure",
                message: String(describing: error)
            ))
        }

        for range in ranges {
            switch generateWitnessValue(for: range) {
            case .witness(let candidate):
                do {
                    if try validator.contains(candidate, in: compiled).isMember {
                        return .witness(candidate)
                    }
                } catch let failure as XSDValidationFailure {
                    return .failure(failure)
                } catch {
                    return .indeterminate(XSDDiagnostic(
                        code: "unexpectedDatatypeFailure",
                        message: String(describing: error)
                    ))
                }
            case .unsatisfiable:
                return .unsatisfiable
            case .indeterminate:
                continue
            case .failure(let failure):
                return .failure(failure)
            }
        }
        return .indeterminate(XSDDiagnostic(
            code: "intersectionWitness",
            message: "no generated candidate proves intersection satisfiability"
        ))
    }

    /// Witness for complement: try diverse canonical values,
    /// return the first that does NOT belong to the inner range.
    private static func generateComplementWitness(inner: OWLDataRange) -> WitnessResult {
        let validator = OWLDatatypeValidator()
        let compiled: CompiledOWLDataRange
        do {
            compiled = try validator.compile(inner)
        } catch let failure as XSDValidationFailure {
            return .failure(failure)
        } catch {
            return .indeterminate(XSDDiagnostic(
                code: "unexpectedDatatypeFailure",
                message: String(describing: error)
            ))
        }
        let candidates = canonicalOWL2Witnesses()
        for candidate in candidates {
            do {
                if !(try validator.contains(candidate, in: compiled).isMember) {
                    return .witness(candidate)
                }
            } catch let failure as XSDValidationFailure {
                return .failure(failure)
            } catch {
                return .indeterminate(XSDDiagnostic(
                    code: "unexpectedDatatypeFailure",
                    message: String(describing: error)
                ))
            }
        }
        return .indeterminate(XSDDiagnostic(
            code: "complementWitness",
            message: "finite canonical candidates do not prove complement satisfiability"
        ))
    }

    /// Generate a witness that satisfies facet constraints.
    ///
    private static func generateFacetAwareWitness(
        baseType: String,
        facets: [FacetRestriction]
    ) -> WitnessResult {
        let range = OWLDataRange.datatypeRestriction(
            datatype: baseType,
            facets: facets
        )
        let validator = OWLDatatypeValidator()
        let compiled: CompiledOWLDataRange
        do {
            compiled = try validator.compile(range)
        } catch let failure as XSDValidationFailure {
            return .failure(failure)
        } catch {
            return .indeterminate(XSDDiagnostic(
                code: "unexpectedDatatypeFailure",
                message: String(describing: error)
            ))
        }

        let facetAnalysis: FacetAnalysis
        do {
            facetAnalysis = try analyzeFacets(
                baseType: baseType,
                facets: facets,
                validator: validator
            )
        } catch let failure as XSDValidationFailure {
            return .failure(failure)
        } catch {
            return .indeterminate(XSDDiagnostic(
                code: "unexpectedFacetAnalysisFailure",
                message: String(describing: error)
            ))
        }
        if facetAnalysis.isUnsatisfiable {
            return .unsatisfiable
        }

        var candidates: [RDFLiteral] = []
        if case .witness(let canonical) = canonicalWitness(for: baseType) {
            candidates.append(canonical)
        }
        candidates.append(contentsOf: facetAnalysis.candidates)
        for restriction in facets {
            switch restriction.facet {
            case .minInclusive, .maxInclusive:
                candidates.append(restriction.value)
            default:
                continue
            }
        }

        for candidate in candidates {
            do {
                if try validator.contains(candidate, in: compiled).isMember {
                    return .witness(candidate)
                }
            } catch let failure as XSDValidationFailure {
                return .failure(failure)
            } catch {
                return .indeterminate(XSDDiagnostic(
                    code: "unexpectedDatatypeFailure",
                    message: String(describing: error)
                ))
            }
        }
        return .indeterminate(XSDDiagnostic(
            code: "restrictedWitness",
            message: "compiled range is valid but no exact witness was generated"
        ))
    }

    private struct FacetAnalysis {
        let isUnsatisfiable: Bool
        let candidates: [RDFLiteral]
    }

    private struct OrderedFacetBound {
        let value: RDFLiteral
        let isInclusive: Bool
    }

    private static func analyzeFacets(
        baseType: String,
        facets: [FacetRestriction],
        validator: OWLDatatypeValidator
    ) throws -> FacetAnalysis {
        guard let kind = XSDDatatypeKind(iri: baseType) else {
            return FacetAnalysis(isUnsatisfiable: false, candidates: [])
        }

        let ordered = try analyzeOrderedBounds(
            baseType: baseType,
            facets: facets,
            kind: kind,
            validator: validator
        )
        if ordered.isUnsatisfiable {
            return ordered
        }

        let lengths = try analyzeLengthFacets(
            baseType: baseType,
            facets: facets,
            kind: kind,
            maximumCandidateLength: validator.limits.maxLexicalUTF8Bytes
        )
        return FacetAnalysis(
            isUnsatisfiable: lengths.isUnsatisfiable,
            candidates: ordered.candidates + lengths.candidates
        )
    }

    private static func analyzeOrderedBounds(
        baseType: String,
        facets: [FacetRestriction],
        kind: XSDDatatypeKind,
        validator: OWLDatatypeValidator
    ) throws -> FacetAnalysis {
        var lower: OrderedFacetBound?
        var upper: OrderedFacetBound?

        for restriction in facets {
            switch restriction.facet {
            case .minInclusive:
                lower = try strongerLowerBound(
                    lower,
                    candidate: OrderedFacetBound(
                        value: restriction.value,
                        isInclusive: true
                    ),
                    validator: validator
                )
            case .minExclusive:
                lower = try strongerLowerBound(
                    lower,
                    candidate: OrderedFacetBound(
                        value: restriction.value,
                        isInclusive: false
                    ),
                    validator: validator
                )
            case .maxInclusive:
                upper = try strongerUpperBound(
                    upper,
                    candidate: OrderedFacetBound(
                        value: restriction.value,
                        isInclusive: true
                    ),
                    validator: validator
                )
            case .maxExclusive:
                upper = try strongerUpperBound(
                    upper,
                    candidate: OrderedFacetBound(
                        value: restriction.value,
                        isInclusive: false
                    ),
                    validator: validator
                )
            default:
                continue
            }
        }

        if let lower, let upper {
            switch try validator.compare(lower.value, upper.value) {
            case .greater:
                return FacetAnalysis(isUnsatisfiable: true, candidates: [])
            case .equal where !lower.isInclusive || !upper.isInclusive:
                return FacetAnalysis(isUnsatisfiable: true, candidates: [])
            case .less, .equal, .unordered:
                break
            }
        }

        guard kind.isInteger else {
            return FacetAnalysis(isUnsatisfiable: false, candidates: [])
        }

        var candidates: [RDFLiteral] = []
        let minimum = try integerCandidate(
            from: lower,
            baseType: baseType,
            direction: .minimum
        )
        let maximum = try integerCandidate(
            from: upper,
            baseType: baseType,
            direction: .maximum
        )
        if let minimum {
            candidates.append(minimum)
        }
        if let maximum {
            candidates.append(maximum)
        }
        if let minimum, let maximum {
            switch try validator.compare(minimum, maximum) {
            case .greater:
                return FacetAnalysis(isUnsatisfiable: true, candidates: [])
            case .less, .equal, .unordered:
                break
            }
        }
        return FacetAnalysis(isUnsatisfiable: false, candidates: candidates)
    }

    private static func strongerLowerBound(
        _ current: OrderedFacetBound?,
        candidate: OrderedFacetBound,
        validator: OWLDatatypeValidator
    ) throws -> OrderedFacetBound {
        guard let current else { return candidate }
        switch try validator.compare(candidate.value, current.value) {
        case .greater:
            return candidate
        case .equal:
            return current.isInclusive && !candidate.isInclusive
                ? candidate
                : current
        case .less, .unordered:
            return current
        }
    }

    private static func strongerUpperBound(
        _ current: OrderedFacetBound?,
        candidate: OrderedFacetBound,
        validator: OWLDatatypeValidator
    ) throws -> OrderedFacetBound {
        guard let current else { return candidate }
        switch try validator.compare(candidate.value, current.value) {
        case .less:
            return candidate
        case .equal:
            return current.isInclusive && !candidate.isInclusive
                ? candidate
                : current
        case .greater, .unordered:
            return current
        }
    }

    private enum IntegerCandidateDirection {
        case minimum
        case maximum
    }

    private static func integerCandidate(
        from bound: OrderedFacetBound?,
        baseType: String,
        direction: IntegerCandidateDirection
    ) throws -> RDFLiteral? {
        guard let bound else { return nil }
        if bound.isInclusive {
            return bound.value
        }
        let lexicalForm: String
        switch direction {
        case .minimum:
            lexicalForm = incrementInteger(bound.value.lexicalForm)
        case .maximum:
            lexicalForm = decrementInteger(bound.value.lexicalForm)
        }
        return try RDFLiteral(
            lexicalForm: lexicalForm,
            datatype: baseType
        )
    }

    private static func incrementInteger(_ lexicalForm: String) -> String {
        let normalized = normalizedIntegerParts(lexicalForm)
        if normalized.isNegative {
            if normalized.magnitude == "1" {
                return "0"
            }
            return "-" + decrementMagnitude(normalized.magnitude)
        }
        return incrementMagnitude(normalized.magnitude)
    }

    private static func decrementInteger(_ lexicalForm: String) -> String {
        let normalized = normalizedIntegerParts(lexicalForm)
        if normalized.isNegative {
            return "-" + incrementMagnitude(normalized.magnitude)
        }
        if normalized.magnitude == "0" {
            return "-1"
        }
        return decrementMagnitude(normalized.magnitude)
    }

    private static func normalizedIntegerParts(
        _ lexicalForm: String
    ) -> (isNegative: Bool, magnitude: String) {
        var bytes = Array(lexicalForm.utf8)
        let isNegative = bytes.first == 45
        if bytes.first == 45 || bytes.first == 43 {
            bytes.removeFirst()
        }
        while bytes.count > 1, bytes.first == 48 {
            bytes.removeFirst()
        }
        return (
            isNegative && bytes.contains(where: { $0 != 48 }),
            String(decoding: bytes, as: UTF8.self)
        )
    }

    private static func incrementMagnitude(_ magnitude: String) -> String {
        var bytes = Array(magnitude.utf8)
        var index = bytes.count
        while index > 0 {
            index -= 1
            if bytes[index] < 57 {
                bytes[index] += 1
                return String(decoding: bytes, as: UTF8.self)
            }
            bytes[index] = 48
        }
        bytes.insert(49, at: 0)
        return String(decoding: bytes, as: UTF8.self)
    }

    private static func decrementMagnitude(_ magnitude: String) -> String {
        var bytes = Array(magnitude.utf8)
        var index = bytes.count
        while index > 0 {
            index -= 1
            if bytes[index] > 48 {
                bytes[index] -= 1
                break
            }
            bytes[index] = 57
        }
        while bytes.count > 1, bytes.first == 48 {
            bytes.removeFirst()
        }
        return String(decoding: bytes, as: UTF8.self)
    }

    private static func analyzeLengthFacets(
        baseType: String,
        facets: [FacetRestriction],
        kind: XSDDatatypeKind,
        maximumCandidateLength: Int
    ) throws -> FacetAnalysis {
        guard kind.isStringFamily else {
            return FacetAnalysis(isUnsatisfiable: false, candidates: [])
        }

        var exact: XSDDecimalValue?
        var minimum: XSDDecimalValue?
        var maximum: XSDDecimalValue?
        for restriction in facets {
            guard restriction.facet == .length
                    || restriction.facet == .minLength
                    || restriction.facet == .maxLength,
                  let value = XSDDecimalValue(
                    integer: restriction.value.lexicalForm
                  ) else {
                continue
            }
            switch restriction.facet {
            case .length:
                if let exact, exact.compare(to: value) != 0 {
                    return FacetAnalysis(isUnsatisfiable: true, candidates: [])
                }
                exact = value
            case .minLength:
                if let currentMinimum = minimum {
                    if currentMinimum.compare(to: value) < 0 {
                        minimum = value
                    }
                } else {
                    minimum = value
                }
            case .maxLength:
                if let currentMaximum = maximum {
                    if currentMaximum.compare(to: value) > 0 {
                        maximum = value
                    }
                } else {
                    maximum = value
                }
            default:
                break
            }
        }

        if let minimum, let maximum, minimum.compare(to: maximum) > 0 {
            return FacetAnalysis(isUnsatisfiable: true, candidates: [])
        }
        if let exact {
            if let minimum, exact.compare(to: minimum) < 0 {
                return FacetAnalysis(isUnsatisfiable: true, candidates: [])
            }
            if let maximum, exact.compare(to: maximum) > 0 {
                return FacetAnalysis(isUnsatisfiable: true, candidates: [])
            }
        }

        guard let required = exact ?? minimum else {
            return FacetAnalysis(isUnsatisfiable: false, candidates: [])
        }
        guard let length = Int(required.source),
              length <= maximumCandidateLength else {
            return FacetAnalysis(isUnsatisfiable: false, candidates: [])
        }
        let witness = try RDFLiteral(
            lexicalForm: String(repeating: "a", count: length),
            datatype: baseType
        )
        return FacetAnalysis(isUnsatisfiable: false, candidates: [witness])
    }

    private static func canonicalWitness(for iri: String) -> WitnessResult {
        guard let kind = XSDDatatypeKind(iri: iri),
              XSDDatatypeProfile.owl2.supports(kind) else {
            return .failure(.unsupportedDatatype(iri))
        }
        let lexicalForm: String
        let language: String?
        let datatype: String
        switch kind {
        case .owlReal:
            return .witness(RDFLiteral(
                lexicalForm: "0",
                datatype: XSDDatatype.decimal.typedLiteralDatatype
            ))
        case .owlRational:
            lexicalForm = "0/1"; language = nil; datatype = iri
        case .rdfsLiteral:
            return .witness(RDFLiteral.string("witness"))
        case .string, .normalizedString:
            lexicalForm = ""; language = nil; datatype = iri
        case .token, .nmtoken, .name, .ncname:
            lexicalForm = "a"; language = nil; datatype = iri
        case .language:
            lexicalForm = "en"; language = nil; datatype = iri
        case .boolean:
            lexicalForm = "false"; language = nil; datatype = iri
        case .decimal, .integer, .nonPositiveInteger, .nonNegativeInteger,
             .long, .int, .short, .byte, .unsignedLong, .unsignedInt,
             .unsignedShort, .unsignedByte:
            lexicalForm = "0"; language = nil; datatype = iri
        case .negativeInteger:
            lexicalForm = "-1"; language = nil; datatype = iri
        case .positiveInteger:
            lexicalForm = "1"; language = nil; datatype = iri
        case .float, .double:
            lexicalForm = "0"; language = nil; datatype = iri
        case .dateTime:
            lexicalForm = "2000-01-01T00:00:00"; language = nil; datatype = iri
        case .dateTimeStamp:
            lexicalForm = "2000-01-01T00:00:00Z"; language = nil; datatype = iri
        case .anyURI, .base64Binary, .hexBinary, .rdfXMLLiteral:
            lexicalForm = ""; language = nil; datatype = iri
        case .rdfPlainLiteral:
            lexicalForm = "@"; language = nil; datatype = iri
        case .rdfLangString:
            return .witness(RDFLiteral(
                lexicalForm: "witness",
                language: .english
            ))
        case .duration, .time, .date:
            return .failure(.unsupportedDatatype(iri))
        }
        if let language {
            do {
                return .witness(RDFLiteral(
                    lexicalForm: lexicalForm,
                    language: try RDFLanguageTag(language)
                ))
            } catch {
                return .failure(.unsupportedDatatype(datatype))
            }
        }
        do {
            return .witness(try RDFLiteral(
                lexicalForm: lexicalForm,
                datatype: datatype
            ))
        } catch {
            return .failure(.unsupportedDatatype(datatype))
        }
    }

    private static func canonicalOWL2Witnesses() -> [RDFLiteral] {
        let iris = [
            XSDDatatype.string.iri.rawValue,
            XSDDatatype.boolean.iri.rawValue,
            XSDDatatype.integer.iri.rawValue,
            XSDDatatype.decimal.iri.rawValue,
            XSDDatatype.float.iri.rawValue,
            XSDDatatype.double.iri.rawValue,
            XSDDatatype.dateTime.iri.rawValue,
            XSDDatatype.dateTimeStamp.iri.rawValue,
            XSDDatatype.anyURI.iri.rawValue,
            XSDDatatype.hexBinary.iri.rawValue,
            XSDDatatype.base64Binary.iri.rawValue,
            XSDDatatypeKind.rdfNamespace + "langString",
        ]
        var values: [RDFLiteral] = []
        values.reserveCapacity(iris.count)
        for iri in iris {
            if case .witness(let value) = canonicalWitness(for: iri) {
                values.append(value)
            }
        }
        return values
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
