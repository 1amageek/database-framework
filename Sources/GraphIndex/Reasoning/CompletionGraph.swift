// CompletionGraph.swift
// GraphIndex - Completion graph for Tableaux algorithm
//
// Implements a proper completion graph with:
// - Trail-based state management for efficient backtracking
// - Proper nominal handling
// - Dynamic blocking support
//
// Reference:
// - Horrocks, I., & Sattler, U. (2007). "A Tableaux Decision Procedure for SHOIQ"
// - Motik, B., Shearer, R., & Horrocks, I. (2009). "Hypertableau Reasoning for Description Logics"

import Foundation
import Graph

// MARK: - Node Identifier

/// Unique identifier for nodes in the completion graph
public struct NodeID: Hashable, Sendable, CustomStringConvertible {
    private let value: Int
    private let isNominal: Bool
    private let nominalIRI: String?

    /// Create a generated node ID
    static func generated(_ id: Int) -> NodeID {
        NodeID(value: id, isNominal: false, nominalIRI: nil)
    }

    /// Create a nominal node ID (for named individuals)
    static func nominal(_ iri: String) -> NodeID {
        NodeID(value: iri.hashValue, isNominal: true, nominalIRI: iri)
    }

    public var description: String {
        if let iri = nominalIRI {
            return "nominal(\(iri))"
        }
        return "n\(value)"
    }

    public var isNominalNode: Bool { isNominal }
    public var iri: String? { nominalIRI }
}

// MARK: - Edge

/// Represents an edge in the completion graph
public struct Edge: Hashable, Sendable {
    public let from: NodeID
    public let role: String
    public let to: NodeID

    public init(from: NodeID, role: String, to: NodeID) {
        self.from = from
        self.role = role
        self.to = to
    }
}

// MARK: - Trail Action

/// Saved processed flags for undo
struct ProcessedFlags: Sendable {
    let intersections: Set<OWLClassExpression>
    let unions: Set<OWLClassExpression>
    let existentials: Set<OWLClassExpression>
    let universals: Set<OWLClassExpression>
}

/// Actions that can be undone during backtracking
enum TrailAction: Sendable {
    case addedConcept(node: NodeID, concept: OWLClassExpression)
    case addedEdge(edge: Edge)
    case createdNode(id: NodeID)
    case mergedNodes(survivor: NodeID, merged: NodeID, mergedConcepts: Set<OWLClassExpression>, mergedEdges: Set<Edge>, survivorFlags: ProcessedFlags)
    case blocked(node: NodeID)
    case unblocked(node: NodeID)
    case addedDataValue(node: NodeID, property: String, value: OWLLiteral)
    case choicePoint(id: Int)
    case addedProcessedFlag(node: NodeID, flagType: ProcessedFlagType, concept: OWLClassExpression)
}

/// Types of processed flags tracked in the trail
enum ProcessedFlagType: Sendable {
    case intersection
    case union
    case existential
    case universal
}

// MARK: - Choice Point

/// Represents a choice point for non-deterministic rules
struct TableauxChoicePoint: Sendable {
    let id: Int
    let trailPosition: Int
    let nodeID: NodeID
    let expression: OWLClassExpression
    let alternatives: [OWLClassExpression]
    let currentChoice: Int
}

// MARK: - Completion Graph Node

/// A node in the completion graph
final class CompletionNode: @unchecked Sendable {
    let id: NodeID
    var concepts: Set<OWLClassExpression> = []
    var outgoingEdges: [String: Set<NodeID>] = [:]  // role -> targets
    var incomingEdges: [String: Set<NodeID>] = [:]  // role -> sources
    var dataValues: [String: Set<OWLLiteral>] = [:]
    var parent: NodeID?
    var depth: Int = 0

    // Blocking state
    var isBlocked: Bool = false
    var blockedBy: NodeID?

    // Processed flags to avoid redundant rule applications
    var processedIntersections: Set<OWLClassExpression> = []
    var processedUnions: Set<OWLClassExpression> = []
    var processedExistentials: Set<OWLClassExpression> = []
    var processedUniversals: Set<OWLClassExpression> = []

    init(id: NodeID) {
        self.id = id
    }

    /// Deep copy for state snapshot
    func copy() -> CompletionNode {
        let node = CompletionNode(id: id)
        node.concepts = concepts
        node.outgoingEdges = outgoingEdges
        node.incomingEdges = incomingEdges
        node.dataValues = dataValues
        node.parent = parent
        node.depth = depth
        node.isBlocked = isBlocked
        node.blockedBy = blockedBy
        node.processedIntersections = processedIntersections
        node.processedUnions = processedUnions
        node.processedExistentials = processedExistentials
        node.processedUniversals = processedUniversals
        return node
    }
}

// MARK: - Completion Graph

/// The completion graph (forest) for Tableaux reasoning
///
/// Implements trail-based backtracking for efficient state management.
/// Reference: Horrocks & Sattler (2007), Section 4
public final class CompletionGraph: @unchecked Sendable {

    // MARK: - Properties

    /// All nodes indexed by ID
    private(set) var nodes: [NodeID: CompletionNode] = [:]

    /// Nominal nodes (cannot be blocked)
    private(set) var nominals: Set<NodeID> = []

    /// All edges in the graph
    private(set) var edges: Set<Edge> = []

    /// Trail for backtracking
    private var trail: [TrailAction] = []

    /// Choice points stack
    private var choicePoints: [TableauxChoicePoint] = []

    /// Node ID counter
    private var nodeCounter: Int = 0

    /// Choice point ID counter
    private var choicePointCounter: Int = 0

    /// Role hierarchy for property reasoning
    private let roleHierarchy: RoleHierarchy

    /// Class hierarchy for disjoint checking
    private let classHierarchy: ClassHierarchy

    // MARK: - Initialization

    public init(roleHierarchy: RoleHierarchy, classHierarchy: ClassHierarchy) {
        self.roleHierarchy = roleHierarchy
        self.classHierarchy = classHierarchy
    }

    // MARK: - Node Operations

    /// Create a new generated node
    func createNode(parent: NodeID? = nil) -> NodeID {
        nodeCounter += 1
        let id = NodeID.generated(nodeCounter)
        let node = CompletionNode(id: id)
        node.parent = parent

        if let parentID = parent, let parentNode = nodes[parentID] {
            node.depth = parentNode.depth + 1
        }

        nodes[id] = node
        trail.append(.createdNode(id: id))
        return id
    }

    /// Create or get a nominal node
    func getOrCreateNominal(_ iri: String) -> NodeID {
        let id = NodeID.nominal(iri)
        if nodes[id] == nil {
            let node = CompletionNode(id: id)
            nodes[id] = node
            nominals.insert(id)
            trail.append(.createdNode(id: id))
        }
        return id
    }

    /// Get node by ID
    func node(_ id: NodeID) -> CompletionNode? {
        nodes[id]
    }

    // MARK: - Concept Operations

    /// Add a concept to a node
    /// Returns true if the concept was newly added
    @discardableResult
    func addConcept(_ concept: OWLClassExpression, to nodeID: NodeID) -> Bool {
        guard let node = nodes[nodeID] else { return false }
        guard !node.concepts.contains(concept) else { return false }

        node.concepts.insert(concept)
        trail.append(.addedConcept(node: nodeID, concept: concept))
        return true
    }

    /// Check if a node has a concept
    func hasConcept(_ concept: OWLClassExpression, at nodeID: NodeID) -> Bool {
        nodes[nodeID]?.concepts.contains(concept) ?? false
    }

    /// Get all concepts at a node
    func concepts(at nodeID: NodeID) -> Set<OWLClassExpression> {
        nodes[nodeID]?.concepts ?? []
    }

    // MARK: - Edge Operations

    /// Add an edge between nodes
    /// Returns true if the edge was newly added
    @discardableResult
    func addEdge(from: NodeID, role: String, to: NodeID) -> Bool {
        let edge = Edge(from: from, role: role, to: to)
        guard !edges.contains(edge) else { return false }

        edges.insert(edge)
        nodes[from]?.outgoingEdges[role, default: []].insert(to)
        nodes[to]?.incomingEdges[role, default: []].insert(from)
        trail.append(.addedEdge(edge: edge))

        // Handle role characteristics
        applyRoleCharacteristics(edge: edge)

        return true
    }

    /// Apply role characteristics when adding an edge
    private func applyRoleCharacteristics(edge: Edge) {
        let role = edge.role

        // Symmetric: R(x,y) → R(y,x)
        if roleHierarchy.isSymmetric(role) {
            let inverseEdge = Edge(from: edge.to, role: role, to: edge.from)
            if !edges.contains(inverseEdge) {
                edges.insert(inverseEdge)
                nodes[edge.to]?.outgoingEdges[role, default: []].insert(edge.from)
                nodes[edge.from]?.incomingEdges[role, default: []].insert(edge.to)
                trail.append(.addedEdge(edge: inverseEdge))
            }
        }

        // Inverse: R(x,y) → R⁻(y,x)
        if let inverseRole = roleHierarchy.inverse(of: role) {
            let inverseEdge = Edge(from: edge.to, role: inverseRole, to: edge.from)
            if !edges.contains(inverseEdge) {
                edges.insert(inverseEdge)
                nodes[edge.to]?.outgoingEdges[inverseRole, default: []].insert(edge.from)
                nodes[edge.from]?.incomingEdges[inverseRole, default: []].insert(edge.to)
                trail.append(.addedEdge(edge: inverseEdge))
            }
        }

        // Reflexive: For all nodes with R-edge, add R(x,x)
        if roleHierarchy.isReflexive(role) {
            let selfEdge = Edge(from: edge.to, role: role, to: edge.to)
            if !edges.contains(selfEdge) {
                edges.insert(selfEdge)
                nodes[edge.to]?.outgoingEdges[role, default: []].insert(edge.to)
                nodes[edge.to]?.incomingEdges[role, default: []].insert(edge.to)
                trail.append(.addedEdge(edge: selfEdge))
            }
        }
    }

    /// Get successors of a node via a role
    func successors(of nodeID: NodeID, via role: String) -> Set<NodeID> {
        nodes[nodeID]?.outgoingEdges[role] ?? []
    }

    /// Get predecessors of a node via a role
    func predecessors(of nodeID: NodeID, via role: String) -> Set<NodeID> {
        nodes[nodeID]?.incomingEdges[role] ?? []
    }

    /// Get all successors via any sub-role
    func successorsViaSubRoles(of nodeID: NodeID, via role: String) -> Set<NodeID> {
        var result = successors(of: nodeID, via: role)
        for subRole in roleHierarchy.directSubRoles(of: role) {
            result.formUnion(successors(of: nodeID, via: subRole))
        }
        return result
    }

    // MARK: - Processed Flag Operations

    /// Record a processed flag addition in the trail for backtracking support
    func recordProcessedFlag(_ flagType: ProcessedFlagType, concept: OWLClassExpression, at nodeID: NodeID) {
        trail.append(.addedProcessedFlag(node: nodeID, flagType: flagType, concept: concept))
    }

    // MARK: - Data Values

    /// Add a data value to a node
    @discardableResult
    func addDataValue(_ value: OWLLiteral, property: String, to nodeID: NodeID) -> Bool {
        guard let node = nodes[nodeID] else { return false }
        guard !(node.dataValues[property]?.contains(value) ?? false) else { return false }

        node.dataValues[property, default: []].insert(value)
        trail.append(.addedDataValue(node: nodeID, property: property, value: value))
        return true
    }

    // MARK: - Node Merging (for ≤ rule and nominals)

    /// Merge two nodes (for max cardinality and nominal equality)
    /// The survivor keeps both nodes' content
    func mergeNodes(survivor: NodeID, merged: NodeID) {
        guard let survivorNode = nodes[survivor],
              let mergedNode = nodes[merged] else { return }

        // Save merged node state for undo
        let mergedConcepts = mergedNode.concepts
        var mergedEdges = Set<Edge>()

        // Move concepts
        for concept in mergedNode.concepts {
            if !survivorNode.concepts.contains(concept) {
                survivorNode.concepts.insert(concept)
            }
        }

        // Move outgoing edges
        for (role, targets) in mergedNode.outgoingEdges {
            for target in targets {
                let oldEdge = Edge(from: merged, role: role, to: target)
                let newEdge = Edge(from: survivor, role: role, to: target)
                mergedEdges.insert(oldEdge)
                edges.remove(oldEdge)
                if !edges.contains(newEdge) {
                    edges.insert(newEdge)
                    survivorNode.outgoingEdges[role, default: []].insert(target)
                }
                // Update target's incoming edges
                nodes[target]?.incomingEdges[role]?.remove(merged)
                nodes[target]?.incomingEdges[role]?.insert(survivor)
            }
        }

        // Move incoming edges
        for (role, sources) in mergedNode.incomingEdges {
            for source in sources {
                let oldEdge = Edge(from: source, role: role, to: merged)
                let newEdge = Edge(from: source, role: role, to: survivor)
                mergedEdges.insert(oldEdge)
                edges.remove(oldEdge)
                if !edges.contains(newEdge) {
                    edges.insert(newEdge)
                    survivorNode.incomingEdges[role, default: []].insert(source)
                }
                // Update source's outgoing edges
                nodes[source]?.outgoingEdges[role]?.remove(merged)
                nodes[source]?.outgoingEdges[role]?.insert(survivor)
            }
        }

        // Move data values
        for (prop, values) in mergedNode.dataValues {
            survivorNode.dataValues[prop, default: []].formUnion(values)
        }

        // Save survivor's processed flags for undo
        let survivorFlags = ProcessedFlags(
            intersections: survivorNode.processedIntersections,
            unions: survivorNode.processedUnions,
            existentials: survivorNode.processedExistentials,
            universals: survivorNode.processedUniversals
        )

        // CRITICAL: Clear processed flags on survivor after merge
        // The merged concepts may require re-evaluation of rules that were
        // already applied. For example:
        // - New intersections from merged node need to be expanded
        // - New universal constraints may apply to existing successors
        // - The combination of concepts may enable new inferences
        // Reference: Horrocks & Sattler (2007), Section 4.2 - Merging in ≤-rule
        survivorNode.processedIntersections.removeAll()
        survivorNode.processedUnions.removeAll()
        survivorNode.processedExistentials.removeAll()
        survivorNode.processedUniversals.removeAll()

        // Record action for undo (includes saved flags for restoration)
        trail.append(.mergedNodes(survivor: survivor, merged: merged,
                                  mergedConcepts: mergedConcepts, mergedEdges: mergedEdges,
                                  survivorFlags: survivorFlags))

        // Remove merged node
        nodes.removeValue(forKey: merged)
        nominals.remove(merged)
    }

    // MARK: - Blocking

    /// Check and apply blocking for all nodes
    func updateBlocking() {
        // First, unblock all nodes (blocking is recomputed each time)
        for (id, node) in nodes {
            if node.isBlocked {
                node.isBlocked = false
                node.blockedBy = nil
                trail.append(.unblocked(node: id))
            }
        }

        // Sort nodes by depth (deeper nodes first for checking)
        let sortedNodes = nodes.values.sorted { $0.depth > $1.depth }

        for node in sortedNodes {
            // Nominals cannot be blocked
            if nominals.contains(node.id) { continue }

            // Find a blocking ancestor
            if let blocker = findBlocker(for: node) {
                node.isBlocked = true
                node.blockedBy = blocker
                trail.append(.blocked(node: node.id))
            }
        }
    }

    /// Find a node that blocks the given node
    /// Uses pairwise blocking for SHOIN(D)
    private func findBlocker(for node: CompletionNode) -> NodeID? {
        var current = node.parent
        while let ancestorID = current, let ancestor = nodes[ancestorID] {
            // Pairwise blocking: x is blocked by y if L(x) ⊆ L(y)
            if node.concepts.isSubset(of: ancestor.concepts) {
                // Also check edge labels match for SHOIN(D)
                if edgeLabelsMatch(node: node, ancestor: ancestor) {
                    return ancestorID
                }
            }
            current = ancestor.parent
        }
        return nil
    }

    /// Check if edge labels match for blocking (SHOIN(D) requirement)
    private func edgeLabelsMatch(node: CompletionNode, ancestor: CompletionNode) -> Bool {
        // For each role R where node has an R-neighbor:
        // ancestor must also have an R-neighbor
        for (role, _) in node.outgoingEdges {
            if ancestor.outgoingEdges[role] == nil || ancestor.outgoingEdges[role]!.isEmpty {
                return false
            }
        }
        return true
    }

    /// Check if a node is blocked
    func isBlocked(_ nodeID: NodeID) -> Bool {
        nodes[nodeID]?.isBlocked ?? false
    }

    // MARK: - Choice Points and Backtracking

    /// Create a choice point for non-deterministic rule
    func createChoicePoint(
        nodeID: NodeID,
        expression: OWLClassExpression,
        alternatives: [OWLClassExpression]
    ) -> Int {
        choicePointCounter += 1
        let cp = TableauxChoicePoint(
            id: choicePointCounter,
            trailPosition: trail.count,
            nodeID: nodeID,
            expression: expression,
            alternatives: alternatives,
            currentChoice: 0
        )
        choicePoints.append(cp)
        trail.append(.choicePoint(id: choicePointCounter))
        return choicePointCounter
    }

    /// Backtrack to the last choice point with remaining alternatives
    /// Returns the next alternative, or nil if no more choices
    func backtrack() -> (nodeID: NodeID, choice: OWLClassExpression)? {
        while let cp = choicePoints.popLast() {
            let nextChoice = cp.currentChoice + 1

            if nextChoice < cp.alternatives.count {
                // Undo actions back to choice point
                undoToTrailPosition(cp.trailPosition)

                // Create new choice point with next alternative
                let newCP = TableauxChoicePoint(
                    id: cp.id,
                    trailPosition: cp.trailPosition,
                    nodeID: cp.nodeID,
                    expression: cp.expression,
                    alternatives: cp.alternatives,
                    currentChoice: nextChoice
                )
                choicePoints.append(newCP)

                return (cp.nodeID, cp.alternatives[nextChoice])
            }

            // No more alternatives at this choice point, undo and continue
            undoToTrailPosition(cp.trailPosition)
        }

        return nil
    }

    /// Undo trail actions back to a position
    private func undoToTrailPosition(_ position: Int) {
        while trail.count > position {
            let action = trail.removeLast()
            undoAction(action)
        }
    }

    /// Undo a single trail action
    private func undoAction(_ action: TrailAction) {
        switch action {
        case .addedConcept(let nodeID, let concept):
            nodes[nodeID]?.concepts.remove(concept)

        case .addedEdge(let edge):
            edges.remove(edge)
            nodes[edge.from]?.outgoingEdges[edge.role]?.remove(edge.to)
            nodes[edge.to]?.incomingEdges[edge.role]?.remove(edge.from)

        case .createdNode(let id):
            nodes.removeValue(forKey: id)
            nominals.remove(id)

        case .mergedNodes(let survivor, let merged, let mergedConcepts, let mergedEdges, let survivorFlags):
            // Recreate merged node
            let mergedNode = CompletionNode(id: merged)
            mergedNode.concepts = mergedConcepts
            nodes[merged] = mergedNode

            // Restore edges
            for edge in mergedEdges {
                edges.insert(edge)
                if edge.from == merged {
                    mergedNode.outgoingEdges[edge.role, default: []].insert(edge.to)
                    nodes[edge.to]?.incomingEdges[edge.role]?.insert(merged)
                    nodes[edge.to]?.incomingEdges[edge.role]?.remove(survivor)
                    nodes[survivor]?.outgoingEdges[edge.role]?.remove(edge.to)
                }
                if edge.to == merged {
                    mergedNode.incomingEdges[edge.role, default: []].insert(edge.from)
                    nodes[edge.from]?.outgoingEdges[edge.role]?.insert(merged)
                    nodes[edge.from]?.outgoingEdges[edge.role]?.remove(survivor)
                    nodes[survivor]?.incomingEdges[edge.role]?.remove(edge.from)
                }
            }

            // Remove concepts from survivor that came from merged
            for concept in mergedConcepts {
                nodes[survivor]?.concepts.remove(concept)
            }

            // Restore survivor's processed flags
            if let survivorNode = nodes[survivor] {
                survivorNode.processedIntersections = survivorFlags.intersections
                survivorNode.processedUnions = survivorFlags.unions
                survivorNode.processedExistentials = survivorFlags.existentials
                survivorNode.processedUniversals = survivorFlags.universals
            }

            if merged.isNominalNode {
                nominals.insert(merged)
            }

        case .blocked(let nodeID):
            nodes[nodeID]?.isBlocked = false
            nodes[nodeID]?.blockedBy = nil

        case .unblocked(let nodeID):
            // This was from updateBlocking - no explicit action needed
            // The blocking will be recomputed
            _ = nodeID

        case .addedDataValue(let nodeID, let property, let value):
            nodes[nodeID]?.dataValues[property]?.remove(value)

        case .choicePoint:
            // Choice point marker - no undo action
            break

        case .addedProcessedFlag(let nodeID, let flagType, let concept):
            guard let node = nodes[nodeID] else { break }
            switch flagType {
            case .intersection:
                node.processedIntersections.remove(concept)
            case .union:
                node.processedUnions.remove(concept)
            case .existential:
                node.processedExistentials.remove(concept)
            case .universal:
                node.processedUniversals.remove(concept)
            }
        }
    }

    /// Check if there are active choice points
    var hasChoicePoints: Bool {
        !choicePoints.isEmpty
    }

    // MARK: - Transitive Closure

    /// Compute transitive closure for a role
    func expandTransitiveRole(_ role: String) -> Bool {
        guard roleHierarchy.isTransitive(role) else { return false }

        var changed = false
        var newEdges: [Edge] = []

        // Find all edges to add: R(x,y) ∧ R(y,z) → R(x,z)
        for edge1 in edges where edge1.role == role {
            for edge2 in edges where edge2.role == role && edge2.from == edge1.to {
                let newEdge = Edge(from: edge1.from, role: role, to: edge2.to)
                if !edges.contains(newEdge) && edge1.from != edge2.to {
                    newEdges.append(newEdge)
                }
            }
        }

        // Add new edges
        for edge in newEdges {
            if addEdge(from: edge.from, role: edge.role, to: edge.to) {
                changed = true
            }
        }

        return changed
    }

    // MARK: - Property Chain

    /// Apply property chain: R₁ ∘ R₂ ∘ ... ∘ Rₙ ⊑ S
    func applyPropertyChain(_ chain: [String], implies: String) -> Bool {
        guard chain.count >= 2 else { return false }

        var changed = false

        // Find all paths matching the chain
        let paths = findChainPaths(chain)

        for path in paths {
            let start = path.first!
            let end = path.last!

            if addEdge(from: start, role: implies, to: end) {
                changed = true
            }
        }

        return changed
    }

    /// Find all paths matching a property chain
    private func findChainPaths(_ chain: [String]) -> [[NodeID]] {
        guard let firstRole = chain.first else { return [] }

        // Start with all edges of the first role
        var paths: [[NodeID]] = []
        for edge in edges where edge.role == firstRole {
            paths.append([edge.from, edge.to])
        }

        // Extend paths for remaining roles
        for i in 1..<chain.count {
            let role = chain[i]
            var extendedPaths: [[NodeID]] = []

            for path in paths {
                guard let lastNode = path.last else { continue }
                for successor in successors(of: lastNode, via: role) {
                    var newPath = path
                    newPath.append(successor)
                    extendedPaths.append(newPath)
                }
            }

            paths = extendedPaths
        }

        return paths
    }

    // MARK: - Statistics

    /// Get graph statistics
    var statistics: (nodes: Int, edges: Int, nominals: Int, blocked: Int) {
        let blockedCount = nodes.values.filter { $0.isBlocked }.count
        return (nodes.count, edges.count, nominals.count, blockedCount)
    }

    // MARK: - Debug

    /// Get a description of the graph state
    var debugDescription: String {
        var lines: [String] = ["CompletionGraph:"]
        lines.append("  Nodes: \(nodes.count), Edges: \(edges.count), Nominals: \(nominals.count)")

        for (id, node) in nodes.sorted(by: { $0.key.description < $1.key.description }) {
            var line = "  \(id)"
            if node.isBlocked {
                line += " [BLOCKED by \(node.blockedBy?.description ?? "?")]"
            }
            if nominals.contains(id) {
                line += " [NOMINAL]"
            }
            lines.append(line)

            if !node.concepts.isEmpty {
                let conceptsStr = node.concepts.map { $0.description }.sorted().joined(separator: ", ")
                lines.append("    concepts: \(conceptsStr)")
            }

            for (role, targets) in node.outgoingEdges.sorted(by: { $0.key < $1.key }) {
                let targetsStr = targets.map { $0.description }.sorted().joined(separator: ", ")
                lines.append("    -[\(role)]-> \(targetsStr)")
            }
        }

        return lines.joined(separator: "\n")
    }
}
