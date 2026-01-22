// PersistentUnionFind.swift
// GraphIndex - Persistent Union-Find for owl:sameAs
//
// Implements Union-Find (Disjoint Set Union) data structure persisted in FoundationDB
// for efficient owl:sameAs handling without exponential materialization.
//
// Reference: Tarjan, R. E. (1975). "Efficiency of a Good But Not Linear Set Union Algorithm"
// Reference: W3C OWL 2 https://www.w3.org/TR/owl2-syntax/#Individual_Equality

import Foundation
import FoundationDB

/// Persistent Union-Find for owl:sameAs equivalence classes
///
/// **Problem**: Full materialization of owl:sameAs can cause exponential explosion.
/// For N individuals in an equivalence class, full materialization generates O(N²) triples.
/// Real-world datasets: 500M triples → 35B inferred triples (70x explosion).
///
/// **Solution**: Use Union-Find with path compression and union by rank.
/// - Store only O(N) parent pointers instead of O(N²) triples
/// - Query-time expansion to find all equivalent individuals
/// - Canonical representative for each equivalence class
///
/// **Reference**: RDFox, Stardog, and other production triple stores use similar approaches.
///
/// **Storage Layout**:
/// ```
/// [sameAs]/0/[individual] → parent IRI
/// [sameAs]/1/[individual] → rank (Int)
/// [sameAs]/2/[representative]/[member] → empty (index for members lookup)
/// ```
///
/// **Example**:
/// ```swift
/// let uf = PersistentUnionFind(subspace: ontologySubspace)
///
/// try await context.withTransaction { tx in
///     // Union two individuals
///     try await uf.union("ex:Alice", "ex:AliceJones", ontologyIRI: iri, transaction: tx)
///
///     // Find canonical representative
///     let canonical = try await uf.find("ex:AliceJones", ontologyIRI: iri, transaction: tx)
///     // canonical == "ex:Alice" (or could be "ex:AliceJones")
///
///     // Get all members of equivalence class
///     let members = try await uf.members(of: "ex:Alice", ontologyIRI: iri, transaction: tx)
///     // members == ["ex:Alice", "ex:AliceJones"]
/// }
/// ```
public struct PersistentUnionFind: Sendable {

    // MARK: - Properties

    /// Subspace for Union-Find storage
    public let subspace: OntologySubspace

    // MARK: - Initialization

    public init(subspace: OntologySubspace) {
        self.subspace = subspace
    }

    // MARK: - Core Operations

    /// Find the canonical representative for an individual
    ///
    /// Implements path compression: all nodes along the path point directly to root.
    ///
    /// - Parameters:
    ///   - individual: The individual IRI
    ///   - ontologyIRI: The ontology IRI
    ///   - transaction: The transaction to use
    /// - Returns: The canonical representative (root of equivalence class)
    public func find(
        _ individual: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> String {
        var current = individual
        var path: [String] = []

        // Follow parent pointers to root
        while true {
            let parentKey = subspace.sameAsParentKey(ontologyIRI, individual: current)
            guard let parentData = try await transaction.getValue(for: parentKey, snapshot: true) else {
                // No parent = this is a root (or not in Union-Find)
                break
            }
            let parent = String(decoding: parentData, as: UTF8.self)
            if parent == current {
                // Self-loop = root
                break
            }
            path.append(current)
            current = parent
        }

        // Path compression: point all nodes directly to root
        let root = current
        for node in path {
            let parentKey = subspace.sameAsParentKey(ontologyIRI, individual: node)
            transaction.setValue(Array(root.utf8), for: parentKey)
        }

        return root
    }

    /// Union two individuals into the same equivalence class
    ///
    /// Implements union by rank: attach smaller tree under larger tree.
    ///
    /// **Note**: This method does NOT require prior `makeSet()` calls.
    /// Individuals are implicitly initialized when first encountered.
    /// However, for explicit initialization with proper rank setup,
    /// use `makeSet()` before `union()`.
    ///
    /// - Parameters:
    ///   - individual1: First individual IRI
    ///   - individual2: Second individual IRI
    ///   - ontologyIRI: The ontology IRI
    ///   - transaction: The transaction to use
    /// - Returns: The canonical representative after union
    @discardableResult
    public func union(
        _ individual1: String,
        _ individual2: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> String {
        // Find roots
        let root1 = try await find(individual1, ontologyIRI: ontologyIRI, transaction: transaction)
        let root2 = try await find(individual2, ontologyIRI: ontologyIRI, transaction: transaction)

        // Already in same equivalence class
        if root1 == root2 {
            return root1
        }

        // Get ranks
        let rank1 = try await getRank(root1, ontologyIRI: ontologyIRI, transaction: transaction)
        let rank2 = try await getRank(root2, ontologyIRI: ontologyIRI, transaction: transaction)

        let newRoot: String
        let attached: String

        // Union by rank: attach smaller tree under larger
        if rank1 < rank2 {
            newRoot = root2
            attached = root1
        } else if rank1 > rank2 {
            newRoot = root1
            attached = root2
        } else {
            // Same rank: choose lexicographically smaller as root (deterministic)
            if root1 < root2 {
                newRoot = root1
                attached = root2
            } else {
                newRoot = root2
                attached = root1
            }
            // Increase rank of new root
            try await setRank(newRoot, rank: rank1 + 1, ontologyIRI: ontologyIRI, transaction: transaction)
        }

        // Set parent pointer
        let parentKey = subspace.sameAsParentKey(ontologyIRI, individual: attached)
        transaction.setValue(Array(newRoot.utf8), for: parentKey)

        // Update members index
        // Move all members of attached to newRoot
        let attachedMembers = try await getMembersInternal(attached, ontologyIRI: ontologyIRI, transaction: transaction)
        for member in attachedMembers {
            // Remove from old representative
            let oldMemberKey = subspace.sameAsMemberKey(ontologyIRI, representative: attached, member: member)
            transaction.clear(key: oldMemberKey)

            // Add to new representative
            let newMemberKey = subspace.sameAsMemberKey(ontologyIRI, representative: newRoot, member: member)
            transaction.setValue([], for: newMemberKey)
        }

        // Add the attached root itself as member of newRoot
        let attachedMemberKey = subspace.sameAsMemberKey(ontologyIRI, representative: newRoot, member: attached)
        transaction.setValue([], for: attachedMemberKey)

        // Ensure newRoot is also in its own members list
        let newRootMemberKey = subspace.sameAsMemberKey(ontologyIRI, representative: newRoot, member: newRoot)
        transaction.setValue([], for: newRootMemberKey)

        return newRoot
    }

    /// Get all members of an equivalence class
    ///
    /// - Parameters:
    ///   - individual: Any individual in the equivalence class
    ///   - ontologyIRI: The ontology IRI
    ///   - transaction: The transaction to use
    /// - Returns: Set of all equivalent individuals
    public func members(
        of individual: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        // Find the representative first
        let representative = try await find(individual, ontologyIRI: ontologyIRI, transaction: transaction)

        // Get members from index
        var members = try await getMembersInternal(representative, ontologyIRI: ontologyIRI, transaction: transaction)

        // Ensure the representative is included
        members.insert(representative)

        return members
    }

    /// Check if two individuals are in the same equivalence class
    public func areEquivalent(
        _ individual1: String,
        _ individual2: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Bool {
        let root1 = try await find(individual1, ontologyIRI: ontologyIRI, transaction: transaction)
        let root2 = try await find(individual2, ontologyIRI: ontologyIRI, transaction: transaction)
        return root1 == root2
    }

    /// Initialize an individual in the Union-Find structure
    ///
    /// Call this when first encountering an individual to ensure it has proper structure.
    public func makeSet(
        _ individual: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws {
        let parentKey = subspace.sameAsParentKey(ontologyIRI, individual: individual)

        // Only initialize if not already present
        if try await transaction.getValue(for: parentKey, snapshot: true) == nil {
            // Self-loop indicates root
            transaction.setValue(Array(individual.utf8), for: parentKey)
            try await setRank(individual, rank: 0, ontologyIRI: ontologyIRI, transaction: transaction)

            // Add to own members list
            let memberKey = subspace.sameAsMemberKey(ontologyIRI, representative: individual, member: individual)
            transaction.setValue([], for: memberKey)
        }
    }

    /// Get all equivalence classes
    ///
    /// Returns a dictionary mapping representatives to their members.
    public func getAllEquivalenceClasses(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> [String: Set<String>] {
        let (beginKey, endKey) = subspace.sameAsMembers(ontologyIRI).range()
        var classes: [String: Set<String>] = [:]

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.sameAsMembers(ontologyIRI).unpack(key),
               let representative = tuple[0] as? String,
               let member = tuple[1] as? String {
                classes[representative, default: []].insert(member)
            }
        }

        return classes
    }

    /// Get the number of equivalence classes
    public func countEquivalenceClasses(
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Int {
        let classes = try await getAllEquivalenceClasses(ontologyIRI: ontologyIRI, transaction: transaction)
        return classes.count
    }

    // MARK: - Private Helpers

    private func getRank(
        _ individual: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Int {
        let rankKey = subspace.sameAsRankKey(ontologyIRI, individual: individual)
        guard let data = try await transaction.getValue(for: rankKey, snapshot: true) else {
            return 0
        }
        // Decode as Int
        if data.count >= 8 {
            let value = data.withUnsafeBufferPointer { buffer in
                buffer.baseAddress!.withMemoryRebound(to: Int64.self, capacity: 1) { $0.pointee }
            }
            return Int(value)
        }
        return 0
    }

    private func setRank(
        _ individual: String,
        rank: Int,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws {
        let rankKey = subspace.sameAsRankKey(ontologyIRI, individual: individual)
        var value = Int64(rank)
        let data = withUnsafeBytes(of: &value) { Array($0) }
        transaction.setValue(data, for: rankKey)
    }

    private func getMembersInternal(
        _ representative: String,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        let (beginKey, endKey) = subspace.sameAsMembers(ontologyIRI).subspace(representative).range()
        var members: Set<String> = []

        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        for try await (key, _) in stream {
            if let tuple = try? subspace.sameAsMembers(ontologyIRI).subspace(representative).unpack(key),
               let member = tuple[0] as? String {
                members.insert(member)
            }
        }

        return members
    }
}

// MARK: - Batch Operations

extension PersistentUnionFind {
    /// Process multiple owl:sameAs assertions efficiently
    ///
    /// - Parameters:
    ///   - pairs: Array of (individual1, individual2) pairs
    ///   - ontologyIRI: The ontology IRI
    ///   - transaction: The transaction to use
    public func unionAll(
        _ pairs: [(String, String)],
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws {
        for (ind1, ind2) in pairs {
            try await union(ind1, ind2, ontologyIRI: ontologyIRI, transaction: transaction)
        }
    }

    /// Expand a set of individuals to include all equivalent individuals
    ///
    /// Useful for query expansion.
    ///
    /// - Parameters:
    ///   - individuals: Initial set of individuals
    ///   - ontologyIRI: The ontology IRI
    ///   - transaction: The transaction to use
    /// - Returns: Expanded set including all owl:sameAs equivalents
    public func expand(
        _ individuals: Set<String>,
        ontologyIRI: String,
        transaction: any TransactionProtocol
    ) async throws -> Set<String> {
        var expanded: Set<String> = []

        for individual in individuals {
            let equivalents = try await members(of: individual, ontologyIRI: ontologyIRI, transaction: transaction)
            expanded.formUnion(equivalents)
        }

        return expanded
    }
}
