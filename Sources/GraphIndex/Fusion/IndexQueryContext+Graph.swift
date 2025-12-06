// IndexQueryContext+Graph.swift
// GraphIndex - Extension for graph query execution

import Foundation
import Core
import Graph
import DatabaseEngine
import FoundationDB

/// Direction for graph traversal
public enum GraphDirection: Sendable {
    case outgoing
    case incoming
    case both
}

extension IndexQueryContext {

    /// Execute graph neighbor query
    ///
    /// Finds all nodes connected to the given node via edges.
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the graph index
    ///   - node: The node to find neighbors of
    ///   - edgeType: Optional edge type filter
    ///   - direction: Direction of edges to follow
    /// - Returns: Array of neighbor node identifiers
    public func executeGraphNeighbors<T: Persistable>(
        type: T.Type,
        indexName: String,
        node: String,
        edgeType: String?,
        direction: GraphDirection
    ) async throws -> [String] {
        // Get subspace via DirectoryLayer based on Persistable type
        let typeSubspace = try await indexSubspace(for: type)
        let indexSub = typeSubspace.subspace(indexName)

        // Find strategy from index descriptor
        let strategy = findGraphStrategy(for: T.self, indexName: indexName)

        let neighbors = try await withTransaction { transaction in
            var results: Set<String> = []

            switch direction {
            case .outgoing:
                let outNeighbors = try await self.queryOutgoingEdges(
                    transaction: transaction,
                    subspace: indexSub,
                    strategy: strategy,
                    from: node,
                    edgeType: edgeType
                )
                results.formUnion(outNeighbors)

            case .incoming:
                let inNeighbors = try await self.queryIncomingEdges(
                    transaction: transaction,
                    subspace: indexSub,
                    strategy: strategy,
                    to: node,
                    edgeType: edgeType
                )
                results.formUnion(inNeighbors)

            case .both:
                let outNeighbors = try await self.queryOutgoingEdges(
                    transaction: transaction,
                    subspace: indexSub,
                    strategy: strategy,
                    from: node,
                    edgeType: edgeType
                )
                let inNeighbors = try await self.queryIncomingEdges(
                    transaction: transaction,
                    subspace: indexSub,
                    strategy: strategy,
                    to: node,
                    edgeType: edgeType
                )
                results.formUnion(outNeighbors)
                results.formUnion(inNeighbors)
            }

            return results
        }

        return Array(neighbors)
    }

    /// Create a Connected query for Fusion
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(Person.self) {
    ///     context.indexQueryContext.connected(Person.self, \.userId)
    ///         .from("user123")
    ///         .via("follows")
    /// }
    /// ```
    public func connected<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, String>
    ) -> Connected<T> {
        Connected(keyPath, context: self)
    }

    /// Create a Connected query for optional field
    public func connected<T: Persistable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, String?>
    ) -> Connected<T> {
        Connected(keyPath, context: self)
    }

    // MARK: - Private Helpers

    private func findGraphStrategy<T: Persistable>(for type: T.Type, indexName: String) -> GraphIndexStrategy {
        guard let descriptor = T.indexDescriptors.first(where: { $0.name == indexName }),
              let kind = descriptor.kind as? GraphIndexKind<T> else {
            return .adjacency  // Default
        }
        return kind.strategy
    }

    private func queryOutgoingEdges(
        transaction: any TransactionProtocol,
        subspace: Subspace,
        strategy: GraphIndexStrategy,
        from: String,
        edgeType: String?
    ) async throws -> [String] {
        var results: [String] = []

        switch strategy {
        case .adjacency:
            // Key format: [out]/[edge]/[from]/[to]
            let outSubspace = subspace.subspace(GraphSubspaceKey.out.rawValue)
            if let edge = edgeType {
                let edgeFromSubspace = outSubspace.subspace(edge).subspace(from)
                let (beginKey, endKey) = edgeFromSubspace.range()
                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: false
                )
                for try await (key, _) in stream {
                    if let tuple = try? Tuple.unpack(from: key),
                       let to = tuple.last as? String {
                        results.append(to)
                    }
                }
            } else {
                // Scan all edge types for this from node
                let (beginKey, endKey) = outSubspace.range()
                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: false
                )
                for try await (key, _) in stream {
                    if let tuple = try? Tuple.unpack(from: key),
                       tuple.count >= 3,
                       let fromNode = tuple[tuple.count - 2] as? String,
                       let to = tuple.last as? String,
                       fromNode == from {
                        results.append(to)
                    }
                }
            }

        case .tripleStore, .hexastore:
            // Key format: [spo]/[from]/[edge]/[to]
            let spoSubspace = subspace.subspace(GraphSubspaceKey.spo.rawValue)
            let fromSubspace = spoSubspace.subspace(from)
            if let edge = edgeType {
                let edgeSubspace = fromSubspace.subspace(edge)
                let (beginKey, endKey) = edgeSubspace.range()
                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: false
                )
                for try await (key, _) in stream {
                    if let tuple = try? Tuple.unpack(from: key),
                       let to = tuple.last as? String {
                        results.append(to)
                    }
                }
            } else {
                let (beginKey, endKey) = fromSubspace.range()
                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: false
                )
                for try await (key, _) in stream {
                    if let tuple = try? Tuple.unpack(from: key),
                       let to = tuple.last as? String {
                        results.append(to)
                    }
                }
            }
        }

        return results
    }

    private func queryIncomingEdges(
        transaction: any TransactionProtocol,
        subspace: Subspace,
        strategy: GraphIndexStrategy,
        to: String,
        edgeType: String?
    ) async throws -> [String] {
        var results: [String] = []

        switch strategy {
        case .adjacency:
            // Key format: [in]/[edge]/[to]/[from]
            let inSubspace = subspace.subspace(GraphSubspaceKey.`in`.rawValue)
            if let edge = edgeType {
                let edgeToSubspace = inSubspace.subspace(edge).subspace(to)
                let (beginKey, endKey) = edgeToSubspace.range()
                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: false
                )
                for try await (key, _) in stream {
                    if let tuple = try? Tuple.unpack(from: key),
                       let from = tuple.last as? String {
                        results.append(from)
                    }
                }
            } else {
                let (beginKey, endKey) = inSubspace.range()
                let stream = transaction.getRange(
                    beginSelector: .firstGreaterOrEqual(beginKey),
                    endSelector: .firstGreaterOrEqual(endKey),
                    snapshot: false
                )
                for try await (key, _) in stream {
                    if let tuple = try? Tuple.unpack(from: key),
                       tuple.count >= 3,
                       let toNode = tuple[tuple.count - 2] as? String,
                       let from = tuple.last as? String,
                       toNode == to {
                        results.append(from)
                    }
                }
            }

        case .tripleStore, .hexastore:
            // Key format: [osp]/[to]/[from]/[edge]
            let ospSubspace = subspace.subspace(GraphSubspaceKey.osp.rawValue)
            let toSubspace = ospSubspace.subspace(to)
            let (beginKey, endKey) = toSubspace.range()
            let stream = transaction.getRange(
                beginSelector: .firstGreaterOrEqual(beginKey),
                endSelector: .firstGreaterOrEqual(endKey),
                snapshot: false
            )
            for try await (key, _) in stream {
                if let tuple = try? Tuple.unpack(from: key),
                   let from = tuple[tuple.count - 2] as? String {
                    if let edge = edgeType {
                        if let edgeValue = tuple.last as? String, edgeValue == edge {
                            results.append(from)
                        }
                    } else {
                        results.append(from)
                    }
                }
            }
        }

        return results
    }
}

// MARK: - Graph Subspace Keys

/// Subspace keys for graph index storage
enum GraphSubspaceKey: Int64 {
    case out = 0    // Outgoing edges: [out]/[edge]/[from]/[to]
    case `in` = 1   // Incoming edges: [in]/[edge]/[to]/[from]
    case spo = 2    // Subject-Predicate-Object: [spo]/[from]/[edge]/[to]
    case pos = 3    // Predicate-Object-Subject: [pos]/[edge]/[to]/[from]
    case osp = 4    // Object-Subject-Predicate: [osp]/[to]/[from]/[edge]
    case sop = 5    // Subject-Object-Predicate: [sop]/[from]/[to]/[edge]
    case pso = 6    // Predicate-Subject-Object: [pso]/[edge]/[from]/[to]
    case ops = 7    // Object-Predicate-Subject: [ops]/[to]/[edge]/[from]
}
