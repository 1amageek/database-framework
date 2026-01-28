import Foundation
import FoundationDB

/// Handler for graph indexes (social graphs, knowledge graphs)
///
/// Storage layout:
/// - out/<from>/<to> = edge data
/// - in/<to>/<from> = edge data (reverse index)
public struct GraphIndexHandler: IndexHandler, Sendable {
    public let indexDefinition: IndexDefinition
    public let schemaName: String

    public init(indexDefinition: IndexDefinition, schemaName: String) {
        self.indexDefinition = indexDefinition
        self.schemaName = schemaName
    }

    public func updateIndex(
        oldItem: [String: Any]?,
        newItem: [String: Any]?,
        id: String,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws {
        guard let config = indexDefinition.config,
              case .graph(let graphConfig) = config else {
            return
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .graph,
            indexName: indexDefinition.name
        )
        let outSubspace = indexSubspace.subspace(Tuple(["out"]))
        let inSubspace = indexSubspace.subspace(Tuple(["in"]))

        // Extract edge endpoints
        let oldFrom = oldItem?[graphConfig.fromField] as? String
        let oldTo = oldItem?[graphConfig.toField] as? String
        let newFrom = newItem?[graphConfig.fromField] as? String
        let newTo = newItem?[graphConfig.toField] as? String

        // Remove old edges
        if let from = oldFrom, let to = oldTo {
            let outKey = outSubspace.pack(Tuple([from, to, id]))
            let inKey = inSubspace.pack(Tuple([to, from, id]))
            transaction.clear(key: outKey)
            transaction.clear(key: inKey)
        }

        // Add new edges
        if let from = newFrom, let to = newTo {
            let outKey = outSubspace.pack(Tuple([from, to, id]))
            let inKey = inSubspace.pack(Tuple([to, from, id]))

            // Store edge label if present
            var edgeData: FDB.Bytes = []
            if let labelField = graphConfig.edgeLabelField,
               let label = newItem?[labelField] as? String {
                edgeData = Array(label.utf8)
            }

            transaction.setValue(edgeData, for: outKey)
            transaction.setValue(edgeData, for: inKey)
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let graphQuery = query as? GraphQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .graph,
            indexName: indexDefinition.name
        )
        let outSubspace = indexSubspace.subspace(Tuple(["out"]))
        let inSubspace = indexSubspace.subspace(Tuple(["in"]))

        switch graphQuery {
        case .outgoing(let from):
            return try await scanEdges(
                subspace: outSubspace,
                node: from,
                limit: limit,
                transaction: transaction
            )

        case .incoming(let to):
            return try await scanEdges(
                subspace: inSubspace,
                node: to,
                limit: limit,
                transaction: transaction
            )

        case .traverse(let start, let depth):
            return try await bfsTraverse(
                start: start,
                maxDepth: depth,
                outSubspace: outSubspace,
                limit: limit,
                transaction: transaction
            )

        case .shortestPath(let from, let to):
            return try await findShortestPath(
                from: from,
                to: to,
                outSubspace: outSubspace,
                transaction: transaction
            )
        }
    }

    // MARK: - Graph Operations

    private func scanEdges(
        subspace: Subspace,
        node: String,
        limit: Int,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let nodeSubspace = subspace.subspace(Tuple([node]))
        let (begin, end) = nodeSubspace.range()

        var neighbors: [String] = []
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (key, _) in sequence {
            guard neighbors.count < limit else { break }
            if let tuple = try? nodeSubspace.unpack(key),
               let neighbor = tuple[0] as? String {
                neighbors.append(neighbor)
            }
        }

        return neighbors
    }

    private func bfsTraverse(
        start: String,
        maxDepth: Int,
        outSubspace: Subspace,
        limit: Int,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        var visited = Set<String>()
        var queue: [(node: String, depth: Int)] = [(start, 0)]
        var result: [String] = []

        while !queue.isEmpty && result.count < limit {
            let (current, depth) = queue.removeFirst()

            if visited.contains(current) { continue }
            visited.insert(current)

            if current != start {
                result.append(current)
            }

            if depth < maxDepth {
                let neighbors = try await scanEdges(
                    subspace: outSubspace,
                    node: current,
                    limit: limit,
                    transaction: transaction
                )

                for neighbor in neighbors where !visited.contains(neighbor) {
                    queue.append((neighbor, depth + 1))
                }
            }
        }

        return result
    }

    private func findShortestPath(
        from: String,
        to: String,
        outSubspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        var visited = Set<String>()
        var queue: [(node: String, path: [String])] = [(from, [from])]

        while !queue.isEmpty {
            let (current, path) = queue.removeFirst()

            if current == to {
                return path
            }

            if visited.contains(current) { continue }
            visited.insert(current)

            let neighbors = try await scanEdges(
                subspace: outSubspace,
                node: current,
                limit: 1000,
                transaction: transaction
            )

            for neighbor in neighbors where !visited.contains(neighbor) {
                queue.append((neighbor, path + [neighbor]))
            }
        }

        return [] // No path found
    }
}

// MARK: - Graph Query

public enum GraphQuery {
    case outgoing(String)
    case incoming(String)
    case traverse(start: String, depth: Int)
    case shortestPath(from: String, to: String)
}
