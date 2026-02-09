// SHACLShapesStore.swift
// GraphIndex - FDB persistence for SHACL shapes graphs
//
// Stores and retrieves SHACL shapes graphs in FoundationDB.
// Follows the OntologyStore pattern: transaction-receiver, JSON encoding.
//
// Reference: W3C SHACL §2.1 (Shapes Graph)
// https://www.w3.org/TR/shacl/#shapes-graph

import Foundation
import FoundationDB
import Graph

/// Persistent storage for SHACL shapes graphs in FoundationDB
///
/// **Key Layout**:
/// ```
/// [S]/0/[shapesGraphIRI]  → JSON-encoded SHACLShapesGraph
/// ```
///
/// SHACL shapes graphs are typically small (dozens of shapes),
/// so each graph is stored as a single JSON document.
struct SHACLShapesStore: Sendable {

    /// Subspace key for shapes graph entries
    private enum SubspaceKey: Int, Sendable {
        /// Shapes graph data
        case graphs = 0
    }

    private let subspace: Subspace

    init(subspace: Subspace) {
        self.subspace = subspace
    }

    // MARK: - Graph Subspace

    private var graphsSubspace: Subspace {
        subspace.subspace(SubspaceKey.graphs.rawValue)
    }

    private func graphKey(_ iri: String) -> FDB.Bytes {
        graphsSubspace.pack(Tuple(iri))
    }

    // MARK: - Save

    /// Save a shapes graph
    ///
    /// If a shapes graph with the same IRI already exists, it will be replaced.
    ///
    /// - Parameters:
    ///   - graph: The shapes graph to save
    ///   - transaction: The FDB transaction
    func save(
        _ graph: SHACLShapesGraph,
        transaction: any TransactionProtocol
    ) throws {
        let data = try JSONEncoder().encode(graph)
        let key = graphKey(graph.iri)
        transaction.setValue(Array(data), for: key)
    }

    // MARK: - Get

    /// Get a shapes graph by IRI
    ///
    /// - Parameters:
    ///   - iri: The shapes graph IRI
    ///   - transaction: The FDB transaction
    /// - Returns: The shapes graph, or nil if not found
    func get(
        iri: String,
        transaction: any TransactionProtocol
    ) async throws -> SHACLShapesGraph? {
        let key = graphKey(iri)
        guard let data = try await transaction.getValue(for: key, snapshot: true) else {
            return nil
        }
        return try JSONDecoder().decode(SHACLShapesGraph.self, from: Data(data))
    }

    // MARK: - List

    /// List all shapes graph IRIs
    ///
    /// - Parameter transaction: The FDB transaction
    /// - Returns: Array of shapes graph IRIs
    func listGraphIRIs(
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let (beginKey, endKey) = graphsSubspace.range()
        let stream = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(beginKey),
            endSelector: .firstGreaterOrEqual(endKey),
            snapshot: true
        )

        var iris: [String] = []
        for try await (key, _) in stream {
            if let tuple = try? graphsSubspace.unpack(key),
               let iri = tuple[0] as? String {
                iris.append(iri)
            }
        }
        return iris
    }

    // MARK: - Delete

    /// Delete a shapes graph by IRI
    ///
    /// - Parameters:
    ///   - iri: The shapes graph IRI to delete
    ///   - transaction: The FDB transaction
    func delete(
        iri: String,
        transaction: any TransactionProtocol
    ) {
        let key = graphKey(iri)
        transaction.clear(key: key)
    }

    /// Delete all shapes graphs
    ///
    /// - Parameter transaction: The FDB transaction
    func deleteAll(
        transaction: any TransactionProtocol
    ) {
        let (beginKey, endKey) = graphsSubspace.range()
        transaction.clearRange(beginKey: beginKey, endKey: endKey)
    }
}
