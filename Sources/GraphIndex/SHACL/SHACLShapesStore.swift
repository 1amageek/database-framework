// SHACLShapesStore.swift
// GraphIndex - FDB persistence for SHACL shapes graphs
//
// Stores and retrieves bounded canonical SHACL shape frames.
//
// Reference: W3C SHACL §2.1 (Shapes Graph)
// https://www.w3.org/TR/shacl/#shapes-graph

import StorageKit
import DatabaseKit
import DatabaseTypes
/// Persistent storage for SHACL shapes graphs in FoundationDB
///
/// **Key Layout**:
/// ```
/// [S]/0/[shapesGraphIRI]  → canonical SHACL storage frame
/// ```
///
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

    private func graphKey(_ iri: String) -> ByteString {
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
        transaction: any TransactionAccess
    ) throws {
        let frame = try SHACLShapesGraphStorageFormat.encode(graph)
        let key = graphKey(graph.iri)
        try transaction.setValue(frame, for: key)
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
        transaction: any TransactionReadAccess
    ) async throws -> SHACLShapesGraph? {
        let key = graphKey(iri)
        guard let data = try await transaction.getValue(for: key, snapshot: true) else {
            return nil
        }
        return try SHACLShapesGraphStorageFormat.decode(data)
    }

    // MARK: - List

    /// List all shapes graph IRIs
    ///
    /// - Parameter transaction: The FDB transaction
    /// - Returns: Array of shapes graph IRIs
    func listGraphIRIs(
        transaction: any TransactionReadAccess
    ) async throws -> [String] {
        let (beginKey, endKey) = graphsSubspace.range()
        let stream = try await TransactionRangeCollection.collect(using: transaction,
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )

        var iris: [String] = []
        for (key, _) in stream {
            let tuple = try graphsSubspace.unpack(key)
            guard tuple.count == 1,
                  let element = tuple[0],
                  case .string(let iri) = element.tupleValue else {
                throw StorageError(
                    code: .dataCorruption,
                    operation: .read,
                    message: "SHACL shapes graph key has an invalid tuple layout"
                )
            }
            iris.append(iri)
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
        transaction: any TransactionAccess
    ) throws {
        let key = graphKey(iri)
        try transaction.clear(key: key)
    }

    /// Delete all shapes graphs
    ///
    /// - Parameter transaction: The FDB transaction
    func deleteAll(
        transaction: any TransactionAccess
    ) throws {
        let (beginKey, endKey) = graphsSubspace.range()
        try transaction.clearRange(beginKey: beginKey, endKey: endKey)
    }
}
