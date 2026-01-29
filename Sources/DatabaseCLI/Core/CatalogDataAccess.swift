/// CatalogDataAccess - Direct FDB data access using TypeCatalog metadata
///
/// Provides read/write operations on FDB data without requiring compiled @Persistable types.
/// Uses DirectoryLayer for path resolution, ItemEnvelope for wire format,
/// and DynamicProtobufDecoder/Encoder for Protobuf serialization.

import Foundation
import FoundationDB
import DatabaseEngine
import Core

/// Direct FDB data access driven by TypeCatalog
struct CatalogDataAccess: Sendable {
    nonisolated(unsafe) let database: any DatabaseProtocol
    private let catalogs: [String: TypeCatalog]
    private let transformer = TransformingSerializer(configuration: .default)

    init(database: any DatabaseProtocol, catalogs: [TypeCatalog]) {
        self.database = database
        var map: [String: TypeCatalog] = [:]
        for catalog in catalogs {
            map[catalog.typeName] = catalog
        }
        self.catalogs = map
    }

    func catalog(for typeName: String) throws -> TypeCatalog {
        guard let catalog = catalogs[typeName] else {
            throw CLIError.entityNotFound(typeName)
        }
        return catalog
    }

    var allCatalogs: [TypeCatalog] {
        catalogs.values.sorted { $0.typeName < $1.typeName }
    }

    // MARK: - Directory Resolution

    /// Resolve the FDB subspace for a type using its catalog's directory path
    func resolveSubspace(for catalog: TypeCatalog, partitionValues: [String: String] = [:]) async throws -> Subspace {
        let path = try catalog.resolvedDirectoryPath(partitionValues: partitionValues)
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path)
        return dirSubspace.subspace
    }

    /// Build the item subspace: [directory]/R/[typeName]
    func itemSubspace(for catalog: TypeCatalog, partitionValues: [String: String] = [:]) async throws -> Subspace {
        let subspace = try await resolveSubspace(for: catalog, partitionValues: partitionValues)
        return subspace.subspace(SubspaceKey.items).subspace(catalog.typeName)
    }

    // MARK: - Get by ID

    /// Fetch a single record by ID, returning decoded dictionary
    func get(typeName: String, id: String, partitionValues: [String: String] = [:]) async throws -> [String: Any]? {
        let catalog = try catalog(for: typeName)
        let typeSubspace = try await itemSubspace(for: catalog, partitionValues: partitionValues)
        let key = typeSubspace.pack(Tuple([id]))

        // Get raw bytes inside transaction
        let rawValue: FDB.Bytes? = try await database.withTransaction { transaction in
            try await transaction.getValue(for: key, snapshot: false)
        }

        guard let rawValue else { return nil }

        // Decode outside transaction (avoid Sendable issues with [String: Any])
        let protobufBytes = try unwrapEnvelope(rawValue)
        var dict = try DynamicProtobufDecoder.decode(protobufBytes, catalog: catalog)
        dict["id"] = id
        return dict
    }

    // MARK: - Find (range scan)

    /// Fetch all records for a type, returning decoded dictionaries
    func findAll(typeName: String, limit: Int?, partitionValues: [String: String] = [:]) async throws -> [[String: Any]] {
        let catalog = try catalog(for: typeName)
        let typeSubspace = try await itemSubspace(for: catalog, partitionValues: partitionValues)
        let (begin, end) = typeSubspace.range()

        // Collect raw key-value pairs inside transaction
        let rawPairs: [(key: FDB.Bytes, value: FDB.Bytes)] = try await database.withTransaction { transaction in
            var pairs: [(key: FDB.Bytes, value: FDB.Bytes)] = []
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

            for try await (key, value) in sequence {
                pairs.append((key: key, value: value))
                if let limit, pairs.count >= limit {
                    break
                }
            }

            return pairs
        }

        // Decode outside transaction
        var results: [[String: Any]] = []
        for (key, value) in rawPairs {
            let id = extractID(from: key, subspace: typeSubspace)
            let protobufBytes = try unwrapEnvelope(value)
            var dict = try DynamicProtobufDecoder.decode(protobufBytes, catalog: catalog)
            dict["id"] = id
            results.append(dict)
        }

        return results
    }

    // MARK: - Insert

    /// Insert a record from a JSON dictionary
    ///
    /// **Warning**: CLI writes do NOT update indexes.
    func insert(typeName: String, dict: [String: Any], partitionValues: [String: String] = [:]) async throws {
        let catalog = try catalog(for: typeName)
        let typeSubspace = try await itemSubspace(for: catalog, partitionValues: partitionValues)

        guard let id = dict["id"] as? String else {
            throw CLIError.invalidJSON("JSON must contain an 'id' field")
        }

        // Encode to Protobuf (include all fields including id)
        let protobufBytes = try DynamicProtobufEncoder.encode(dict, catalog: catalog)

        // Compress + wrap in ItemEnvelope
        let compressed = try transformer.serializeSync(Data(protobufBytes))
        let envelope = ItemEnvelope.inline(data: Array(compressed))
        let envelopeBytes = envelope.serialize()

        let key = typeSubspace.pack(Tuple([id]))

        try await database.withTransaction { transaction in
            transaction.setValue(envelopeBytes, for: key)
        }
    }

    // MARK: - Delete

    /// Delete a record by ID
    func delete(typeName: String, id: String, partitionValues: [String: String] = [:]) async throws {
        let catalog = try catalog(for: typeName)
        let typeSubspace = try await itemSubspace(for: catalog, partitionValues: partitionValues)
        let key = typeSubspace.pack(Tuple([id]))

        try await database.withTransaction { transaction in
            transaction.clear(key: key)
        }
    }

    // MARK: - Envelope Helpers

    /// Unwrap ItemEnvelope bytes -> decompressed Protobuf bytes
    private func unwrapEnvelope(_ rawValue: FDB.Bytes) throws -> [UInt8] {
        guard ItemEnvelope.isEnvelope(rawValue) else {
            // Not an envelope — treat as raw Protobuf
            return rawValue
        }

        let envelope = try ItemEnvelope.deserialize(rawValue)

        let compressed: FDB.Bytes
        switch envelope.content {
        case .inline(let data):
            compressed = data
        case .external:
            throw CLIError.invalidJSON("External blob loading not supported in CLI")
        }

        // Decompress
        let decompressed = try transformer.deserializeSync(Data(compressed))
        return Array(decompressed)
    }

    // MARK: - Key Helpers

    /// Extract ID string from an FDB key by unpacking the Tuple suffix
    private func extractID(from key: FDB.Bytes, subspace: Subspace) -> String {
        do {
            let tuple = try subspace.unpack(key)
            if let first = tuple[0] {
                return "\(first)"
            }
        } catch {
            // Fallback
        }
        return "<unknown>"
    }
}
