/// CatalogDataAccess - Direct FDB data access using Schema.Entity metadata
///
/// Provides read/write operations on FDB data without requiring compiled @Persistable types.
/// Uses DirectoryLayer for path resolution, ItemEnvelope for wire format,
/// and DynamicProtobufDecoder/Encoder for Protobuf serialization.

import Foundation
import FoundationDB
import DatabaseEngine
import Core
import Graph

/// Direct FDB data access driven by Schema.Entity
public struct CatalogDataAccess: Sendable {
    nonisolated(unsafe) public let database: any DatabaseProtocol
    private let entities: [String: Schema.Entity]
    private let transformer = TransformingSerializer(configuration: .default)

    public init(database: any DatabaseProtocol, entities: [Schema.Entity]) {
        self.database = database
        var map: [String: Schema.Entity] = [:]
        for entity in entities {
            map[entity.name] = entity
        }
        self.entities = map
    }

    func entity(for typeName: String) throws -> Schema.Entity {
        guard let entity = entities[typeName] else {
            throw CLIError.entityNotFound(typeName)
        }
        return entity
    }

    var allEntities: [Schema.Entity] {
        entities.values.sorted { $0.name < $1.name }
    }

    // MARK: - Directory Resolution

    /// Resolve the FDB subspace for a type using its entity's directory path
    func resolveSubspace(for entity: Schema.Entity, partitionValues: [String: String] = [:]) async throws -> Subspace {
        let path = try entity.resolvedDirectoryPath(partitionValues: partitionValues)
        let directoryLayer = DirectoryLayer(database: database)
        let dirSubspace = try await directoryLayer.createOrOpen(path: path)
        return dirSubspace.subspace
    }

    /// Build the item subspace: [directory]/R/[typeName]
    func itemSubspace(for entity: Schema.Entity, partitionValues: [String: String] = [:]) async throws -> Subspace {
        let subspace = try await resolveSubspace(for: entity, partitionValues: partitionValues)
        return subspace.subspace(SubspaceKey.items).subspace(entity.name)
    }

    // MARK: - Get by ID

    /// Fetch a single record by ID, returning decoded dictionary
    public func get(typeName: String, id: String, partitionValues: [String: String] = [:]) async throws -> [String: Any]? {
        let entity = try entity(for: typeName)
        let typeSubspace = try await itemSubspace(for: entity, partitionValues: partitionValues)
        let key = typeSubspace.pack(Tuple([id]))

        // Get raw bytes inside transaction
        let rawValue: FDB.Bytes? = try await database.withTransaction { transaction in
            try await transaction.getValue(for: key, snapshot: false)
        }

        guard let rawValue else { return nil }

        // Decode outside transaction (avoid Sendable issues with [String: Any])
        let protobufBytes = try unwrapEnvelope(rawValue)
        var dict = try DynamicProtobufDecoder.decode(protobufBytes, entity: entity)
        dict["id"] = id
        return dict
    }

    // MARK: - Find (range scan)

    /// Fetch all records for a type, returning decoded dictionaries
    public func findAll(typeName: String, limit: Int?, partitionValues: [String: String] = [:]) async throws -> [[String: Any]] {
        let entity = try entity(for: typeName)
        let typeSubspace = try await itemSubspace(for: entity, partitionValues: partitionValues)
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
            var dict = try DynamicProtobufDecoder.decode(protobufBytes, entity: entity)
            dict["id"] = id
            results.append(dict)
        }

        return results
    }

    // MARK: - Insert

    /// Insert a record from a JSON dictionary
    ///
    /// **Warning**: CLI writes do NOT update indexes.
    public func insert(typeName: String, dict: [String: Any], partitionValues: [String: String] = [:]) async throws {
        let entity = try entity(for: typeName)
        let typeSubspace = try await itemSubspace(for: entity, partitionValues: partitionValues)

        guard let id = dict["id"] as? String else {
            throw CLIError.invalidJSON("JSON must contain an 'id' field")
        }

        // Encode to Protobuf (include all fields including id)
        let protobufBytes = try DynamicProtobufEncoder.encode(dict, entity: entity)

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
    public func delete(typeName: String, id: String, partitionValues: [String: String] = [:]) async throws {
        let entity = try entity(for: typeName)
        let typeSubspace = try await itemSubspace(for: entity, partitionValues: partitionValues)
        let key = typeSubspace.pack(Tuple([id]))

        try await database.withTransaction { transaction in
            transaction.clear(key: key)
        }
    }

    // MARK: - Index Resolution

    /// Build the index subspace: [directory]/I
    func indexSubspace(for entity: Schema.Entity, partitionValues: [String: String] = [:]) async throws -> Subspace {
        let subspace = try await resolveSubspace(for: entity, partitionValues: partitionValues)
        return subspace.subspace(SubspaceKey.indexes)
    }

    /// Resolve graph index metadata from entity
    ///
    /// Extracts the graph index name, strategy, and field roles from the
    /// `AnyIndexDescriptor.kind.metadata` dictionary (populated by JSON-encoding the IndexKind).
    ///
    /// - Parameter entity: The schema entity to extract graph index metadata from
    /// - Returns: Tuple of (indexName, strategy, fromField, edgeField, toField)
    /// - Throws: `CLIError` if the type has no graph index or metadata is incomplete
    func graphIndexMetadata(for entity: Schema.Entity) throws -> (
        indexName: String,
        strategy: GraphIndexStrategy,
        fromField: String,
        edgeField: String,
        toField: String
    ) {
        guard let graphIndex = entity.indexes.first(where: { $0.kindIdentifier == "graph" }) else {
            throw CLIError.invalidArguments("Type '\(entity.name)' has no graph index")
        }

        guard let strategyStr = graphIndex.kind.metadata["strategy"]?.stringValue,
              let strategy = GraphIndexStrategy(rawValue: strategyStr) else {
            throw CLIError.invalidArguments("Graph index '\(graphIndex.name)' missing strategy metadata. Re-run schema registration to populate metadata.")
        }

        let fromField = graphIndex.kind.metadata["fromField"]?.stringValue ?? graphIndex.fieldNames[0]
        let edgeField = graphIndex.kind.metadata["edgeField"]?.stringValue ?? (graphIndex.fieldNames.count > 1 ? graphIndex.fieldNames[1] : "")
        let toField = graphIndex.kind.metadata["toField"]?.stringValue ?? (graphIndex.fieldNames.count > 2 ? graphIndex.fieldNames[2] : "")

        return (graphIndex.name, strategy, fromField, edgeField, toField)
    }

    // MARK: - Clear All

    /// Clear all data for a type (items, indexes, metadata, blobs)
    func clearAll(typeName: String, partitionValues: [String: String] = [:]) async throws {
        let entity = try entity(for: typeName)
        let subspace = try await resolveSubspace(for: entity, partitionValues: partitionValues)
        let (begin, end) = subspace.range()
        try await database.withTransaction { transaction in
            transaction.clearRange(beginKey: begin, endKey: end)
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
