import Foundation
import FoundationDB

/// Handler for version indexes (history tracking, point-in-time queries)
///
/// Storage layout:
/// - snapshots/<id>/<versionstamp> = JSON snapshot of item
/// - current/<id> = current version number
public struct VersionIndexHandler: IndexHandler, Sendable {
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
        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .version,
            indexName: indexDefinition.name
        )
        let snapshotsSubspace = indexSubspace.subspace(Tuple(["snapshots", id]))
        let currentSubspace = indexSubspace.subspace(Tuple(["current"]))

        // Get current version number
        let currentKey = currentSubspace.pack(Tuple([id]))
        let currentVersionBytes = try await transaction.getValue(for: currentKey, snapshot: false)
        var versionNumber: Int64 = currentVersionBytes.map { unpackInt64($0) } ?? 0

        // If there's a new item, create a snapshot
        if let item = newItem {
            versionNumber += 1

            // Store snapshot with version number
            let snapshotKey = snapshotsSubspace.pack(Tuple([versionNumber]))
            let snapshotData = try JSONSerialization.data(withJSONObject: item, options: [])
            transaction.setValue(Array(snapshotData), for: snapshotKey)

            // Update current version
            transaction.setValue(packInt64(versionNumber), for: currentKey)

            // Apply retention policy
            if let config = indexDefinition.config,
               case .version(let versionConfig) = config {
                try await applyRetention(
                    subspace: snapshotsSubspace,
                    currentVersion: versionNumber,
                    retention: versionConfig.retention,
                    transaction: transaction
                )
            }
        } else if oldItem != nil {
            // Item deleted - store deletion marker
            versionNumber += 1
            let snapshotKey = snapshotsSubspace.pack(Tuple([versionNumber]))
            transaction.setValue(Array("__DELETED__".utf8), for: snapshotKey)
            transaction.setValue(packInt64(versionNumber), for: currentKey)
        }
    }

    public func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String] {
        guard let versionQuery = query as? VersionQuery else {
            return []
        }

        let indexSubspace = storage.indexSubspace(
            schema: schemaName,
            kind: .version,
            indexName: indexDefinition.name
        )

        switch versionQuery {
        case .history(let id):
            return try await getHistory(
                id: id,
                subspace: indexSubspace,
                limit: limit,
                transaction: transaction
            )

        case .atVersion(let id, let version):
            return try await getAtVersion(
                id: id,
                version: version,
                subspace: indexSubspace,
                transaction: transaction
            )

        case .diff(let id, let v1, let v2):
            return try await getDiff(
                id: id,
                v1: v1,
                v2: v2,
                subspace: indexSubspace,
                transaction: transaction
            )
        }
    }

    // MARK: - Version Operations

    private func getHistory(
        id: String,
        subspace: Subspace,
        limit: Int,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let snapshotsSubspace = subspace.subspace(Tuple(["snapshots", id]))
        let (begin, end) = snapshotsSubspace.range()

        var versions: [String] = []
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        for try await (key, value) in sequence {
            if let tuple = try? snapshotsSubspace.unpack(key),
               let version = tuple[0] as? Int64 {
                let data = String(decoding: value, as: UTF8.self)
                if data == "__DELETED__" {
                    versions.append("v\(version): [DELETED]")
                } else {
                    versions.append("v\(version): \(data)")
                }
            }
        }

        // Return in reverse order (newest first) and limit
        return Array(versions.reversed().prefix(limit))
    }

    private func getAtVersion(
        id: String,
        version: Int64,
        subspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let snapshotsSubspace = subspace.subspace(Tuple(["snapshots", id]))
        let key = snapshotsSubspace.pack(Tuple([version]))

        if let bytes = try await transaction.getValue(for: key, snapshot: true) {
            let data = String(decoding: bytes, as: UTF8.self)
            return [data]
        }

        return ["Version not found"]
    }

    private func getDiff(
        id: String,
        v1: Int64,
        v2: Int64,
        subspace: Subspace,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let snapshotsSubspace = subspace.subspace(Tuple(["snapshots", id]))

        let key1 = snapshotsSubspace.pack(Tuple([v1]))
        let key2 = snapshotsSubspace.pack(Tuple([v2]))

        let bytes1 = try await transaction.getValue(for: key1, snapshot: true)
        let bytes2 = try await transaction.getValue(for: key2, snapshot: true)

        guard let b1 = bytes1, let b2 = bytes2 else {
            return ["One or both versions not found"]
        }

        // Parse as JSON and compute diff
        guard let dict1 = try? JSONSerialization.jsonObject(with: Data(b1), options: []) as? [String: Any],
              let dict2 = try? JSONSerialization.jsonObject(with: Data(b2), options: []) as? [String: Any] else {
            return ["Could not parse versions as JSON"]
        }

        var diffs: [String] = []

        // Find changed/added fields
        for (key, value2) in dict2 {
            if let value1 = dict1[key] {
                if !areEqual(value1, value2) {
                    diffs.append("~ \(key): \(value1) -> \(value2)")
                }
            } else {
                diffs.append("+ \(key): \(value2)")
            }
        }

        // Find removed fields
        for (key, value1) in dict1 {
            if dict2[key] == nil {
                diffs.append("- \(key): \(value1)")
            }
        }

        return diffs.isEmpty ? ["No differences"] : diffs
    }

    // MARK: - Retention

    private func applyRetention(
        subspace: Subspace,
        currentVersion: Int64,
        retention: RetentionPolicy,
        transaction: any TransactionProtocol
    ) async throws {
        switch retention {
        case .keepAll:
            return // No cleanup needed

        case .keepLast(let n):
            let minVersion = currentVersion - Int64(n)
            if minVersion > 0 {
                let beginKey = subspace.pack(Tuple([Int64(1)]))
                let endKey = subspace.pack(Tuple([minVersion]))
                transaction.clearRange(beginKey: beginKey, endKey: endKey)
            }

        case .keepForDuration:
            // Would need timestamp tracking - skip for simplicity
            return
        }
    }

    // MARK: - Helpers

    private func packInt64(_ value: Int64) -> FDB.Bytes {
        var v = value
        return withUnsafeBytes(of: &v) { Array($0) }
    }

    private func unpackInt64(_ bytes: FDB.Bytes) -> Int64 {
        guard bytes.count >= 8 else { return 0 }
        return bytes.withUnsafeBytes { $0.load(as: Int64.self) }
    }

    private func areEqual(_ a: Any, _ b: Any) -> Bool {
        switch (a, b) {
        case (let a as String, let b as String): return a == b
        case (let a as Int, let b as Int): return a == b
        case (let a as Double, let b as Double): return a == b
        case (let a as Bool, let b as Bool): return a == b
        default: return false
        }
    }
}

// MARK: - Version Query

public enum VersionQuery {
    case history(id: String)
    case atVersion(id: String, version: Int64)
    case diff(id: String, v1: Int64, v2: Int64)
}
