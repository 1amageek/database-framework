import DatabaseKit
import DatabaseTypes

enum SQLCatalogCodec {
    private static let magic: UInt32 = 0x434C_5153
    private static let version: UInt16 = 1
    private static let digestFootprint = ByteString(repeating: 0, count: SHA256Accumulator.digestByteCount)

    @discardableResult
    static func admit(_ catalog: SQLCatalog, fingerprint: SchemaFingerprint,
        protectedContracts: [SQLCatalogProtectedContract], limits: StorageFrameLimits,
        runtimeConfiguration: DatabaseRuntimeConfiguration? = nil
    ) throws -> Int {
        // Existing protection keys have an admitted input footprint. Count new
        // compiled witnesses without allocating their digests or lookup indexes.
        let byteCount = try StorageFrameEncoder.measure(limits: limits) { writer throws(StorageFrameError) in
            try write(catalog, fingerprint: fingerprint, protectedContracts: protectedContracts, into: &writer)
            try writer.writeBytes(digestFootprint)
        }
        guard let runtimeConfiguration else { return byteCount }
        let existing = Set(protectedContracts.map { SQLCatalogName($0.relationID) })
        var additions = 0
        for relation in catalog.relations where !existing.contains(SQLCatalogName(relation.id)) {
            if runtimeConfiguration.entityRuntimes.registration(named: relation.entityName)?.compiledModelContract != nil {
                let next = additions.addingReportingOverflow(1)
                guard !next.overflow else { throw StorageFrameError.byteCountOverflow }
                additions = next.partialValue
            }
        }
        let count = protectedContracts.count.addingReportingOverflow(additions)
        guard !count.overflow else { throw StorageFrameError.byteCountOverflow }
        return try StorageFrameEncoder.measure(limits: limits) { writer throws(StorageFrameError) in
            try writeDeclaration(catalog, fingerprint: fingerprint, into: &writer)
            try writer.writeCount(count.partialValue)
            for protection in protectedContracts { try writeProtection(protection, into: &writer) }
            for relation in catalog.relations where !existing.contains(SQLCatalogName(relation.id)) {
                if runtimeConfiguration.entityRuntimes.registration(named: relation.entityName)?.compiledModelContract != nil {
                    writer.writeUInt8(SQLCatalogProtectedContract.Source.compiledModel.rawValue)
                    try writer.writeString(relation.id)
                    try writer.writeString(relation.entityName)
                    try writer.writeBytes(digestFootprint)
                }
            }
            try writer.writeBytes(digestFootprint)
        }
    }

    static func canonicalBytes(_ catalog: SQLCatalog, fingerprint: SchemaFingerprint,
        protectedContracts: [SQLCatalogProtectedContract], limits: StorageFrameLimits = .default
    ) throws -> ByteString {
        try StorageFrameEncoder.encode(limits: limits) { writer throws(StorageFrameError) in
            try write(catalog, fingerprint: fingerprint, protectedContracts: protectedContracts, into: &writer)
        }
    }

    static func encode(_ snapshot: SQLCatalogSnapshot,
        limits: StorageFrameLimits = .default
    ) throws -> ByteString {
        try encode(SQLCatalogStoredState(catalog: snapshot.catalog,
            physicalSchemaFingerprint: snapshot.physicalSchemaFingerprint,
            protectedContracts: snapshot.protectedContracts, digest: snapshot.digest), limits: limits)
    }

    static func encode(_ state: SQLCatalogStoredState,
        limits: StorageFrameLimits = .default
    ) throws -> ByteString {
        // Persistence requires one final owner; no intermediate payload is copied.
        try StorageFrameEncoder.encode(limits: limits) { writer throws(StorageFrameError) in
            try write(state.catalog, fingerprint: state.physicalSchemaFingerprint,
                protectedContracts: state.protectedContracts, into: &writer)
            try writer.writeBytes(state.digest)
        }
    }

    static func decode(_ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws -> SQLCatalogStoredState {
        var reader = try StorageFrameDecoder(bytes, limits: limits)
        guard try reader.readUInt32() == magic else { throw StorageFrameError.invalidMagic }
        let decodedVersion = try reader.readUInt16()
        guard decodedVersion == version else { throw StorageFrameError.unsupportedVersion(decodedVersion) }
        let fingerprint = try SchemaFingerprint(reader.readBytes())
        let namespaceCount = try count(from: &reader, minimumBytes: 8)
        var namespaces: [SQLNamespace] = []
        namespaces.reserveCapacity(namespaceCount)
        for _ in 0..<namespaceCount {
            namespaces.append(try SQLNamespace(id: reader.readString(), name: reader.readString()))
        }
        let relationCount = try count(from: &reader, minimumBytes: 21)
        var relations: [SQLRelation] = []
        relations.reserveCapacity(relationCount)
        for _ in 0..<relationCount {
            let id = try reader.readString()
            let namespaceID = try reader.readString()
            let name = try reader.readString()
            let entityName = try reader.readString()
            let columnCount = try count(from: &reader, minimumBytes: 16)
            var columns: [SQLColumn] = []
            columns.reserveCapacity(columnCount)
            for _ in 0..<columnCount {
                let id = try reader.readString()
                let name = try reader.readString()
                guard let number = Int(exactly: try reader.readInt64()) else {
                    throw StorageFrameError.integerOutOfRange
                }
                columns.append(try SQLColumn(id: id, name: name, fieldNumber: number))
            }
            var key: [String]?
            if try reader.readBool() {
                let keyCount = try count(from: &reader, minimumBytes: 4)
                var ids: [String] = []
                ids.reserveCapacity(keyCount)
                for _ in 0..<keyCount { ids.append(try reader.readString()) }
                key = ids
            }
            relations.append(try SQLRelation(id: id, namespaceID: namespaceID, name: name,
                entityName: entityName, columns: columns, primaryKeyColumnIDs: key))
        }
        let protectionCount = try count(from: &reader, minimumBytes: 13)
        var protections: [SQLCatalogProtectedContract] = []
        protections.reserveCapacity(protectionCount)
        for _ in 0..<protectionCount {
            let tag = try reader.readUInt8()
            guard let source = SQLCatalogProtectedContract.Source(rawValue: tag) else {
                throw StorageFrameError.invalidValueTag(tag)
            }
            protections.append(try SQLCatalogProtectedContract(relationID: reader.readString(),
                entityName: reader.readString(), entityDigest: reader.readBytes(), source: source))
        }
        let canonicalByteCount = bytes.count - reader.remainingCount
        let digest = try reader.readBytes()
        try reader.ensureFullyRead()
        let catalog = try SQLCatalog(namespaces: namespaces, relations: relations)
        var protectedIDs: Set<SQLCatalogName> = []
        let relationsByID = Dictionary(uniqueKeysWithValues: catalog.relations.map {
            (SQLCatalogName($0.id), $0)
        })
        for protection in protections {
            let id = SQLCatalogName(protection.relationID)
            guard protectedIDs.insert(id).inserted,
                protection.entityDigest.count == SHA256Accumulator.digestByteCount,
                let relation = relationsByID[id],
                SQLCatalogName(relation.entityName) == SQLCatalogName(protection.entityName) else {
                throw SQLCatalogBindingError.invalidProtection(protection.relationID)
            }
        }
        var accumulator = SHA256Accumulator()
        // Hash the retained validated source range, without re-encoding a copy.
        accumulator.update(bytes[bytes.startIndex..<(bytes.startIndex + canonicalByteCount)])
        guard digest == accumulator.finalize() else { throw SQLCatalogBindingError.digestMismatch }
        return SQLCatalogStoredState(catalog: catalog, physicalSchemaFingerprint: fingerprint,
            protectedContracts: protections, digest: digest)
    }

    private static func count(from reader: inout StorageFrameDecoder,
        minimumBytes: Int
    ) throws(StorageFrameError) -> Int {
        let count = try reader.readCount()
        guard count <= reader.remainingCount / minimumBytes else { throw .truncated }
        return count
    }

    private static func write(_ catalog: SQLCatalog, fingerprint: SchemaFingerprint,
        protectedContracts: [SQLCatalogProtectedContract], into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        try writeDeclaration(catalog, fingerprint: fingerprint, into: &writer)
        try writer.writeCount(protectedContracts.count)
        for protection in protectedContracts { try writeProtection(protection, into: &writer) }
    }

    private static func writeDeclaration(_ catalog: SQLCatalog, fingerprint: SchemaFingerprint,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        writer.writeUInt32(magic)
        writer.writeUInt16(version)
        try writer.writeBytes(fingerprint.bytes)
        try writer.writeCount(catalog.namespaces.count)
        for namespace in catalog.namespaces {
            try writer.writeString(namespace.id)
            try writer.writeString(namespace.name)
        }
        try writer.writeCount(catalog.relations.count)
        for relation in catalog.relations {
            try writer.writeString(relation.id)
            try writer.writeString(relation.namespaceID)
            try writer.writeString(relation.name)
            try writer.writeString(relation.entityName)
            try writer.writeCount(relation.columns.count)
            for column in relation.columns {
                try writer.writeString(column.id)
                try writer.writeString(column.name)
                guard let number = Int64(exactly: column.fieldNumber) else { throw .integerOutOfRange }
                writer.writeInt64(number)
            }
            writer.writeBool(relation.primaryKeyColumnIDs != nil)
            if let key = relation.primaryKeyColumnIDs {
                try writer.writeCount(key.count)
                for id in key { try writer.writeString(id) }
            }
        }
    }

    private static func writeProtection(_ protection: SQLCatalogProtectedContract,
        into writer: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        writer.writeUInt8(protection.source.rawValue)
        try writer.writeString(protection.relationID)
        try writer.writeString(protection.entityName)
        try writer.writeBytes(protection.entityDigest)
    }
}
