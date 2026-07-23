// Opaque continuation for typed database scans.
import DatabaseValue
import DatabaseWire
import StorageKit

/// Opaque, bounded cursor for continuing a scan in the same entity partition.
public struct DatabaseScanContinuation: Sendable, Hashable {
    private static let magic: UInt32 = 0x4452_4331
    private static let version: UInt8 = 1

    public let encodedBytes: DatabaseBytes

    package let entity: String
    package let partitionPath: [String]
    package let storageKey: Bytes

    public init(
        encodedBytes: DatabaseBytes,
        limits: DatabaseWireLimits = .default
    ) throws {
        var reader = DatabaseWireReader(encodedBytes, limits: limits)
        guard try reader.readUInt32() == Self.magic else {
            throw DatabaseScanContinuationError.invalidMagic
        }
        guard try reader.readUInt8() == Self.version else {
            throw DatabaseScanContinuationError.unsupportedVersion
        }
        let entity = try reader.readString()
        let partitionCount = try reader.readCount()
        var partitionPath: [String] = []
        partitionPath.reserveCapacity(partitionCount)
        for _ in 0..<partitionCount {
            partitionPath.append(try reader.readString())
        }
        let key = try reader.readBytes()
        try reader.ensureFullyRead()

        self.encodedBytes = encodedBytes
        self.entity = entity
        self.partitionPath = partitionPath
        self.storageKey = Bytes(retaining: key)
    }

    package init(
        entity: String,
        partitionPath: [String],
        storageKey: Bytes,
        limits: DatabaseWireLimits = .default
    ) throws {
        let encodedBytes = try DatabaseWireWriter.encodeThrowing(limits: limits) {
            writer in
            writer.writeUInt32(Self.magic)
            writer.writeUInt8(Self.version)
            try writer.writeString(entity)
            try writer.writeCount(partitionPath.count)
            for component in partitionPath {
                try writer.writeString(component)
            }
            try writer.writeBytes(DatabaseBytes(retaining: storageKey))
        }

        self.encodedBytes = encodedBytes
        self.entity = entity
        self.partitionPath = partitionPath
        self.storageKey = storageKey
    }

    public static func == (
        lhs: DatabaseScanContinuation,
        rhs: DatabaseScanContinuation
    ) -> Bool {
        lhs.encodedBytes == rhs.encodedBytes
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(encodedBytes)
    }
}

public enum DatabaseScanContinuationError: Error, Sendable, Equatable {
    case invalidMagic
    case unsupportedVersion
    case mismatchedEntity(expected: String, actual: String)
    case mismatchedPartition
    case keyOutsideEntityRange
}
