import DatabaseTypes
import StorageKit

/// Canonical bounded persistence codec for uniqueness violation entries.
enum UniquenessViolationCodec {
    private static let magic: [UInt8] = [0x55, 0x56, 0x49, 0x4F]
    private static let version: UInt16 = 2

    static func encode(
        _ violation: UniquenessViolation,
        limits: StorageFrameLimits = .default
    ) throws -> ByteString {
        let conflictingValueBytes = Tuple(
            try FieldValue.toTupleElements(violation.conflictingValues)
        ).pack()
        let encoded = try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            for byte in magic {
                writer.writeUInt8(byte)
            }
            writer.writeUInt16(version)
            try writer.writeString(violation.indexName)
            try writer.writeString(violation.persistableType)
            try writer.writeBytes(violation.valueKey)
            try writer.writeBytes(conflictingValueBytes)
            try writer.writeCount(violation.primaryKeys.count)
            for primaryKey in violation.primaryKeys {
                try writer.writeBytes(primaryKey)
            }
            writer.writeInt64(violation.detectedAt.secondsSinceUnixEpoch)
            writer.writeUInt32(violation.detectedAt.nanoseconds)
        }
        return encoded
    }

    static func decode(
        _ bytes: ByteString,
        limits: StorageFrameLimits = .default
    ) throws -> UniquenessViolation {
        var reader = try StorageFrameDecoder(
            bytes,
            limits: limits
        )
        for byte in magic {
            guard try reader.readUInt8() == byte else {
                throw StorageFrameError.invalidMagic
            }
        }
        let decodedVersion = try reader.readUInt16()
        guard decodedVersion == version else {
            throw StorageFrameError.unsupportedVersion(decodedVersion)
        }
        let indexName = try reader.readString()
        let persistableType = try reader.readString()
        let valueKey = try reader.readBytes()
        let conflictingValueBytes = try reader.readBytes()
        let conflictingValues = try Tuple.unpack(
            from: conflictingValueBytes
        ).map { try FieldValue(tupleElement: $0) }
        let count = try reader.readCount()
        var primaryKeys: [ByteString] = []
        primaryKeys.reserveCapacity(count)
        for _ in 0..<count {
            primaryKeys.append(try reader.readBytes())
        }
        let detectedSeconds = try reader.readInt64()
        let detectedNanoseconds = try reader.readUInt32()
        try reader.ensureFullyRead()
        let detectedAt: Timestamp
        do {
            detectedAt = try Timestamp(
                secondsSinceUnixEpoch: detectedSeconds,
                nanoseconds: detectedNanoseconds
            )
        } catch {
            throw StorageFrameError.invalidTimestamp
        }
        return UniquenessViolation(
            indexName: indexName,
            persistableType: persistableType,
            valueKey: valueKey,
            conflictingValues: conflictingValues,
            primaryKeys: primaryKeys,
            detectedAt: detectedAt
        )
    }
}
