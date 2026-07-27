import DatabaseTypes
#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit

/// Canonical bounded persistence codec for uniqueness violation entries.
enum UniquenessViolationCodec {
    private static let magic: [UInt8] = [0x55, 0x56, 0x49, 0x4F]
    private static let version: UInt16 = 1

    static func encode(
        _ violation: UniquenessViolation,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> Bytes {
        let encoded = try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            for byte in magic {
                writer.writeUInt8(byte)
            }
            writer.writeUInt16(version)
            try writer.writeString(violation.indexName)
            try writer.writeString(violation.persistableType)
            try writer.writeBytes(ByteString(retaining: violation.valueKey))
            try writer.writeCount(violation.primaryKeys.count)
            for primaryKey in violation.primaryKeys {
                try writer.writeBytes(ByteString(retaining: primaryKey))
            }
            writer.writeDouble(violation.detectedAt.timeIntervalSince1970)
        }
        return Bytes(retaining: encoded)
    }

    static func decode(
        _ bytes: Bytes,
        limits: StorageFrameLimits = .default
    ) throws -> UniquenessViolation {
        var reader = try StorageFrameDecoder(
            ByteString(retaining: bytes),
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
        let valueKey = Bytes(retaining: try reader.readBytes())
        let count = try reader.readCount()
        var primaryKeys: [Bytes] = []
        primaryKeys.reserveCapacity(count)
        for _ in 0..<count {
            primaryKeys.append(Bytes(retaining: try reader.readBytes()))
        }
        let detectedInterval = try reader.readDouble()
        try reader.ensureFullyRead()
        guard detectedInterval.isFinite else {
            throw StorageFrameError.invalidTimestamp
        }
        return UniquenessViolation(
            indexName: indexName,
            persistableType: persistableType,
            valueKey: valueKey,
            primaryKeys: primaryKeys,
            detectedAt: Date(timeIntervalSince1970: detectedInterval)
        )
    }
}
