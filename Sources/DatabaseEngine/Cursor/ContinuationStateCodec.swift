import DatabaseKit
import DatabaseTypes
import StorageKit

/// Bounded binary representation of a typed-query continuation.
internal enum ContinuationStateFormat {
    private static let magic: UInt32 = 0x4355_5231
    static let maximumByteCount = 128

    private static let limits: StorageFrameLimits = {
        do {
            return try StorageFrameLimits(
                maximumFrameBytes: maximumByteCount,
                maximumStringBytes: 0,
                maximumByteStringBytes: SHA256Accumulator.digestByteCount,
                maximumCollectionCount: 0,
                maximumNestingDepth: 0
            )
        } catch {
            preconditionFailure(
                "The continuation frame limits must be valid: \(error)"
            )
        }
    }()

    static func encode(
        _ state: ContinuationState
    ) throws -> ByteString {
        guard state.queryFingerprint.count
            == SHA256Accumulator.digestByteCount else {
            throw ContinuationError.corruptedToken
        }
        do {
            return try StorageFrameEncoder.encode(limits: limits) {
                (writer: inout StorageFrameEncoder) throws(
                    StorageFrameError
                ) in
                writer.writeUInt32(magic)
                writer.writeUInt8(state.version)
                writer.writeInt64(Int64(bitPattern: state.nextOffset))
                writer.writeBool(state.remainingLimit != nil)
                if let remainingLimit = state.remainingLimit {
                    writer.writeInt64(Int64(bitPattern: remainingLimit))
                }
                try writer.writeBytes(state.queryFingerprint)
            }
        } catch {
            throw ContinuationError.corruptedToken
        }
    }

    static func decode(
        _ bytes: ByteString
    ) throws -> ContinuationState {
        guard bytes.count <= maximumByteCount else {
            throw ContinuationError.tokenTooLarge(
                actual: bytes.count,
                maximum: maximumByteCount
            )
        }
        do {
            var reader = try StorageFrameDecoder(
                bytes,
                limits: limits
            )
            guard try reader.readUInt32() == magic else {
                throw ContinuationError.corruptedToken
            }
            let version = try reader.readUInt8()
            guard version == ContinuationToken.currentVersion else {
                throw ContinuationError.versionMismatch(
                    expected: ContinuationToken.currentVersion,
                    actual: version
                )
            }
            let nextOffset = UInt64(bitPattern: try reader.readInt64())
            let remainingLimit = try reader.readBool()
                ? UInt64(bitPattern: try reader.readInt64())
                : nil
            let fingerprint = try reader.readBytes()
            guard fingerprint.count == SHA256Accumulator.digestByteCount else {
                throw ContinuationError.corruptedToken
            }
            try reader.ensureFullyRead()
            return ContinuationState(
                version: version,
                nextOffset: nextOffset,
                remainingLimit: remainingLimit,
                queryFingerprint: fingerprint.detached()
            )
        } catch let error as ContinuationError {
            throw error
        } catch {
            throw ContinuationError.corruptedToken
        }
    }
}
