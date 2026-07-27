import DatabaseTypes
import StorageKit

/// Canonical bounded persistence codec for resumable range progress.
public enum RangeSetCodec {
    private static let magic: [UInt8] = [0x52, 0x53, 0x45, 0x54]
    private static let version: UInt16 = 1

    public static func encode(
        _ rangeSet: RangeSet,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> Bytes {
        let encoded = try StorageFrameEncoder.encode(limits: limits) {
            (writer: inout StorageFrameEncoder) throws(StorageFrameError) in
            for byte in magic {
                writer.writeUInt8(byte)
            }
            writer.writeUInt16(version)
            let continuations = rangeSet.persistedContinuations
            try writer.writeCount(continuations.count)
            for continuation in continuations {
                try writer.writeBytes(ByteString(retaining: continuation.rangeBegin))
                try writer.writeBytes(ByteString(retaining: continuation.rangeEnd))
                try writer.writeOptionalBytes(
                    continuation.lastProcessedKey.map(ByteString.init(retaining:))
                )
                writer.writeBool(continuation.isComplete)
            }
        }
        return Bytes(retaining: encoded)
    }

    public static func decode(
        _ bytes: Bytes,
        limits: StorageFrameLimits = .default
    ) throws(StorageFrameError) -> RangeSet {
        var reader = try StorageFrameDecoder(
            ByteString(retaining: bytes),
            limits: limits
        )
        for byte in magic {
            guard try reader.readUInt8() == byte else {
                throw .invalidMagic
            }
        }
        let decodedVersion = try reader.readUInt16()
        guard decodedVersion == version else {
            throw .unsupportedVersion(decodedVersion)
        }

        let count = try reader.readCount()
        var continuations: [RangeContinuation] = []
        continuations.reserveCapacity(count)
        for _ in 0..<count {
            let begin = Bytes(retaining: try reader.readBytes())
            let end = Bytes(retaining: try reader.readBytes())
            let lastProcessedKey = try reader.readOptionalBytes().map(Bytes.init(retaining:))
            var continuation = RangeContinuation(begin: begin, end: end)
            continuation.lastProcessedKey = lastProcessedKey
            continuation.isComplete = try reader.readBool()
            continuations.append(continuation)
        }
        try reader.ensureFullyRead()
        return RangeSet(continuations: continuations)
    }
}
