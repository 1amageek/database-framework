import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseGraphQueryPageCursor: Sendable, Hashable {
    enum Kind: UInt8, Sendable, Hashable {
        case construct = 1
        case describe = 2
    }

    private static let formatVersion: UInt8 = 1

    let kind: Kind
    let requestFingerprint: ByteString
    let snapshotVersion: Int64
    let resultFingerprint: ByteString
    let tripleOffset: UInt64

    func encode(limits: DatabaseWireLimits) throws -> ByteString {
        do {
            return try DatabaseWireWriter.encode(limits: limits) {
                (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                writer.writeUInt8(Self.formatVersion)
                writer.writeUInt8(kind.rawValue)
                try writer.writeBytes(requestFingerprint)
                writer.writeInt64(snapshotVersion)
                try writer.writeBytes(resultFingerprint)
                writer.writeUInt64(tripleOffset)
            }
        } catch {
            throw DatabaseGraphQueryError.invalidContinuation
        }
    }

    static func decode(
        _ bytes: ByteString,
        limits: DatabaseWireLimits
    ) throws -> Self {
        do {
            var reader = DatabaseWireReader(bytes, limits: limits)
            guard try reader.readUInt8() == Self.formatVersion,
                  let kind = Kind(rawValue: try reader.readUInt8()) else {
                throw DatabaseGraphQueryError.invalidContinuation
            }
            let requestFingerprint = try reader.readBytes()
            let snapshotVersion = try reader.readInt64()
            let resultFingerprint = try reader.readBytes()
            guard requestFingerprint.count == DatabaseRequestDigest.byteCount,
                  resultFingerprint.count == DatabaseRequestDigest.byteCount,
                  snapshotVersion >= 0 else {
                throw DatabaseGraphQueryError.invalidContinuation
            }
            let tripleOffset = try reader.readUInt64()
            guard tripleOffset > 0 else {
                throw DatabaseGraphQueryError.invalidContinuation
            }
            let result = Self(
                kind: kind,
                requestFingerprint: requestFingerprint,
                snapshotVersion: snapshotVersion,
                resultFingerprint: resultFingerprint,
                tripleOffset: tripleOffset
            )
            try reader.ensureFullyRead()
            return result
        } catch let error as DatabaseGraphQueryError {
            throw error
        } catch {
            throw DatabaseGraphQueryError.invalidContinuation
        }
    }
}
