import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseGraphAlgorithmPageCursor: Sendable, Hashable {
    enum Kind: UInt8, Sendable, Hashable {
        case path = 1
        case ranking = 2
        case communities = 3
        case cycles = 4
        case components = 5
        case topologicalOrder = 6
    }

    private static let formatVersion: UInt8 = 1

    let kind: Kind
    let requestFingerprint: ByteString
    let resultFingerprint: ByteString
    let offset: UInt64

    func encode(limits: DatabaseWireLimits) throws -> ByteString {
        do {
            return try DatabaseWireWriter.encode(limits: limits) {
                (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                writer.writeUInt8(Self.formatVersion)
                writer.writeUInt8(kind.rawValue)
                try writer.writeBytes(requestFingerprint)
                try writer.writeBytes(resultFingerprint)
                writer.writeUInt64(offset)
            }
        } catch {
            throw DatabaseGraphAlgorithmError.invalidContinuation
        }
    }

    static func decode(
        _ bytes: ByteString,
        limits: DatabaseWireLimits
    ) throws -> Self {
        do {
            var reader = DatabaseWireReader(bytes, limits: limits)
            guard try reader.readUInt8() == formatVersion,
                  let kind = Kind(rawValue: try reader.readUInt8()) else {
                throw DatabaseGraphAlgorithmError.invalidContinuation
            }
            let requestFingerprint = try reader.readBytes()
            let resultFingerprint = try reader.readBytes()
            guard requestFingerprint.count == DatabaseRequestDigest.byteCount,
                  resultFingerprint.count == DatabaseRequestDigest.byteCount else {
                throw DatabaseGraphAlgorithmError.invalidContinuation
            }
            let cursor = Self(
                kind: kind,
                requestFingerprint: requestFingerprint,
                resultFingerprint: resultFingerprint,
                offset: try reader.readUInt64()
            )
            try reader.ensureFullyRead()
            return cursor
        } catch let error as DatabaseGraphAlgorithmError {
            throw error
        } catch {
            throw DatabaseGraphAlgorithmError.invalidContinuation
        }
    }
}
