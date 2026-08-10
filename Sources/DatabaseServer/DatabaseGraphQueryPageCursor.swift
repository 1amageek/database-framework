#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseTypes
import DatabaseKit
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseGraphQueryPageCursor: Sendable, Hashable {
    enum Kind: UInt8, Sendable, Hashable {
        case construct = 1
        case describe = 2
    }

    private static let formatVersion: UInt8 = 3

    let kind: Kind
    let baseID: Base.ID
    let placementGeneration: UInt64
    let requestFingerprint: ByteString
    let readPosition: DomainReadPoint.Position
    let resultFingerprint: ByteString
    let tripleOffset: UInt64

    func encode(limits: DatabaseWireLimits) throws -> ByteString {
        do {
            return try DatabaseWireWriter.encode(limits: limits) {
                (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                writer.writeUInt8(Self.formatVersion)
                writer.writeUInt8(kind.rawValue)
                try writer.writeString(baseID.value)
                writer.writeUInt64(placementGeneration)
                try writer.writeBytes(requestFingerprint)
                switch readPosition {
                case .version(let version):
                    writer.writeUInt8(1)
                    writer.writeUInt64(version)
                case .opaque(let identifier):
                    writer.writeUInt8(2)
                    try writer.writeBytes(identifier)
                }
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
            let baseID = try Base.ID(reader.readString())
            let placementGeneration = try reader.readUInt64()
            let requestFingerprint = try reader.readBytes()
            let readPosition: DomainReadPoint.Position
            switch try reader.readUInt8() {
            case 1:
                readPosition = .version(try reader.readUInt64())
            case 2:
                let identifier = try reader.readBytes()
                guard !identifier.isEmpty else {
                    throw DatabaseGraphQueryError.invalidContinuation
                }
                readPosition = .opaque(identifier)
            default:
                throw DatabaseGraphQueryError.invalidContinuation
            }
            let resultFingerprint = try reader.readBytes()
            guard requestFingerprint.count == DatabaseRequestDigest.byteCount,
                  resultFingerprint.count == DatabaseRequestDigest.byteCount else {
                throw DatabaseGraphQueryError.invalidContinuation
            }
            let tripleOffset = try reader.readUInt64()
            guard tripleOffset > 0 else {
                throw DatabaseGraphQueryError.invalidContinuation
            }
            let result = Self(
                kind: kind,
                baseID: baseID,
                placementGeneration: placementGeneration,
                requestFingerprint: requestFingerprint,
                readPosition: readPosition,
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

#endif
