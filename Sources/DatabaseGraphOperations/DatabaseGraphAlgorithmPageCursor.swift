import DatabaseMutationOperations
import DatabaseQueryOperations
import DatabaseOperationCore
#if DATABASE_GRAPH_OPERATIONS_ENABLED
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabaseGraphAlgorithmPageCursor: Sendable, Hashable {
    package enum Kind: UInt8, Sendable, Hashable {
        case path = 1
        case ranking = 2
        case communities = 3
        case cycles = 4
        case components = 5
        case topologicalOrder = 6
    }

    private static let formatVersion: UInt8 = 1

    package let kind: Kind
    package let requestFingerprint: ByteString
    package let resultFingerprint: ByteString
    package let offset: UInt64

    package init(
        kind: Kind,
        requestFingerprint: ByteString,
        resultFingerprint: ByteString,
        offset: UInt64
    ) {
        self.kind = kind
        self.requestFingerprint = requestFingerprint
        self.resultFingerprint = resultFingerprint
        self.offset = offset
    }

    package func encode(limits: DatabaseWireLimits) throws -> ByteString {
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

    package static func decode(
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

#endif
