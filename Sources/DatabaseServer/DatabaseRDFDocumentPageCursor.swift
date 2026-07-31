#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

struct DatabaseRDFDocumentPageCursor: Sendable, Hashable {
    private static let formatVersion: UInt16 = 1

    enum Domain: UInt8, Sendable, Hashable {
        case ontology = 1
        case shacl = 2
    }

    let domain: Domain
    let identifier: String
    let revision: UInt64
    let offset: UInt64

    func encode(limits: DatabaseWireLimits) throws -> ByteString {
        do {
            return try DatabaseWireWriter.encode(limits: limits) {
                (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
                writer.writeUInt16(Self.formatVersion)
                writer.writeUInt8(domain.rawValue)
                try writer.writeString(identifier)
                writer.writeUInt64(revision)
                writer.writeUInt64(offset)
            }
        } catch {
            throw DatabaseRDFDocumentStoreError.invalidContinuation
        }
    }

    static func decode(
        _ bytes: ByteString,
        domain: Domain,
        identifier: String,
        limits: DatabaseWireLimits
    ) throws -> Self {
        do {
            var reader = DatabaseWireReader(bytes, limits: limits)
            guard try reader.readUInt16() == formatVersion,
                  let decodedDomain = Domain(rawValue: try reader.readUInt8()),
                  decodedDomain == domain,
                  try reader.readString() == identifier else {
                throw DatabaseRDFDocumentStoreError.invalidContinuation
            }
            let cursor = Self(
                domain: decodedDomain,
                identifier: identifier,
                revision: try reader.readUInt64(),
                offset: try reader.readUInt64()
            )
            try reader.ensureFullyRead()
            return cursor
        } catch let error as DatabaseRDFDocumentStoreError {
            throw error
        } catch {
            throw DatabaseRDFDocumentStoreError.invalidContinuation
        }
    }
}

#endif
