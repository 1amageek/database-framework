import DatabaseOperationCore
#if DATABASE_QUERY_OPERATIONS_GRAPH_INDEXES
import DatabaseTypes
@_spi(DatabaseOperations) import DatabaseWire

package struct DatabaseRDFDocumentPageCursor: Sendable, Hashable {
    private static let formatVersion: UInt16 = 1

    package enum Domain: UInt8, Sendable, Hashable {
        case ontology = 1
        case shacl = 2
    }

    package let domain: Domain
    package let identifier: String
    package let revision: UInt64
    package let offset: UInt64

    package init(
        domain: Domain,
        identifier: String,
        revision: UInt64,
        offset: UInt64
    ) {
        self.domain = domain
        self.identifier = identifier
        self.revision = revision
        self.offset = offset
    }

    package func encode(limits: DatabaseWireLimits) throws -> ByteString {
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

    package static func decode(
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
