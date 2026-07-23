import DatabaseEngine
import DatabaseValue
import DatabaseWire
import StorageKit

public struct DatabaseRDFDocumentStore: Sendable {
    private static let metadataFormatVersion: UInt16 = 1

    private let root: Subspace
    private let wireLimits: DatabaseWireLimits

    public init(
        container: DBContainer,
        namespace: String,
        wireLimits: DatabaseWireLimits = .default
    ) async throws {
        guard !namespace.isEmpty else {
            throw DatabaseRDFDocumentStoreError.emptyIdentifier
        }
        self.root = try await container.engine.createOrOpenDirectory(
            path: ["database-framework", "rdf-documents", namespace]
        )
        self.wireLimits = wireLimits
    }

    public func page(
        identifier: String,
        offset: Int,
        limit: Int,
        transaction: any Transaction
    ) async throws -> DatabaseRDFStoredDocumentPage? {
        try validate(identifier: identifier)
        guard offset >= 0, limit > 0 else {
            throw DatabaseRDFDocumentStoreError.invalidPage(
                offset: offset,
                limit: limit
            )
        }
        guard let metadata = try await metadata(
            identifier: identifier,
            transaction: transaction
        ), !metadata.isDeleted else {
            return nil
        }
        guard Int64(exactly: offset) != nil,
              metadata.quadCount <= UInt64(Int.max),
              metadata.auxiliaryCount <= UInt64(Int.max) else {
            throw DatabaseRDFDocumentStoreError.corruptedMetadata(identifier)
        }

        let auxiliaryIdentifiers = try await readAuxiliaryIdentifiers(
            identifier: identifier,
            count: Int(metadata.auxiliaryCount),
            transaction: transaction
        )
        let quads = try await readQuads(
            identifier: identifier,
            offset: offset,
            limit: limit,
            totalCount: Int(metadata.quadCount),
            transaction: transaction
        )
        let consumed = offset + quads.count
        let nextOffset = consumed < Int(metadata.quadCount)
            ? UInt64(consumed)
            : nil
        return DatabaseRDFStoredDocumentPage(
            identifier: identifier,
            revision: metadata.revision,
            auxiliaryIdentifiers: auxiliaryIdentifiers,
            quads: quads,
            totalQuadCount: metadata.quadCount,
            nextOffset: nextOffset
        )
    }

    public func replace(
        identifier: String,
        auxiliaryIdentifiers: [String],
        quads: [DatabaseRDFQuad],
        expectedRevision: UInt64?,
        transaction: any Transaction
    ) async throws -> UInt64 {
        try validate(identifier: identifier)
        let current = try await metadata(
            identifier: identifier,
            transaction: transaction
        )
        try validate(
            expectedRevision: expectedRevision,
            actualRevision: current?.revision ?? 0
        )
        let revision = try nextRevision(
            current?.revision ?? 0,
            identifier: identifier
        )
        let canonicalQuads = try canonicalize(quads)
        let canonicalAuxiliary = Array(Set(auxiliaryIdentifiers)).sorted()
        let document = documentSubspace(identifier)
        try clear(document.subspace("auxiliary"), transaction: transaction)
        try clear(document.subspace("quads"), transaction: transaction)

        for (index, auxiliary) in canonicalAuxiliary.enumerated() {
            try transaction.setValue(
                Bytes(auxiliary.utf8),
                for: document.subspace("auxiliary").pack(Tuple(Int64(index)))
            )
        }
        for (index, encoded) in canonicalQuads.enumerated() {
            try transaction.setValue(
                Bytes(retaining: encoded),
                for: document.subspace("quads").pack(Tuple(Int64(index)))
            )
        }
        let metadata = Metadata(
            identifier: identifier,
            revision: revision,
            isDeleted: false,
            auxiliaryCount: UInt64(canonicalAuxiliary.count),
            quadCount: UInt64(canonicalQuads.count)
        )
        try transaction.setValue(
            Bytes(retaining: try encode(metadata)),
            for: metadataKey(identifier)
        )
        return revision
    }

    public func delete(
        identifier: String,
        expectedRevision: UInt64?,
        transaction: any Transaction
    ) async throws -> UInt64 {
        try validate(identifier: identifier)
        guard let current = try await metadata(
            identifier: identifier,
            transaction: transaction
        ), !current.isDeleted else {
            throw DatabaseRDFDocumentStoreError.documentNotFound(identifier)
        }
        try validate(
            expectedRevision: expectedRevision,
            actualRevision: current.revision
        )
        let revision = try nextRevision(
            current.revision,
            identifier: identifier
        )
        let document = documentSubspace(identifier)
        try clear(document.subspace("auxiliary"), transaction: transaction)
        try clear(document.subspace("quads"), transaction: transaction)
        try transaction.setValue(
            Bytes(retaining: try encode(
                Metadata(
                    identifier: identifier,
                    revision: revision,
                    isDeleted: true,
                    auxiliaryCount: 0,
                    quadCount: 0
                )
            )),
            for: metadataKey(identifier)
        )
        return revision
    }

    private func metadata(
        identifier: String,
        transaction: any Transaction
    ) async throws -> Metadata? {
        guard let bytes = try await transaction.getValue(
            for: metadataKey(identifier)
        ) else {
            return nil
        }
        var reader = DatabaseWireReader(
            DatabaseBytes(retaining: bytes),
            limits: wireLimits
        )
        guard try reader.readUInt16() == Self.metadataFormatVersion else {
            throw DatabaseRDFDocumentStoreError.corruptedMetadata(identifier)
        }
        let metadata = Metadata(
            identifier: try reader.readString(),
            revision: try reader.readUInt64(),
            isDeleted: try reader.readBool(),
            auxiliaryCount: try reader.readUInt64(),
            quadCount: try reader.readUInt64()
        )
        try reader.ensureFullyRead()
        guard metadata.identifier == identifier else {
            throw DatabaseRDFDocumentStoreError.corruptedMetadata(identifier)
        }
        return metadata
    }

    private func readAuxiliaryIdentifiers(
        identifier: String,
        count: Int,
        transaction: any Transaction
    ) async throws -> [String] {
        guard count > 0 else { return [] }
        let range = documentSubspace(identifier).subspace("auxiliary").range()
        var values: [String] = []
        values.reserveCapacity(count)
        let rows = try await transaction.collectRange(
            begin: range.begin,
            end: range.end,
            limit: count
        )
        for (_, bytes) in rows {
            guard let value = String(validating: bytes, as: UTF8.self) else {
                throw DatabaseRDFDocumentStoreError.corruptedMetadata(identifier)
            }
            values.append(value)
        }
        guard values.count == count else {
            throw DatabaseRDFDocumentStoreError.corruptedItemCount(
                expected: count,
                actual: values.count
            )
        }
        return values
    }

    private func readQuads(
        identifier: String,
        offset: Int,
        limit: Int,
        totalCount: Int,
        transaction: any Transaction
    ) async throws -> [DatabaseRDFQuad] {
        guard offset < totalCount else { return [] }
        guard let encodedOffset = Int64(exactly: offset) else {
            throw DatabaseRDFDocumentStoreError.invalidPage(
                offset: offset,
                limit: limit
            )
        }
        let quads = documentSubspace(identifier).subspace("quads")
        let end = quads.range().end
        var values: [DatabaseRDFQuad] = []
        values.reserveCapacity(min(limit, totalCount - offset))
        let rows = try await transaction.collectRange(
            begin: quads.pack(Tuple(encodedOffset)),
            end: end,
            limit: min(limit, totalCount - offset)
        )
        for (_, bytes) in rows {
            values.append(
                try DatabaseEnvelopeCodec.decode(
                    DatabaseRDFQuad.self,
                    from: DatabaseBytes(retaining: bytes),
                    limits: wireLimits
                )
            )
        }
        let expected = min(limit, totalCount - offset)
        guard values.count == expected else {
            throw DatabaseRDFDocumentStoreError.corruptedItemCount(
                expected: expected,
                actual: values.count
            )
        }
        return values
    }

    private func canonicalize(
        _ quads: [DatabaseRDFQuad]
    ) throws -> [DatabaseBytes] {
        var unique = Set<DatabaseBytes>()
        unique.reserveCapacity(quads.count)
        for quad in quads {
            unique.insert(
                try DatabaseEnvelopeCodec.encode(quad, limits: wireLimits)
            )
        }
        return unique.sorted { left, right in
            left.lexicographicallyPrecedes(right)
        }
    }

    private func validate(identifier: String) throws {
        guard !identifier.isEmpty else {
            throw DatabaseRDFDocumentStoreError.emptyIdentifier
        }
    }

    private func validate(
        expectedRevision: UInt64?,
        actualRevision: UInt64
    ) throws {
        guard let expectedRevision else { return }
        guard expectedRevision == actualRevision else {
            throw DatabaseRDFDocumentStoreError.revisionConflict(
                expected: expectedRevision,
                actual: actualRevision
            )
        }
    }

    private func nextRevision(
        _ current: UInt64,
        identifier: String
    ) throws -> UInt64 {
        guard current < UInt64.max else {
            throw DatabaseRDFDocumentStoreError.revisionOverflow(identifier)
        }
        return current + 1
    }

    private func encode(_ metadata: Metadata) throws -> DatabaseBytes {
        try DatabaseWireWriter.encode(limits: wireLimits) {
            (writer: inout DatabaseWireWriter) throws(DatabaseWireError) in
            writer.writeUInt16(Self.metadataFormatVersion)
            try writer.writeString(metadata.identifier)
            writer.writeUInt64(metadata.revision)
            writer.writeBool(metadata.isDeleted)
            writer.writeUInt64(metadata.auxiliaryCount)
            writer.writeUInt64(metadata.quadCount)
        }
    }

    private func documentSubspace(_ identifier: String) -> Subspace {
        root.subspace(identifier)
    }

    private func metadataKey(_ identifier: String) -> Bytes {
        documentSubspace(identifier).pack(Tuple("metadata"))
    }

    private func clear(
        _ subspace: Subspace,
        transaction: any Transaction
    ) throws {
        let range = subspace.range()
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)
    }

    private struct Metadata: Sendable {
        let identifier: String
        let revision: UInt64
        let isDeleted: Bool
        let auxiliaryCount: UInt64
        let quadCount: UInt64
    }
}
