@_spi(DatabaseExecution) import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

public struct DatabaseRDFDocumentStore: Sendable {
    // Version 2 is the framework-owned StorageFrame metadata representation.
    private static let metadataFormatVersion: UInt16 = 2

    private let container: DBContainer
    private let namespace: String

    public init(
        container: DBContainer,
        namespace: String
    ) async throws {
        guard !namespace.isEmpty else {
            throw DatabaseRDFDocumentStoreError.emptyIdentifier
        }
        self.container = container
        self.namespace = namespace
    }

    public func page(
        identifier: String,
        offset: Int,
        limit: Int,
        transaction: any TransactionAccess
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
        quads: [RDFQuad],
        expectedRevision: UInt64?,
        transaction: any TransactionAccess
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
        let canonicalAuxiliary = canonicalize(auxiliaryIdentifiers)
        let document = try documentSubspace(identifier)
        try clear(document.subspace("auxiliary"), transaction: transaction)
        try clear(document.subspace("quads"), transaction: transaction)

        for (index, auxiliary) in canonicalAuxiliary.enumerated() {
            try transaction.setValue(
                ByteString(utf8: auxiliary),
                for: document.subspace("auxiliary").pack(Tuple(Int64(index)))
            )
        }
        for (index, encoded) in canonicalQuads.enumerated() {
            try transaction.setValue(
                encoded,
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
            try encode(metadata),
            for: try metadataKey(identifier)
        )
        return revision
    }

    public func delete(
        identifier: String,
        expectedRevision: UInt64?,
        transaction: any TransactionAccess
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
        let document = try documentSubspace(identifier)
        try clear(document.subspace("auxiliary"), transaction: transaction)
        try clear(document.subspace("quads"), transaction: transaction)
        try transaction.setValue(
            try encode(
                Metadata(
                    identifier: identifier,
                    revision: revision,
                    isDeleted: true,
                    auxiliaryCount: 0,
                    quadCount: 0
                )
            ),
            for: try metadataKey(identifier)
        )
        return revision
    }

    private func metadata(
        identifier: String,
        transaction: any TransactionAccess
    ) async throws -> Metadata? {
        guard let bytes = try await transaction.getValue(
            for: try metadataKey(identifier),
            snapshot: false
        ) else {
            return nil
        }
        let metadata: Metadata
        do {
            var decoder = try StorageFrameDecoder(bytes)
            guard try decoder.readUInt16() == Self.metadataFormatVersion else {
                throw DatabaseRDFDocumentStoreError.corruptedMetadata(
                    identifier
                )
            }
            metadata = Metadata(
                identifier: try decoder.readString(),
                revision: try decoder.readUInt64(),
                isDeleted: try decoder.readBool(),
                auxiliaryCount: try decoder.readUInt64(),
                quadCount: try decoder.readUInt64()
            )
            try decoder.ensureFullyRead()
        } catch let error as DatabaseRDFDocumentStoreError {
            throw error
        } catch {
            throw DatabaseRDFDocumentStoreError.corruptedMetadata(identifier)
        }
        guard metadata.identifier == identifier else {
            throw DatabaseRDFDocumentStoreError.corruptedMetadata(identifier)
        }
        return metadata
    }

    private func readAuxiliaryIdentifiers(
        identifier: String,
        count: Int,
        transaction: any TransactionAccess
    ) async throws -> [String] {
        guard count > 0 else { return [] }
        let range = try documentSubspace(identifier)
            .subspace("auxiliary").range()
        var values: [String] = []
        values.reserveCapacity(count)
        let rows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: count,
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
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
        transaction: any TransactionAccess
    ) async throws -> [RDFQuad] {
        guard offset < totalCount else { return [] }
        guard let encodedOffset = Int64(exactly: offset) else {
            throw DatabaseRDFDocumentStoreError.invalidPage(
                offset: offset,
                limit: limit
            )
        }
        let quads = try documentSubspace(identifier).subspace("quads")
        let end = quads.range().end
        var values: [RDFQuad] = []
        values.reserveCapacity(min(limit, totalCount - offset))
        let rows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(quads.pack(Tuple(encodedOffset))),
            to: .firstGreaterOrEqual(end),
            limit: min(limit, totalCount - offset),
            reverse: false,
            snapshot: false,
            streamingMode: .wantAll
        )
        for (_, bytes) in rows {
            values.append(try decodeQuad(bytes, identifier: identifier))
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
        _ quads: [RDFQuad]
    ) throws -> [ByteString] {
        var encoded: [ByteString] = []
        encoded.reserveCapacity(quads.count)
        var inputIndex = 0
        while inputIndex < quads.count {
            encoded.append(
                try encodeQuad(quads[inputIndex])
            )
            inputIndex += 1
        }
        encoded.sort { left, right in
            left.lexicographicallyPrecedes(right)
        }

        var canonical: [ByteString] = []
        canonical.reserveCapacity(encoded.count)
        var encodedIndex = 0
        while encodedIndex < encoded.count {
            let value = encoded[encodedIndex]
            if canonical.last != value {
                canonical.append(value)
            }
            encodedIndex += 1
        }
        return canonical
    }

    private func canonicalize(_ values: [String]) -> [String] {
        var sorted = values
        sorted.sort()

        var canonical: [String] = []
        canonical.reserveCapacity(sorted.count)
        var index = 0
        while index < sorted.count {
            let value = sorted[index]
            if canonical.last != value {
                canonical.append(value)
            }
            index += 1
        }
        return canonical
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

    private func encode(_ metadata: Metadata) throws -> ByteString {
        do {
            return try StorageFrameEncoder.encode {
                (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
                encoder.writeUInt16(Self.metadataFormatVersion)
                try encoder.writeString(metadata.identifier)
                encoder.writeUInt64(metadata.revision)
                encoder.writeBool(metadata.isDeleted)
                encoder.writeUInt64(metadata.auxiliaryCount)
                encoder.writeUInt64(metadata.quadCount)
            }
        } catch {
            throw DatabaseRDFDocumentStoreError.corruptedMetadata(
                metadata.identifier
            )
        }
    }

    private func encodeQuad(_ quad: RDFQuad) throws -> ByteString {
        do {
            return try StorageFrameEncoder.encode {
                (encoder: inout StorageFrameEncoder) throws(StorageFrameError) in
                try encoder.writeRDFTerm(quad.subject.term)
                try encoder.writeRDFTerm(quad.predicate.term)
                try encoder.writeRDFTerm(quad.object)
                encoder.writeBool(quad.graph != nil)
                if let graph = quad.graph {
                    try encoder.writeRDFTerm(graph.term)
                }
            }
        } catch {
            throw DatabaseRDFDocumentStoreError.invalidQuad
        }
    }

    private func decodeQuad(
        _ bytes: ByteString,
        identifier: String
    ) throws -> RDFQuad {
        do {
            var decoder = try StorageFrameDecoder(bytes)
            let subject = try decoder.readRDFTerm()
            let predicate = try decoder.readRDFTerm()
            let object = try decoder.readRDFTerm()
            let graph = try decoder.readBool()
                ? try decoder.readRDFTerm()
                : nil
            try decoder.ensureFullyRead()
            return try RDFQuad(
                validatingSubject: subject,
                predicate: predicate,
                object: object,
                graph: graph
            )
        } catch {
            throw DatabaseRDFDocumentStoreError.corruptedMetadata(identifier)
        }
    }

    private func documentSubspace(_ identifier: String) throws -> Subspace {
        try container.operationDataSubspace(
            relativePath: [
                "database-framework",
                "rdf-documents",
                namespace,
                identifier,
            ]
        )
    }

    private func metadataKey(_ identifier: String) throws -> ByteString {
        try documentSubspace(identifier).pack(Tuple("metadata"))
    }

    private func clear(
        _ subspace: Subspace,
        transaction: any TransactionAccess
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
