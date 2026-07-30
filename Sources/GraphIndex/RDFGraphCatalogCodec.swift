import DatabaseTypes
import DatabaseEngine
import DatabaseKit
import StorageKit

/// Physical codec for persistent named-graph identity. The graph term is
/// streamed directly into the final tuple key allocation.
package struct RDFGraphCatalogCodec: Sendable {
    package static let marker: ByteString = [1]

    private let subspace: Subspace

    package init(subspace: Subspace) {
        self.subspace = subspace
    }

    package var range: (begin: ByteString, end: ByteString) {
        subspace.range()
    }

    package func key(
        for graph: RDFGraphName
    ) throws -> ByteString {
        let plan: RDFTermStorageEncoding
        do throws(RDFTermStorageError) {
            plan = try RDFTermStorageFormat.encodingPlan(
                graph.term,
                role: .graphName
            )
        } catch let error {
            throw RDFGraphStoreError.invalidTermEncoding(error)
        }

        let (tupleByteCount, overflow) = plan.byteCount.addingReportingOverflow(2)
        guard !overflow else {
            throw RDFGraphStoreError.keyTooLarge(
                actual: Int.max,
                maximum: databaseMaximumKeySize
            )
        }
        let (keyByteCount, keyOverflow) = subspace.prefix.count
            .addingReportingOverflow(tupleByteCount)
        guard !keyOverflow, keyByteCount <= databaseMaximumKeySize else {
            throw RDFGraphStoreError.keyTooLarge(
                actual: keyOverflow ? Int.max : keyByteCount,
                maximum: databaseMaximumKeySize
            )
        }
        let key: ByteString
        do throws(RDFTermStorageError) {
            key = try subspace.pack(
                encodedTupleByteCount: tupleByteCount
            ) { (sink: inout TupleEncodingSink) throws(RDFTermStorageError) in
                sink.writeByte(TupleTypeCode.bytes.rawValue)
                var rdfSink = RDFSink(tupleSink: sink)
                try RDFTermStorageFormat.encode(plan, into: &rdfSink)
                sink = rdfSink.tupleSink
                sink.writeByte(0)
            }
        } catch let error {
            throw RDFGraphStoreError.invalidTermEncoding(error)
        }
        guard key.count <= databaseMaximumKeySize else {
            throw RDFGraphStoreError.keyTooLarge(
                actual: key.count,
                maximum: databaseMaximumKeySize
            )
        }
        assert(key.count == keyByteCount)
        return key
    }

    package func decodeGraph(
        from key: ByteString
    ) throws -> RDFGraphName {
        var cursor: TupleCursor
        do {
            cursor = try subspace.tupleCursor(for: key)
        } catch {
            throw RDFGraphStoreError.catalogPrefixMismatch
        }

        let bytes: ByteString
        do {
            bytes = try cursor.requireBytes()
        } catch TupleError.unexpectedEndOfData {
            throw RDFGraphStoreError.catalogTruncatedKey
        } catch TupleError.invalidTypeCode(let typeCode) {
            throw RDFGraphStoreError.catalogUnexpectedTupleType(typeCode)
        } catch {
            throw RDFGraphStoreError.catalogTruncatedKey
        }
        guard cursor.isAtEnd else {
            throw RDFGraphStoreError.catalogTrailingTupleData(
                offset: cursor.consumedByteCount
            )
        }

        let term: RDFTerm
        do {
            term = try RDFTermStorageFormat.decode(
                bytes,
                role: .graphName
            )
        } catch let error {
            throw RDFGraphStoreError.invalidCatalogGraph(error)
        }
        do {
            return try RDFGraphName(term)
        } catch let error {
            throw RDFGraphStoreError.invalidCatalogGraphName(error)
        }
    }

    package func validateMarker(
        _ value: ByteString
    ) throws {
        guard value == Self.marker else {
            throw RDFGraphStoreError.invalidCatalogMarker
        }
    }

    private struct RDFSink: RDFTermStorageSink {
        var tupleSink: TupleEncodingSink

        mutating func write(_ byte: UInt8) {
            tupleSink.writeByte(byte)
        }

        mutating func write(_ bytes: UnsafeRawBufferPointer) {
            tupleSink.writeBytes(bytes)
        }
    }
}
