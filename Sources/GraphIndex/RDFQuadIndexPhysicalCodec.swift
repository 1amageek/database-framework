import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import StorageKit

/// Borrowed canonical components decoded from one physical RDF index key.
package struct RDFQuadIndexEncodedQuad: Sendable {
    package let subject: ByteString
    package let predicate: ByteString
    package let object: ByteString
    package let graph: ByteString?
}

/// Role-aware physical codec for the six canonical RDF quad indexes.
package struct RDFQuadIndexPhysicalCodec: Sendable {
    private let spo: Subspace
    private let pos: Subspace
    private let osp: Subspace
    private let gspo: Subspace
    private let gpos: Subspace
    private let gosp: Subspace

    package init(baseSubspace: Subspace) {
        self.spo = baseSubspace.subspace(Int64(2))
        self.pos = baseSubspace.subspace(Int64(3))
        self.osp = baseSubspace.subspace(Int64(4))
        self.gspo = baseSubspace.subspace(Int64(8))
        self.gpos = baseSubspace.subspace(Int64(9))
        self.gosp = baseSubspace.subspace(Int64(10))
    }

    /// Emits a fixed four-component key into one final allocation.
    package func encode(
        _ entry: RDFQuadIndexEntryWritePlan
    ) throws(RDFQuadIndexPhysicalCodecError) -> ByteString {
        let target = try subspace(for: entry.ordering)
        let encodedTupleByteCount = try encodedByteCount(
            entry.first,
            entry.second,
            entry.third,
            entry.fourth
        )
        _ = try checkedAdd(target.prefix.count, encodedTupleByteCount)
        return try target.pack(
            encodedTupleByteCount: encodedTupleByteCount
        ) { (sink: inout TupleEncodingSink) throws(RDFQuadIndexPhysicalCodecError) in
            try write(entry.first, terminator: 0, to: &sink)
            try write(entry.second, terminator: 0, to: &sink)
            try write(entry.third, terminator: 0, to: &sink)
            try write(entry.fourth, terminator: 0, to: &sink)
        }
    }

    /// Produces both final range boundaries directly. No packed prefix key is
    /// materialized between the semantic prefix and the two returned keys.
    package func range(
        prefix: RDFQuadIndexPrefixWritePlan,
        ordering: GraphIndexOrdering
    ) throws(RDFQuadIndexPhysicalCodecError) -> (begin: ByteString, end: ByteString) {
        let target = try subspace(for: ordering)
        guard prefix.count > 0 else {
            return target.range()
        }

        let componentByteCount = try encodedByteCount(prefix)
        let beginExtraByteCount = prefix.count < 4 ? 1 : 0
        let beginByteCount = try checkedAdd(
            componentByteCount,
            beginExtraByteCount
        )
        _ = try checkedAdd(target.prefix.count, beginByteCount)
        _ = try checkedAdd(target.prefix.count, componentByteCount)

        let begin = try target.pack(
            encodedTupleByteCount: beginByteCount
        ) { (sink: inout TupleEncodingSink) throws(RDFQuadIndexPhysicalCodecError) in
            try prefix.forEachComponent {
                (
                    component: RDFQuadIndexComponentWritePlan,
                    _: Int
                ) throws(RDFQuadIndexPhysicalCodecError) in
                try write(component, terminator: 0, to: &sink)
            }
            if prefix.count < 4 {
                sink.writeByte(0)
            }
        }
        let end = try target.pack(
            encodedTupleByteCount: componentByteCount
        ) { (sink: inout TupleEncodingSink) throws(RDFQuadIndexPhysicalCodecError) in
            try prefix.forEachComponent {
                (
                    component: RDFQuadIndexComponentWritePlan,
                    position: Int
                ) throws(RDFQuadIndexPhysicalCodecError) in
                let terminator: UInt8 = position == prefix.count - 1 ? 1 : 0
                try write(component, terminator: terminator, to: &sink)
            }
        }
        return (begin, end)
    }

    package func subspace(
        for ordering: GraphIndexOrdering
    ) throws(RDFQuadIndexPhysicalCodecError) -> Subspace {
        switch ordering {
        case .spo: spo
        case .pos: pos
        case .osp: osp
        case .gspo: gspo
        case .gpos: gpos
        case .gosp: gosp
        case .out, .in, .sop, .pso, .ops:
            throw .unsupportedOrdering(ordering)
        }
    }

    /// Decodes borrowed tuple slices and materializes RDF strings only for the
    /// semantic quad returned to the caller.
    package func decodeQuad(
        key: ByteString,
        ordering: GraphIndexOrdering
    ) throws(RDFQuadIndexPhysicalCodecError) -> RDFQuad {
        let encoded = try decodeEncodedQuad(key: key, ordering: ordering)
        let subject = try decode(encoded.subject, component: .subject)
        let predicate = try decode(encoded.predicate, component: .predicate)
        let object = try decode(encoded.object, component: .object)
        let graph: RDFTerm?
        if let encodedGraph = encoded.graph {
            graph = try decode(encodedGraph, component: .graph)
        } else {
            graph = nil
        }
        do {
            return try RDFQuad(
                validatingSubject: subject,
                predicate: predicate,
                object: object,
                graph: graph
            )
        } catch {
            throw .invalidQuad(error)
        }
    }

    /// Reorders four borrowed tuple slices without materializing RDF strings.
    package func decodeEncodedQuad(
        key: ByteString,
        ordering: GraphIndexOrdering
    ) throws(RDFQuadIndexPhysicalCodecError) -> RDFQuadIndexEncodedQuad {
        let orderingSubspace = try subspace(for: ordering)
        var cursor: TupleCursor
        do {
            cursor = try orderingSubspace.tupleCursor(for: key)
        } catch {
            throw .prefixMismatch(ordering)
        }

        let first = try requiredBytes(from: &cursor, position: 0)
        let second = try requiredBytes(from: &cursor, position: 1)
        let third = try requiredBytes(from: &cursor, position: 2)
        let fourth = try requiredBytes(from: &cursor, position: 3)
        guard cursor.isAtEnd else {
            throw .unexpectedTrailingTupleData(offset: cursor.consumedByteCount)
        }

        let firstTriple: ByteString
        let secondTriple: ByteString
        let thirdTriple: ByteString
        let encodedGraph: ByteString
        if ordering.isGraphFirst {
            encodedGraph = first
            firstTriple = second
            secondTriple = third
            thirdTriple = fourth
        } else {
            firstTriple = first
            secondTriple = second
            thirdTriple = third
            encodedGraph = fourth
        }

        let subject: ByteString
        let predicate: ByteString
        let object: ByteString
        switch ordering {
        case .spo, .gspo:
            subject = firstTriple
            predicate = secondTriple
            object = thirdTriple
        case .pos, .gpos:
            predicate = firstTriple
            object = secondTriple
            subject = thirdTriple
        case .osp, .gosp:
            object = firstTriple
            subject = secondTriple
            predicate = thirdTriple
        case .out, .in, .sop, .pso, .ops:
            throw .unsupportedOrdering(ordering)
        }

        let graph: ByteString?
        if encodedGraph.count == 1
            && encodedGraph[encodedGraph.startIndex] == 0xff {
            graph = nil
        } else {
            graph = encodedGraph
        }
        return RDFQuadIndexEncodedQuad(
            subject: subject,
            predicate: predicate,
            object: object,
            graph: graph
        )
    }

    /// Decodes only a borrowed canonical graph component. Named-graph
    /// enumeration does not need to materialize subject, predicate, or object.
    package func decodeGraphComponent(
        _ bytes: ByteString
    ) throws(RDFQuadIndexPhysicalCodecError) -> RDFTerm {
        try decode(bytes, component: .graph)
    }

    private func encodedByteCount(
        _ first: RDFQuadIndexComponentWritePlan,
        _ second: RDFQuadIndexComponentWritePlan,
        _ third: RDFQuadIndexComponentWritePlan,
        _ fourth: RDFQuadIndexComponentWritePlan
    ) throws(RDFQuadIndexPhysicalCodecError) -> Int {
        var result = 0
        result = try addEncodedByteCount(of: first, to: result)
        result = try addEncodedByteCount(of: second, to: result)
        result = try addEncodedByteCount(of: third, to: result)
        result = try addEncodedByteCount(of: fourth, to: result)
        return result
    }

    private func encodedByteCount(
        _ prefix: RDFQuadIndexPrefixWritePlan
    ) throws(RDFQuadIndexPhysicalCodecError) -> Int {
        var result = 0
        try prefix.forEachComponent {
            (
                component: RDFQuadIndexComponentWritePlan,
                _: Int
            ) throws(RDFQuadIndexPhysicalCodecError) in
            result = try addEncodedByteCount(of: component, to: result)
        }
        return result
    }

    private func addEncodedByteCount(
        of component: RDFQuadIndexComponentWritePlan,
        to current: Int
    ) throws(RDFQuadIndexPhysicalCodecError) -> Int {
        let withPayload = try checkedAdd(current, component.payloadByteCount)
        return try checkedAdd(withPayload, 2)
    }

    private func write(
        _ component: RDFQuadIndexComponentWritePlan,
        terminator: UInt8,
        to sink: inout TupleEncodingSink
    ) throws(RDFQuadIndexPhysicalCodecError) {
        sink.writeByte(TupleTypeCode.bytes.rawValue)
        switch component {
        case .term(let role, let encoding):
            var rdfSink = RDFSink(tupleSink: sink)
            do {
                try RDFTermStorageFormat.encode(
                    encoding,
                    into: &rdfSink
                )
            } catch {
                throw .invalidEncoding(role, error)
            }
            sink = rdfSink.tupleSink
        case .canonicalBytes(_, let bytes):
            bytes.withUnsafeBytes { source in
                sink.writeBytes(source)
            }
        case .defaultGraph:
            sink.writeByte(0xff)
        }
        sink.writeByte(terminator)
    }

    private func decode(
        _ bytes: ByteString,
        component: RDFDatasetIndexComponent
    ) throws(RDFQuadIndexPhysicalCodecError) -> RDFTerm {
        let role: RDFTermRole
        switch component {
        case .subject: role = .subject
        case .predicate: role = .predicate
        case .object: role = .object
        case .graph: role = .graphName
        }
        do {
            return try RDFTermStorageFormat.decode(
                bytes,
                role: role
            )
        } catch {
            throw .invalidComponent(component, error)
        }
    }

    private func requiredBytes(
        from cursor: inout TupleCursor,
        position: Int
    ) throws(RDFQuadIndexPhysicalCodecError) -> ByteString {
        do {
            return try cursor.requireBytes()
        } catch TupleError.unexpectedEndOfData {
            throw .truncatedComponent(position: position)
        } catch TupleError.invalidTypeCode(let actualTypeCode) {
            throw .unexpectedTupleType(
                position: position,
                actualTypeCode: actualTypeCode
            )
        } catch {
            throw .invalidTupleEncoding(position: position)
        }
    }

    private func checkedAdd(
        _ lhs: Int,
        _ rhs: Int
    ) throws(RDFQuadIndexPhysicalCodecError) -> Int {
        let (result, overflow) = lhs.addingReportingOverflow(rhs)
        guard !overflow else {
            throw .byteCountOverflow
        }
        return result
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
