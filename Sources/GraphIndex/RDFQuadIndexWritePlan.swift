import DatabaseTypes
import DatabaseKit
import DatabaseEngine

/// One validated component ready for direct emission into an RDF index key.
package enum RDFQuadIndexComponentWritePlan: Sendable {
    case term(
        role: RDFTermRole,
        encoding: RDFTermStorageEncoding
    )
    case canonicalBytes(
        role: RDFTermRole,
        bytes: ByteString
    )
    case defaultGraph

    package init(
        term: RDFTerm,
        role: RDFTermRole,
        limits: RDFTermStorageLimits = .default
    ) throws(RDFTermStorageError) {
        self = .term(
            role: role,
            encoding: try RDFTermStorageFormat.encodingPlan(
                term,
                role: role,
                limits: limits
            )
        )
    }

    package init(
        canonicalBytes: ByteString,
        role: RDFTermRole,
        limits: RDFTermStorageLimits = .default
    ) throws(RDFTermStorageError) {
        _ = try RDFTermStorageFormat.validate(
            canonicalBytes,
            role: role,
            limits: limits
        )
        self = .canonicalBytes(role: role, bytes: canonicalBytes)
    }

    package var payloadByteCount: Int {
        switch self {
        case .term(_, let encoding):
            encoding.byteCount
        case .canonicalBytes(_, let bytes):
            bytes.count
        case .defaultGraph:
            1
        }
    }
}

/// Fixed-width physical key entry. It deliberately avoids component arrays and
/// tuple existential storage on the index write path.
package struct RDFQuadIndexEntryWritePlan: Sendable {
    package let ordering: GraphIndexOrdering
    package let first: RDFQuadIndexComponentWritePlan
    package let second: RDFQuadIndexComponentWritePlan
    package let third: RDFQuadIndexComponentWritePlan
    package let fourth: RDFQuadIndexComponentWritePlan

    /// Returns the exact tuple suffix size before allocating a physical key.
    /// Canonical RDF payloads contain no zero bytes, so tuple escaping cannot
    /// expand these components.
    package func encodedTupleByteCount()
        throws(RDFQuadIndexPhysicalCodecError) -> Int {
        var count = 0
        count = try addingEncodedByteCount(of: first, to: count)
        count = try addingEncodedByteCount(of: second, to: count)
        count = try addingEncodedByteCount(of: third, to: count)
        count = try addingEncodedByteCount(of: fourth, to: count)
        return count
    }

    private func addingEncodedByteCount(
        of component: RDFQuadIndexComponentWritePlan,
        to current: Int
    ) throws(RDFQuadIndexPhysicalCodecError) -> Int {
        let (withPayload, payloadOverflow) = current.addingReportingOverflow(
            component.payloadByteCount
        )
        guard !payloadOverflow else { throw .byteCountOverflow }
        let (withTupleFraming, framingOverflow) = withPayload
            .addingReportingOverflow(2)
        guard !framingOverflow else { throw .byteCountOverflow }
        return withTupleFraming
    }
}

/// Validates an RDF quad once and reuses those plans for all six index keys.
package struct RDFQuadIndexWritePlan: Sendable {
    private let subject: RDFQuadIndexComponentWritePlan
    private let predicate: RDFQuadIndexComponentWritePlan
    private let object: RDFQuadIndexComponentWritePlan
    private let graph: RDFQuadIndexComponentWritePlan

    package init(
        quad: RDFQuad,
        limits: RDFTermStorageLimits = .default
    ) throws(RDFTermStorageError) {
        self.subject = try RDFQuadIndexComponentWritePlan(
            term: quad.subject.term,
            role: .subject,
            limits: limits
        )
        self.predicate = try RDFQuadIndexComponentWritePlan(
            term: quad.predicate.term,
            role: .predicate,
            limits: limits
        )
        self.object = try RDFQuadIndexComponentWritePlan(
            term: quad.object,
            role: .object,
            limits: limits
        )
        if let graph = quad.graph {
            self.graph = try RDFQuadIndexComponentWritePlan(
                term: graph.term,
                role: .graphName,
                limits: limits
            )
        } else {
            self.graph = .defaultGraph
        }
    }

    /// Reuses borrowed canonical term payloads decoded from one stored key.
    /// Every component is validated once, then permuted into all physical keys
    /// without materializing RDF strings.
    package init(
        encodedQuad: RDFQuadIndexEncodedQuad,
        limits: RDFTermStorageLimits = .default
    ) throws(RDFTermStorageError) {
        self.subject = try RDFQuadIndexComponentWritePlan(
            canonicalBytes: encodedQuad.subject,
            role: .subject,
            limits: limits
        )
        self.predicate = try RDFQuadIndexComponentWritePlan(
            canonicalBytes: encodedQuad.predicate,
            role: .predicate,
            limits: limits
        )
        self.object = try RDFQuadIndexComponentWritePlan(
            canonicalBytes: encodedQuad.object,
            role: .object,
            limits: limits
        )
        if let graph = encodedQuad.graph {
            self.graph = try RDFQuadIndexComponentWritePlan(
                canonicalBytes: graph,
                role: .graphName,
                limits: limits
            )
        } else {
            self.graph = .defaultGraph
        }
    }

    /// The canonical existence key. Callers can retain and reuse this final
    /// allocation when a mutation needs a presence check before writing.
    package var primaryEntry: RDFQuadIndexEntryWritePlan {
        RDFQuadIndexEntryWritePlan(
            ordering: .spo,
            first: subject,
            second: predicate,
            third: object,
            fourth: graph
        )
    }

    /// Visits six fixed entries without constructing an entry array.
    package func forEachEntry<Failure: Error>(
        _ body: (RDFQuadIndexEntryWritePlan) throws(Failure) -> Void
    ) throws(Failure) {
        try body(RDFQuadIndexEntryWritePlan(
            ordering: .spo,
            first: subject,
            second: predicate,
            third: object,
            fourth: graph
        ))
        try body(RDFQuadIndexEntryWritePlan(
            ordering: .pos,
            first: predicate,
            second: object,
            third: subject,
            fourth: graph
        ))
        try body(RDFQuadIndexEntryWritePlan(
            ordering: .osp,
            first: object,
            second: subject,
            third: predicate,
            fourth: graph
        ))
        try body(RDFQuadIndexEntryWritePlan(
            ordering: .gspo,
            first: graph,
            second: subject,
            third: predicate,
            fourth: object
        ))
        try body(RDFQuadIndexEntryWritePlan(
            ordering: .gpos,
            first: graph,
            second: predicate,
            third: object,
            fourth: subject
        ))
        try body(RDFQuadIndexEntryWritePlan(
            ordering: .gosp,
            first: graph,
            second: object,
            third: subject,
            fourth: predicate
        ))
    }
}

/// A stack-sized prefix used to build an RDF range without an intermediate key.
package struct RDFQuadIndexPrefixWritePlan: Sendable {
    private var first: RDFQuadIndexComponentWritePlan?
    private var second: RDFQuadIndexComponentWritePlan?
    private var third: RDFQuadIndexComponentWritePlan?
    private var fourth: RDFQuadIndexComponentWritePlan?
    package private(set) var count: Int

    package init() {
        self.first = nil
        self.second = nil
        self.third = nil
        self.fourth = nil
        self.count = 0
    }

    package mutating func append(
        _ component: RDFQuadIndexComponentWritePlan
    ) throws(RDFQuadIndexPhysicalCodecError) {
        switch count {
        case 0: first = component
        case 1: second = component
        case 2: third = component
        case 3: fourth = component
        default:
            throw .invalidComponentCount(expected: 0...4, actual: count + 1)
        }
        count += 1
    }

    package func forEachComponent<Failure: Error>(
        _ body: (
            RDFQuadIndexComponentWritePlan,
            Int
        ) throws(Failure) -> Void
    ) throws(Failure) {
        if let first { try body(first, 0) }
        if let second { try body(second, 1) }
        if let third { try body(third, 2) }
        if let fourth { try body(fourth, 3) }
    }
}
