import DatabaseEngine
import DatabaseTypes

/// Computes canonical retained-memory charges for property-path matches.
///
/// RDF-star terms use the shared single-cursor retained-footprint traversal,
/// so nesting depth requires no scratch Array or request reservation.
enum SPARQLPropertyPathMatchRetainedFootprint {
    private static let matchValueByteCount: UInt64 = 64

    static func retainedArrayLayout() throws
        -> DatabaseRetainedArrayLayout {
        try DatabaseRetainedArrayLayout.validated(
            containerByteCount: 64,
            elementCapacitySlotByteCount: 64,
            sharedOwnerByteCount: 64,
            appendAdmissionByteCount: 64
        )
    }

    static func measure(
        of match: borrowing SPARQLPropertyPathMatch
    ) throws -> DatabaseIntermediateFootprint {
        try measure(start: match.start, end: match.end)
    }

    /// Measures both endpoints before a match value is constructed.
    static func measure(
        start: borrowing RDFTerm,
        end: borrowing RDFTerm
    ) throws -> DatabaseIntermediateFootprint {
        try DatabaseIntermediateFootprint(
            rows: 1,
            bytes: Self.matchValueByteCount
        ).adding(
            RDFTermRetainedFootprint.measure(start)
        ).adding(
            RDFTermRetainedFootprint.measure(end)
        )
    }
}
