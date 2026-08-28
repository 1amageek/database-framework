import DatabaseTypes

/// Linear merge algebra for ordered, unique full-text posting candidates.
///
/// Candidates carry one canonical packed key from the scan boundary, so a
/// merge compares the existing ordering bytes and never decodes, materializes
/// tuple elements, or re-encodes an identifier.
enum FullTextPostingListAlgebra {
    typealias Identifier = FullTextPostingCandidate

    static func intersection(
        _ lhs: [Identifier],
        _ rhs: [Identifier],
        reservingCapacity: Bool = true,
        admitting: (Identifier) throws -> Void = { _ in }
    ) rethrows -> [Identifier] {
        guard !lhs.isEmpty, !rhs.isEmpty else { return [] }

        var result: [Identifier] = []
        if reservingCapacity {
            result.reserveCapacity(Swift.min(lhs.count, rhs.count))
        }
        var lhsIndex = 0
        var rhsIndex = 0

        while lhsIndex < lhs.count, rhsIndex < rhs.count {
            let lhsCandidate = lhs[lhsIndex]
            let rhsCandidate = rhs[rhsIndex]
            if lhsCandidate.canonicalKey == rhsCandidate.canonicalKey {
                try admitting(lhsCandidate)
                result.append(lhsCandidate)
                lhsIndex += 1
                rhsIndex += 1
            } else if lhsCandidate.canonicalKey.lexicographicallyPrecedes(
                rhsCandidate.canonicalKey
            ) {
                lhsIndex += 1
            } else {
                rhsIndex += 1
            }
        }

        return result
    }

    static func union(
        _ lhs: [Identifier],
        _ rhs: [Identifier],
        reservingCapacity: Bool = true,
        admitting: (Identifier) throws -> Void = { _ in }
    ) rethrows -> [Identifier] {
        guard !lhs.isEmpty else {
            return try admittedCopy(
                of: rhs,
                reservingCapacity: reservingCapacity,
                admitting: admitting
            )
        }
        guard !rhs.isEmpty else {
            return try admittedCopy(
                of: lhs,
                reservingCapacity: reservingCapacity,
                admitting: admitting
            )
        }

        var result: [Identifier] = []
        let (maximumCount, overflow) = lhs.count.addingReportingOverflow(
            rhs.count
        )
        if reservingCapacity, !overflow {
            result.reserveCapacity(maximumCount)
        }
        var lhsIndex = 0
        var rhsIndex = 0

        while lhsIndex < lhs.count, rhsIndex < rhs.count {
            let candidate: Identifier
            let lhsCandidate = lhs[lhsIndex]
            let rhsCandidate = rhs[rhsIndex]
            if lhsCandidate.canonicalKey == rhsCandidate.canonicalKey {
                candidate = lhsCandidate
                lhsIndex += 1
                rhsIndex += 1
            } else if lhsCandidate.canonicalKey.lexicographicallyPrecedes(
                rhsCandidate.canonicalKey
            ) {
                candidate = lhsCandidate
                lhsIndex += 1
            } else {
                candidate = rhsCandidate
                rhsIndex += 1
            }
            try admitting(candidate)
            result.append(candidate)
        }

        while lhsIndex < lhs.count {
            let candidate = lhs[lhsIndex]
            try admitting(candidate)
            result.append(candidate)
            lhsIndex += 1
        }
        while rhsIndex < rhs.count {
            let candidate = rhs[rhsIndex]
            try admitting(candidate)
            result.append(candidate)
            rhsIndex += 1
        }

        return result
    }

    private static func admittedCopy(
        of source: [Identifier],
        reservingCapacity: Bool,
        admitting: (Identifier) throws -> Void
    ) rethrows -> [Identifier] {
        var result: [Identifier] = []
        if reservingCapacity {
            result.reserveCapacity(source.count)
        }
        for candidate in source {
            try admitting(candidate)
            result.append(candidate)
        }
        return result
    }
}
