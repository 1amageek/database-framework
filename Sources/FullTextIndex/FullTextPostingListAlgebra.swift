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
        limit: Int? = nil,
        admitting: (Identifier) throws -> Void = { _ in }
    ) rethrows -> [Identifier] {
        let outputLimit = limit.map { max($0, 0) }
        guard outputLimit != 0, !lhs.isEmpty, !rhs.isEmpty else {
            return []
        }

        var result: [Identifier] = []
        if reservingCapacity {
            let maximumCount = Swift.min(lhs.count, rhs.count)
            result.reserveCapacity(
                Swift.min(outputLimit ?? maximumCount, maximumCount)
            )
        }
        var lhsIndex = 0
        var rhsIndex = 0

        while lhsIndex < lhs.count, rhsIndex < rhs.count {
            let lhsCandidate = lhs[lhsIndex]
            let rhsCandidate = rhs[rhsIndex]
            if lhsCandidate.canonicalKey == rhsCandidate.canonicalKey {
                if let outputLimit, result.count >= outputLimit {
                    return result
                }
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
        limit: Int? = nil,
        admitting: (Identifier) throws -> Void = { _ in }
    ) rethrows -> [Identifier] {
        let outputLimit = limit.map { max($0, 0) }
        guard outputLimit != 0 else { return [] }
        guard !lhs.isEmpty else {
            return try admittedCopy(
                of: rhs,
                reservingCapacity: reservingCapacity,
                limit: outputLimit,
                admitting: admitting
            )
        }
        guard !rhs.isEmpty else {
            return try admittedCopy(
                of: lhs,
                reservingCapacity: reservingCapacity,
                limit: outputLimit,
                admitting: admitting
            )
        }

        var result: [Identifier] = []
        let (maximumCount, overflow) = lhs.count.addingReportingOverflow(
            rhs.count
        )
        if reservingCapacity, !overflow {
            result.reserveCapacity(
                Swift.min(outputLimit ?? maximumCount, maximumCount)
            )
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
            if let outputLimit, result.count >= outputLimit {
                return result
            }
            try admitting(candidate)
            result.append(candidate)
        }

        while lhsIndex < lhs.count {
            let candidate = lhs[lhsIndex]
            if let outputLimit, result.count >= outputLimit {
                return result
            }
            try admitting(candidate)
            result.append(candidate)
            lhsIndex += 1
        }
        while rhsIndex < rhs.count {
            let candidate = rhs[rhsIndex]
            if let outputLimit, result.count >= outputLimit {
                return result
            }
            try admitting(candidate)
            result.append(candidate)
            rhsIndex += 1
        }

        return result
    }

    private static func admittedCopy(
        of source: [Identifier],
        reservingCapacity: Bool,
        limit: Int?,
        admitting: (Identifier) throws -> Void
    ) rethrows -> [Identifier] {
        guard limit != 0 else { return [] }
        var result: [Identifier] = []
        if reservingCapacity {
            result.reserveCapacity(
                Swift.min(limit ?? source.count, source.count)
            )
        }
        for candidate in source {
            if let limit, result.count >= limit {
                break
            }
            try admitting(candidate)
            result.append(candidate)
        }
        return result
    }

    static func prefix(
        _ source: [Identifier],
        limit: Int?,
        reservingCapacity: Bool = true,
        admitting: (Identifier) throws -> Void = { _ in }
    ) rethrows -> [Identifier] {
        try admittedCopy(
            of: source,
            reservingCapacity: reservingCapacity,
            limit: limit.map { max($0, 0) },
            admitting: admitting
        )
    }
}
