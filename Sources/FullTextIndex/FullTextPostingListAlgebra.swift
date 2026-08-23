import DatabaseTypes
import StorageKit

/// Linear merge algebra for the ordered, unique identifier suffixes returned
/// by full-text posting-list range scans.
enum FullTextPostingListAlgebra {
    typealias Identifier = [any TupleElement]

    static func intersection<LHS, RHS>(
        _ lhs: LHS,
        _ rhs: RHS,
        reservingCapacity: Bool = true,
        admitting: (Identifier, ByteString) throws -> Void = { _, _ in }
    ) rethrows -> [Identifier]
    where LHS: RandomAccessCollection,
          RHS: RandomAccessCollection,
          LHS.Index == Int,
          RHS.Index == Int,
          LHS.Element == Identifier,
          RHS.Element == Identifier {
        guard !lhs.isEmpty, !rhs.isEmpty else { return [] }

        var result: [Identifier] = []
        if reservingCapacity {
            result.reserveCapacity(Swift.min(lhs.count, rhs.count))
        }
        var lhsIndex = 0
        var rhsIndex = 0
        var lhsKey = stableKey(lhs[lhsIndex])
        var rhsKey = stableKey(rhs[rhsIndex])

        while lhsIndex < lhs.count, rhsIndex < rhs.count {
            if lhsKey == rhsKey {
                try admitting(lhs[lhsIndex], lhsKey)
                result.append(lhs[lhsIndex])
                lhsIndex += 1
                rhsIndex += 1
                if lhsIndex < lhs.count {
                    lhsKey = stableKey(lhs[lhsIndex])
                }
                if rhsIndex < rhs.count {
                    rhsKey = stableKey(rhs[rhsIndex])
                }
            } else if lhsKey.lexicographicallyPrecedes(rhsKey) {
                lhsIndex += 1
                if lhsIndex < lhs.count {
                    lhsKey = stableKey(lhs[lhsIndex])
                }
            } else {
                rhsIndex += 1
                if rhsIndex < rhs.count {
                    rhsKey = stableKey(rhs[rhsIndex])
                }
            }
        }

        return result
    }

    static func union<LHS, RHS>(
        _ lhs: LHS,
        _ rhs: RHS,
        reservingCapacity: Bool = true,
        admitting: (Identifier, ByteString) throws -> Void = { _, _ in }
    ) rethrows -> [Identifier]
    where LHS: RandomAccessCollection,
          RHS: RandomAccessCollection,
          LHS.Index == Int,
          RHS.Index == Int,
          LHS.Element == Identifier,
          RHS.Element == Identifier {
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
        var lhsKey = stableKey(lhs[lhsIndex])
        var rhsKey = stableKey(rhs[rhsIndex])

        while lhsIndex < lhs.count, rhsIndex < rhs.count {
            let identifier: Identifier
            let key: ByteString
            if lhsKey == rhsKey {
                identifier = lhs[lhsIndex]
                key = lhsKey
                lhsIndex += 1
                rhsIndex += 1
                if lhsIndex < lhs.count {
                    lhsKey = stableKey(lhs[lhsIndex])
                }
                if rhsIndex < rhs.count {
                    rhsKey = stableKey(rhs[rhsIndex])
                }
            } else if lhsKey.lexicographicallyPrecedes(rhsKey) {
                identifier = lhs[lhsIndex]
                key = lhsKey
                lhsIndex += 1
                if lhsIndex < lhs.count {
                    lhsKey = stableKey(lhs[lhsIndex])
                }
            } else {
                identifier = rhs[rhsIndex]
                key = rhsKey
                rhsIndex += 1
                if rhsIndex < rhs.count {
                    rhsKey = stableKey(rhs[rhsIndex])
                }
            }
            try admitting(identifier, key)
            result.append(identifier)
        }

        while lhsIndex < lhs.count {
            let identifier = lhs[lhsIndex]
            try admitting(identifier, stableKey(identifier))
            result.append(identifier)
            lhsIndex += 1
        }
        while rhsIndex < rhs.count {
            let identifier = rhs[rhsIndex]
            try admitting(identifier, stableKey(identifier))
            result.append(identifier)
            rhsIndex += 1
        }

        return result
    }

    private static func admittedCopy<Source>(
        of source: Source,
        reservingCapacity: Bool,
        admitting: (Identifier, ByteString) throws -> Void
    ) rethrows -> [Identifier]
    where Source: RandomAccessCollection,
          Source.Index == Int,
          Source.Element == Identifier {
        var result: [Identifier] = []
        if reservingCapacity {
            result.reserveCapacity(source.count)
        }
        for identifier in source {
            try admitting(identifier, stableKey(identifier))
            result.append(identifier)
        }
        return result
    }

    @inline(__always)
    private static func stableKey(_ identifier: Identifier) -> ByteString {
        Tuple(identifier).pack()
    }
}
