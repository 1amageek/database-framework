import DatabaseKit

/// Selected SQL transition inputs. Children are arguments, internal order,
/// FILTER, then WITHIN GROUP; direct arguments alias ordered-set arguments.
final class CanonicalBoundAggregate: Sendable {
    let call: SQLAggregateCall
    let arguments: [CanonicalBoundExpression]
    let directArguments: [CanonicalBoundExpression]
    let orderBy: [CanonicalBoundKey]
    let filter: CanonicalBoundExpression?
    let withinGroup: [CanonicalBoundKey]

    static var storageByteCount: UInt64 {
        UInt64(MemoryLayout<SQLAggregateCall>.stride + MemoryLayout<[CanonicalBoundExpression]>.stride * 2
            + MemoryLayout<[CanonicalBoundKey]>.stride * 2 + MemoryLayout<CanonicalBoundExpression?>.stride
            + MemoryLayout<Int>.stride * 2)
    }

    init(call: SQLAggregateCall, arguments: [CanonicalBoundExpression],
         directArguments: [CanonicalBoundExpression], orderBy: [CanonicalBoundKey],
         filter: CanonicalBoundExpression?, withinGroup: [CanonicalBoundKey]) {
        self.call = call
        self.arguments = arguments
        self.directArguments = directArguments
        self.orderBy = orderBy
        self.filter = filter
        self.withinGroup = withinGroup
    }
}
