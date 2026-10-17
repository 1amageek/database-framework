import DatabaseKit

/// Immutable selected recursion; the working-table binding never retains this owner.
final class CanonicalBoundRecursiveDefinition: Sendable {
    let prelude: [CanonicalBoundCTE]
    let seedQuery: SelectQuery
    let seedPlan: CanonicalBoundSelect
    let termQuery: SelectQuery
    let termPlan: CanonicalBoundSelect
    let columns: [QueryColumn]
    let selfName: String
    let unionAll: Bool
    let generated: SQLRecursiveGeneratedColumns?
    let metadataLeases: [DatabaseQueryPayloadLease]
    let metadataLease: DatabaseQueryPayloadLease

    // Measure fields without constructing a tuple; the header is aligned before the payload.
    static var storageByteCount: UInt64 {
        typealias InstanceLayout = (
            [CanonicalBoundCTE], SelectQuery, CanonicalBoundSelect, SelectQuery, CanonicalBoundSelect,
            [QueryColumn], String, Bool, SQLRecursiveGeneratedColumns?,
            [DatabaseQueryPayloadLease], DatabaseQueryPayloadLease
        )
        let alignment = max(MemoryLayout<Int>.alignment, MemoryLayout<InstanceLayout>.alignment)
        let header = MemoryLayout<(Int, Int)>.stride
        let alignedHeader = ((header + alignment - 1) / alignment) * alignment
        return UInt64(alignedHeader + MemoryLayout<InstanceLayout>.stride)
    }

    init(prelude: [CanonicalBoundCTE], seedQuery: SelectQuery, seedPlan: CanonicalBoundSelect,
        termQuery: SelectQuery, termPlan: CanonicalBoundSelect, columns: [QueryColumn],
        selfName: String, unionAll: Bool, generated: SQLRecursiveGeneratedColumns?,
        metadataLeases: [DatabaseQueryPayloadLease], metadataLease: DatabaseQueryPayloadLease) {
        self.prelude = prelude
        self.seedQuery = seedQuery
        self.seedPlan = seedPlan
        self.termQuery = termQuery
        self.termPlan = termPlan
        self.columns = columns
        self.selfName = selfName
        self.unionAll = unionAll
        self.generated = generated
        self.metadataLeases = metadataLeases
        self.metadataLease = metadataLease
    }
}
