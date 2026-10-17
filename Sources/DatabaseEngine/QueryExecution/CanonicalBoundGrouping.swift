import DatabaseKit

/// Selected grouping leaves and the ordered grouping-set occurrences that use them.
struct CanonicalBoundGrouping: Sendable {
    let leaves: [CanonicalBoundExpression]
    let sets: [[Int]]
    let metadataLease: DatabaseQueryPayloadLease?

    init(leaves: [CanonicalBoundExpression], sets: [[Int]],
         metadataLease: DatabaseQueryPayloadLease? = nil) {
        self.leaves = leaves
        self.sets = sets
        self.metadataLease = metadataLease
    }
}
