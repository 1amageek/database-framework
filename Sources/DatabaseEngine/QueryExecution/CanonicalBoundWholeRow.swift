import DatabaseKit

/// Selected relation fields and the lexical source-presence address.
struct CanonicalBoundWholeRow: Sendable {
    let type: QueryCompositeType
    let fieldSlots: [Int]
    let presenceIndex: Int
    let metadataLease: DatabaseQueryPayloadLease?

    init(type: QueryCompositeType, fieldSlots: [Int], presenceIndex: Int,
         metadataLease: DatabaseQueryPayloadLease? = nil) {
        self.type = type
        self.fieldSlots = fieldSlots
        self.presenceIndex = presenceIndex
        self.metadataLease = metadataLease
    }
}
