struct DatabaseStoreCacheKey: Sendable, Equatable, Comparable {
    let basePlacementGeneration: UInt64
    let entity: String
    let components: [String]

    static func < (
        lhs: DatabaseStoreCacheKey,
        rhs: DatabaseStoreCacheKey
    ) -> Bool {
        if lhs.basePlacementGeneration != rhs.basePlacementGeneration {
            return lhs.basePlacementGeneration < rhs.basePlacementGeneration
        }
        if lhs.entity != rhs.entity {
            return lhs.entity < rhs.entity
        }
        for (left, right) in zip(lhs.components, rhs.components) {
            if left != right {
                return left < right
            }
        }
        return lhs.components.count < rhs.components.count
    }
}
