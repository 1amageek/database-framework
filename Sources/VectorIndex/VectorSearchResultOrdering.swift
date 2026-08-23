import DatabaseTypes
import StorageKit

enum VectorSearchResultOrdering {
    static func isWorse(
        _ lhs: (primaryKey: [any TupleElement], distance: Double),
        than rhs: (primaryKey: [any TupleElement], distance: Double)
    ) -> Bool {
        if lhs.distance == rhs.distance {
            return Tuple(lhs.primaryKey).pack() > Tuple(rhs.primaryKey).pack()
        }
        return lhs.distance > rhs.distance
    }

    static func isBetter(
        _ lhs: (primaryKey: [any TupleElement], distance: Double),
        than rhs: (primaryKey: [any TupleElement], distance: Double)
    ) -> Bool {
        if lhs.distance == rhs.distance {
            return Tuple(lhs.primaryKey).pack() < Tuple(rhs.primaryKey).pack()
        }
        return lhs.distance < rhs.distance
    }
}
