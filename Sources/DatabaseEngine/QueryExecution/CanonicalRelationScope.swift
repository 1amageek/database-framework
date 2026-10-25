import DatabaseKit
import DatabaseTypes

struct CanonicalRelationScope: Sendable, Equatable {
    let name: String
    let columns: [String]
    let types: [QueryColumnType?]
    let nullability: [Bool]

    static func == (left: Self, right: Self) -> Bool {
        CanonicalSourceName.equal(left.name, right.name) && CanonicalSourceName.equal(left.columns, right.columns)
            && left.types == right.types && left.nullability == right.nullability
    }

    init(name: String, columns: [String], types: [QueryColumnType?]? = nil, nullability: [Bool]? = nil) {
        self.name = name
        self.columns = columns
        self.types = types ?? Array(repeating: nil, count: columns.count)
        self.nullability = nullability ?? Array(repeating: true, count: columns.count)
        precondition(self.types.count == columns.count && self.nullability.count == columns.count)
    }
}
