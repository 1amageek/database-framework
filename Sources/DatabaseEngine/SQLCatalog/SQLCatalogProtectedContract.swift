import DatabaseTypes

struct SQLCatalogProtectedContract: Sendable, Hashable {
    enum Source: UInt8, Sendable {
        case compiledModel = 1
    }

    let relationID: String
    let entityName: String
    let entityDigest: ByteString
    let source: Source

    init(relationID: String, entityName: String, entityDigest: ByteString,
        source: Source = .compiledModel
    ) {
        self.relationID = relationID
        self.entityName = entityName
        self.entityDigest = entityDigest
        self.source = source
    }

    static func == (lhs: Self, rhs: Self) -> Bool {
        SQLCatalogName(lhs.relationID) == SQLCatalogName(rhs.relationID)
            && SQLCatalogName(lhs.entityName) == SQLCatalogName(rhs.entityName)
            && lhs.entityDigest == rhs.entityDigest && lhs.source == rhs.source
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(SQLCatalogName(relationID))
        hasher.combine(SQLCatalogName(entityName))
        hasher.combine(entityDigest)
        hasher.combine(source)
    }
}
