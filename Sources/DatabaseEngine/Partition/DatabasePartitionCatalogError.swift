package enum DatabasePartitionCatalogError: Error, Sendable, Equatable {
    case invalidEntity(String)
    case invalidPartitions(entity: String, reason: String)
    case invalidContinuation
    case invalidPageLimit(actual: Int, maximum: Int)
    case digestCollision
    case corruptedEntry
}
