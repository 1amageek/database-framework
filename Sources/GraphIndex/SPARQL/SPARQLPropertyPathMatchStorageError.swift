enum SPARQLPropertyPathMatchStorageError: Error, Sendable, Equatable {
    case invalidMaximumResults(Int)
    case invalidSetCapacity(Int)
    case invalidSetRequiredCount(Int)
    case setCapacityOverflow(currentCapacity: Int)
    case setCountOverflow
}
