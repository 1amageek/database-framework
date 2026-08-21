package enum SPARQLRetainedResultError: Error, Sendable, Equatable {
    case missingVariable(String)
    case workMeterMismatch
}
