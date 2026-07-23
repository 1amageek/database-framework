package enum XSDValueComparisonFailure: Error, Sendable, Equatable {
    case duration(XSDDurationValue.ParseFailure)
    case rationalWork(limit: Int, actual: Int)
    case xmlWork(limit: Int, actual: Int)
}
