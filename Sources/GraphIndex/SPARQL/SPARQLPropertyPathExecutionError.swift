enum SPARQLPropertyPathExecutionError: Error, Sendable, Equatable {
    case statisticsOverflow
    case unexpectedJoinStatistics
    case invalidBindingFootprintComposition
}
