// MARK: - ExecutionStatistics Extension

extension ExecutionStatistics {
    func merged(with other: ExecutionStatistics) -> ExecutionStatistics {
        var result = self
        result.indexScans += other.indexScans
        result.joinOperations += other.joinOperations
        result.intermediateResults += other.intermediateResults
        result.patternsEvaluated += other.patternsEvaluated
        result.optionalMisses += other.optionalMisses
        result.joinStrategies.append(contentsOf: other.joinStrategies)
        result.joinFallbackReasons.append(contentsOf: other.joinFallbackReasons)
        return result
    }
}
