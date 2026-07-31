public enum DatabaseWorkStage: String, Sendable, Hashable {
    case indexScan
    case storageRow
    case bindingCandidate
    case filterEvaluation
    case expressionEvaluation
    case joinCandidate
    case pathExpansion
    case aggregateInput
    case projection
    case sortInput
    case sortComparison
    case deduplication
    case subqueryCache
    case mutationPlanning
    case storageWrite
    case validation
    case resultMaterialization
}
