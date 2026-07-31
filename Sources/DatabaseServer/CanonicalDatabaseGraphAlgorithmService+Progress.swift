#if DATABASE_SERVER_GRAPH_INDEXES
import DatabaseEngine
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire

extension CanonicalDatabaseGraphAlgorithmService {
    func progress(
        isComplete: Bool,
        limitReason: DatabaseEngine.LimitReason?,
        resultPageComplete: Bool = true,
        continuation: ByteString? = nil
    ) throws -> GraphAlgorithmOperation.Progress {
        guard isComplete == (limitReason == nil) else {
            throw DatabaseGraphAlgorithmError.inconsistentAlgorithmResult(
                "completion and limit reason disagree"
            )
        }
        return GraphAlgorithmOperation.Progress(
            algorithmComplete: isComplete,
            resultPageComplete: resultPageComplete,
            limitReason: try limitReason.map(wireLimitReason),
            continuation: continuation
        )
    }

    func wireLimitReason(
        _ reason: DatabaseEngine.LimitReason
    ) throws -> GraphAlgorithmOperation.LimitReason {
        switch reason {
        case .maxDepthReached:
            return .maximumDepth
        case .maxNodesReached:
            return .maximumNodes
        case .maxWeightReached:
            return .maximumWeight
        case .maxIterationsReached:
            return .maximumIterations
        case .maxResultsReached, .maxCyclesReached:
            return .maximumResults
        case .maxWorkUnitsReached:
            return .maximumWorkUnits
        case .maxCellsReached:
            throw DatabaseGraphAlgorithmError.unsupportedAlgorithmLimit(
                reason.description
            )
        }
    }
}
#endif
