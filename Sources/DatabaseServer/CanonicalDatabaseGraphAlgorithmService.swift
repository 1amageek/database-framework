import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import DatabaseEngine
import GraphIndex
import StorageKit

public struct CanonicalDatabaseGraphAlgorithmService: DatabaseGraphAlgorithmService {
    let sourceResolver: any DatabaseGraphSourceResolving
    let wireLimits: DatabaseWireLimits

    public init(
        sourceResolver: any DatabaseGraphSourceResolving,
        wireLimits: DatabaseWireLimits = .default
    ) {
        self.sourceResolver = sourceResolver
        self.wireLimits = wireLimits
    }

    public func execute(
        _ request: GraphAlgorithmOperation.Request,
        context: DatabaseOperationContext
    ) async throws -> GraphAlgorithmOperation.Response {
        let kind = paginationKind(for: request.invocation)
        let requestFingerprint = try requestFingerprint(for: request)
        let cursor = try request.page.continuation.map {
            try DatabaseGraphAlgorithmPageCursor.decode($0, limits: wireLimits)
        }

        guard cursor?.kind == nil || cursor?.kind == kind else {
            throw DatabaseGraphAlgorithmError.invalidContinuation
        }
        guard cursor?.requestFingerprint == nil
                || cursor?.requestFingerprint == requestFingerprint else {
            throw DatabaseGraphAlgorithmError.continuationDoesNotMatchRequest
        }

        let source = try await sourceResolver.resolve(request.source)
        let fullResponse = try await context.container.transactionExecutor
            .withTransaction(
            configuration: .default,
            clock: context.container.monotonicClock
        ) { transaction in
            // The budget belongs to one transaction attempt. A storage retry must
            // restart both the snapshot and accounting from the same boundary.
            let workBudget = GraphAlgorithmWorkBudget(
                maximumWorkUnits: request.budget.maximumWorkUnits
            )
            return try await executeFull(
                request.invocation,
                source: source,
                snapshot: GraphReadSnapshot(
                    transaction: transaction,
                    monotonicClock: context.container.monotonicClock,
                    workBudget: workBudget
                ),
                workBudget: workBudget,
                requestFingerprint: requestFingerprint
            )
        }
        let resultFingerprint = try DatabaseGraphAlgorithmResultFingerprint.compute(
            fullResponse,
            limits: wireLimits
        )

        guard cursor?.resultFingerprint == nil
                || cursor?.resultFingerprint == resultFingerprint else {
            throw DatabaseGraphAlgorithmError.continuationSnapshotChanged
        }

        return try page(
            fullResponse,
            offset: cursor?.offset ?? 0,
            limit: request.page.limit,
            requestFingerprint: requestFingerprint,
            resultFingerprint: resultFingerprint
        )
    }
}
