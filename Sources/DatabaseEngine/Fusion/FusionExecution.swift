import DatabaseKit
import StorageKit

/// One authorized prepared Fusion plan bound to one active read session.
///
/// Construction is centralized here so the executor cannot receive or combine
/// an unrelated query, context, transaction, schema generation, or work meter.
struct FusionExecution: Sendable {
    let plan: FusionPreparedPlan
    let source: FusionSource
    let options: ReadExecutionContext
    let maximumResultCount: Int?

    private let session: DatabaseReadSession
    private let preparedQueryGraph: FusionPreparedQueryGraph
    private let authorization: DatabaseReadAuthorization

    private init(
        plan: FusionPreparedPlan,
        source: FusionSource,
        options: ReadExecutionContext,
        maximumResultCount: Int?,
        session: DatabaseReadSession,
        preparedQueryGraph: FusionPreparedQueryGraph,
        authorization: DatabaseReadAuthorization
    ) {
        self.plan = plan
        self.source = source
        self.options = options
        self.maximumResultCount = maximumResultCount
        self.session = session
        self.preparedQueryGraph = preparedQueryGraph
        self.authorization = authorization
    }

    static func make(
        query: SelectQuery,
        entry: FusionPreparedQueryGraph.Entry,
        graph: FusionPreparedQueryGraph,
        session: DatabaseReadSession,
        options: ReadExecutionContext
    ) throws -> FusionExecution {
        guard let authorization = graph.authorization else {
            throw FusionExecutionError.executionContractViolation
        }
        let listAuthorizationRequirement = try DatabaseReadPolicy
            .listRequirement(
                entityName: entry.plan.entity.name,
                selectQuery: query
            )
        guard listAuthorizationRequirement
                == entry.plan.listAuthorizationRequirement,
              authorization.covers(
                listRequirement: listAuthorizationRequirement
              ) else {
            throw FusionExecutionError.executionContractViolation
        }
        try session.validatePreparedExecution(
            authorization: authorization,
            workMeter: options.workMeter
        )
        return FusionExecution(
            plan: entry.plan,
            source: entry.source,
            options: options,
            maximumResultCount: try scoringOutputPrefixLimit(query),
            session: session,
            preparedQueryGraph: graph,
            authorization: authorization
        )
    }

    var wallClock: any WallClock { session.wallClock }

    func executeRelationalRows(
        _ query: SelectQuery
    ) async throws -> CanonicalRetainedQueryResponse {
        try await session.executeFusionRelationalRows(
            query,
            options: options,
            preparedFusionGraph: preparedQueryGraph,
            authorization: authorization,
            listAuthorizationRequirement:
                plan.listAuthorizationRequirement
        )
    }

    func executeCandidateRelationalRows(
        _ candidates: FusionCandidateDomain,
        query: SelectQuery
    ) async throws -> CanonicalRetainedQueryResponse {
        try await session.executeFusionCandidateRelationalRows(
            candidates,
            query: query,
            options: options,
            preparedFusionGraph: preparedQueryGraph,
            authorization: authorization
        )
    }

    func readableIndex(
        descriptor: IndexDescriptor,
        entity: Schema.Entity,
        partitions: FieldObject
    ) async throws -> ReadableIndex? {
        try await session.readableFusionIndex(
            descriptor: descriptor,
            entity: entity,
            partitions: partitions,
            authorization: authorization
        )
    }

    func withIndexReadLease<Result: Sendable>(
        index: ReadableIndex,
        _ operation: @Sendable (
            FusionIndexReadSession
        ) async throws -> Result
    ) async throws -> Result {
        try await session.withFusionIndexReadLease(
            index: index,
            snapshot: CanonicalReadExecution.resolve(
                requested: options.consistency,
                default: .serializable
            ).consistency == .snapshot,
            workMeter: options.workMeter,
            operation
        )
    }

    func materializeCandidates(
        _ result: FusionIndexReadResult
    ) async throws -> FusionCandidateDomain {
        let primaryKeys = try Self.makeRetainedPrimaryKeys(
            from: result,
            workMeter: options.workMeter
        )
        let snapshot = CanonicalReadExecution.resolve(
            requested: options.consistency,
            default: .serializable
        ).consistency == .snapshot
        let models = try await session
            .fetchRetainedFusionCandidateModelsPreservingOrder(
                entity: plan.entity,
                primaryKeys: primaryKeys,
                partitions: plan.tableRef.partitions,
                snapshot: snapshot,
                authorization: authorization
            )
        return try FusionCandidateDomain.make(
            models: models,
            primaryKeys: primaryKeys,
            entity: plan.entity,
            workMeter: options.workMeter
        )
    }

    static func makeRetainedPrimaryKeys(
        from result: borrowing FusionIndexReadResult,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedPrimaryKeys {
        var builder = try DatabaseRetainedArrayBuilder<
            DatabaseRetainedPrimaryKey
        >(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseRetainedPrimaryKey.self
            ),
            expectedCount: result.matches.count
        )
        for match in result.matches {
            let admission = try builder.prepareAppend(
                footprint: DatabaseIntermediateFootprint(rows: 1),
                at: .storageRow
            )
            let primaryKey = try makeRetainedPrimaryKey(
                match.primaryKey,
                workMeter: workMeter
            )
            builder.append(primaryKey, using: admission)
        }
        return try DatabaseRetainedPrimaryKeys(buffer: builder.finish())
    }

    private static func makeRetainedPrimaryKey(
        _ packedKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedPrimaryKey {
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(packedKey.count),
            at: .storageRow
        )
        do {
            let retainedKey = try DatabaseRetainedByteString.make(
                packedKey,
                reservation: reservation,
                at: .storageRow
            )
            let tuple = try Tuple(packed: retainedKey) {
                additionalByteCount in
                try reservation.reserveAdditional(
                    bytes: UInt64(additionalByteCount),
                    at: .storageRow
                )
            }
            return DatabaseRetainedPrimaryKey(
                value: tuple,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private static func scoringOutputPrefixLimit(
        _ query: SelectQuery
    ) throws -> Int? {
        guard let limit = query.limit,
              query.filter == nil,
              query.groupBy == nil,
              query.having == nil,
              query.orderBy?.isEmpty != false,
              !query.distinct,
              !query.reduced else {
            return nil
        }
        let offset = query.offset ?? 0
        let (prefix, overflow) = limit.addingReportingOverflow(offset)
        guard !overflow else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(
                name: "limit + offset",
                value: UInt64.max
            )
        }
        guard let result = Int(exactly: prefix) else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(
                name: "limit + offset",
                value: prefix
            )
        }
        return result
    }
}
