import DatabaseKit
@_spi(DatabaseExecution) import DatabaseEngine
import StorageKit

/// A rewritten SQL SELECT whose dynamically inlined values remain request
/// accounted for exactly as long as the executable query can borrow them.
@_spi(DatabaseExecution)
public struct DatabasePreparedSQLSelect: Sendable {
    private let query: SelectQuery
    private var sqlResolution: SQLReadResolution? = nil
    private let workMeter: DatabaseWorkMeter
    private let parameters: [QueryParameter]
    private let metadataReservation: DatabaseIntermediateReservation?
    private let occurrences: QueryOccurrenceArena?
    private let occurrence: QueryOccurrenceID?
    private let originalOccurrences: QueryOccurrenceArena?
    private let originalOccurrence: QueryOccurrenceID?
    private let occurrenceOrigins: [QueryOccurrenceID: QueryOccurrenceID]
    private let retainedStorage: DatabasePreparedSQLSelectStorage?

    package init(
        query: SelectQuery,
        workMeter: DatabaseWorkMeter,
        parameters: [QueryParameter] = [],
        metadataReservation: DatabaseIntermediateReservation? = nil,
        occurrences: QueryOccurrenceArena? = nil,
        occurrence: QueryOccurrenceID? = nil,
        originalOccurrences: QueryOccurrenceArena? = nil,
        originalOccurrence: QueryOccurrenceID? = nil,
        occurrenceOrigins: [QueryOccurrenceID: QueryOccurrenceID] = [:],
        retainedStorage: DatabasePreparedSQLSelectStorage? = nil
    ) {
        self.query = query
        self.workMeter = workMeter
        self.parameters = parameters
        self.metadataReservation = metadataReservation
        self.occurrences = occurrences
        self.occurrence = occurrence
        self.originalOccurrences = originalOccurrences
        self.originalOccurrence = originalOccurrence
        self.occurrenceOrigins = occurrenceOrigins
        self.retainedStorage = retainedStorage
    }

    package func withSQLResolution(_ resolution: SQLReadResolution) -> Self {
        var result = self
        result.sqlResolution = resolution
        return result
    }

    /// Executes the prepared query without exposing its copyable syntax value.
    /// Dynamic literals therefore cannot outlive the reservation retained by
    /// this owner. Preparation and execution must share one request meter.
    public func execute(
        in session: DatabaseReadSession,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> QueryResponse {
        guard execution.workMeter === workMeter else {
            throw DatabasePreparedSQLSelectError.workMeterMismatch
        }
        if let sqlResolution { try session.validateSQLReadResolution(sqlResolution) }
        let execution = sqlResolution.map { execution.withSQLResolution($0) } ?? execution
        let lifetimeOwner = retainedStorage
        let metadataOwner = metadataReservation
        defer {
            withExtendedLifetime(lifetimeOwner) {}
            withExtendedLifetime(metadataOwner) {}
        }
        do {
            return try await session.executeCanonical(
                query,
                execution: execution,
                graphPartitions: graphPartitions,
                parameters: parameters,
                occurrences: occurrences,
                occurrence: occurrence
            )
        } catch let failure as CanonicalSQLBindingFailure {
            throw originalFailure(failure)
        } catch CanonicalReadError.expressionEvaluation(let cause, let occurrence) {
            let origin = originalOccurrences == nil ? occurrence : occurrence.flatMap { occurrenceOrigins[$0] }
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: origin)
        }
    }

    /// Executes the prepared query for the complete visible result and moves
    /// it into shared ownership.
    ///
    /// Complete staging disables the client-facing page window rather than
    /// paging, so the returned rows are the whole visible result and never a
    /// truncated prefix. The request row budget still applies through the
    /// shared work meter and reports its own typed limit failure. The
    /// returned rows hold the request reservation until their last owner is
    /// released, so a durable query snapshot may count them and emit bounded
    /// pages after the read snapshot that produced them has closed.
    public func stageCompleteRows(
        in session: DatabaseReadSession,
        execution: ReadExecutionContext,
        graphPartitions: FieldObject = FieldObject()
    ) async throws -> DatabaseSharedRetainedQueryRows {
        guard execution.workMeter === workMeter else {
            throw DatabasePreparedSQLSelectError.workMeterMismatch
        }
        if let sqlResolution { try session.validateSQLReadResolution(sqlResolution) }
        let execution = sqlResolution.map { execution.withSQLResolution($0) } ?? execution
        let lifetimeOwner = retainedStorage
        let metadataOwner = metadataReservation
        defer {
            withExtendedLifetime(lifetimeOwner) {}
            withExtendedLifetime(metadataOwner) {}
        }
        let page: DatabaseRetainedQueryPage
        do {
            page = try await session.retainedCanonicalPage(
                query,
                execution: execution.withoutExternalPageWindow(),
                graphPartitions: graphPartitions,
                parameters: parameters,
                occurrences: occurrences,
                occurrence: occurrence
            )
        } catch let failure as CanonicalSQLBindingFailure {
            throw originalFailure(failure)
        } catch CanonicalReadError.expressionEvaluation(let cause, let occurrence) {
            let origin = originalOccurrences == nil ? occurrence : occurrence.flatMap { occurrenceOrigins[$0] }
            throw CanonicalReadError.expressionEvaluation(cause, occurrence: origin)
        }
        guard page.continuation == nil else {
            throw DatabasePreparedSQLSelectError.stagedResultIsIncomplete
        }
        return try DatabaseSharedRetainedQueryRows(
            rows: page.takeRows(),
            at: .resultMaterialization
        )
    }

    private func originalFailure(_ failure: CanonicalSQLBindingFailure) -> CanonicalSQLBindingFailure {
        guard originalOccurrences != nil else { return failure }
        let origin = failure.occurrence.flatMap { occurrenceOrigins[$0] }
        return CanonicalSQLBindingFailure(cause: failure.cause, occurrence: origin, root: originalOccurrence)
    }
}
