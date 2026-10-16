import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import Synchronization
import TestSupport
import Testing
@_spi(DatabaseExecution) @_spi(Testing) @testable import DatabaseEngine

@Suite("SQL temporal clock ownership", .timeLimit(.minutes(1)))
struct SQLTemporalClockScopeTests {
    @Test("Selected clock values are stable within a statement and fresh between public entries")
    func publicStatementSnapshotsAndLiveClock() async throws {
        let clock = AdvancingSQLScopeClock()
        let container = try await Self.makeContainer(clock: clock)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let first = try await context.query(Self.clockQuery(rows: 2))
        let second = try await context.query(Self.clockQuery(rows: 2))
        #expect(first.rows.count == 2)
        #expect(second.rows.count == 2)
        for response in [first, second] {
            let firstRow = try #require(response.rows.first)
            #expect(firstRow.values[0] == firstRow.values[1])
            #expect(firstRow.values[0] == firstRow.values[2])
            #expect(firstRow.values[0] == firstRow.values[4])
            #expect(response.rows[1].values[0] == firstRow.values[0])
            #expect(response.rows[1].values[2] == firstRow.values[2])
            #expect(response.rows[1].values[4] == firstRow.values[4])
            #expect(try Self.instant(response.rows[1].values[3]) > Self.instant(firstRow.values[3]))
        }
        #expect(try Self.instant(second.rows[0].values[0]) > Self.instant(first.rows[0].values[0]))
        #expect(ActiveSQLTemporalStatement.binding == nil)
        #expect(ActiveDatabaseTransactionContext.binding == nil)
    }

    @Test("Explicit transaction clocks survive body retry while each selected statement gets a new instant")
    func transactionSnapshotSurvivesRetry() async throws {
        let clock = AdvancingSQLScopeClock()
        let container = try await Self.makeContainer(clock: clock)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let attempts = SQLTransactionInstantProbe()
        let key = ByteString(utf8: "sql-temporal-retry")
        let committed = try await context.withTransaction(configuration: TransactionConfiguration(
            maximumAttempts: 2, maxRetryDelay: 0, initialRetryDelay: 0
        )) { transaction in
            let attempt = attempts.record(transaction.transactionInstant)
            let actorEnvironment = transaction.makeSQLTemporalEnvironment()
            let actorStatement = try await context.withSQLTemporalStatement(environment: actorEnvironment) {
                try await context.query(Self.clockQuery())
            }
            #expect(try Self.instant(actorStatement.rows[0].values[0]) == transaction.transactionInstant)
            #expect(try Self.instant(actorStatement.rows[0].values[2]) == actorEnvironment.statementInstant)
            let first = try await context.query(Self.clockQuery())
            let second = try await context.query(Self.clockQuery())
            #expect(try Self.instant(first.rows[0].values[0]) == transaction.transactionInstant)
            #expect(try Self.instant(second.rows[0].values[0]) == transaction.transactionInstant)
            #expect(try Self.instant(first.rows[0].values[4]) == transaction.transactionInstant)
            #expect(try Self.instant(second.rows[0].values[4]) == transaction.transactionInstant)
            #expect(try Self.instant(first.rows[0].values[1]) == transaction.transactionInstant)
            #expect(try Self.instant(second.rows[0].values[2]) > Self.instant(first.rows[0].values[2]))
            #expect(try Self.instant(second.rows[0].values[3]) > Self.instant(first.rows[0].values[3]))
            try transaction.storageAccess.setValue(ByteString([UInt8(attempt)]), for: key)
            if attempt == 1 { throw StorageError.transactionConflict }
            return transaction.transactionInstant
        }
        let observed = attempts.values
        #expect(observed == [committed, committed])
        let stored = try await context.withStorageAccess(requiredAccess: .read) { transaction in
            try await transaction.getValue(for: key)
        }
        #expect(stored == ByteString([2]))
        #expect(ActiveDatabaseTransactionContext.binding == nil)
    }

    @Test("Inherited statement time survives read attenuation and authorized session derivation")
    func inheritedReadSessionEnvironment() async throws {
        let clock = AdvancingSQLScopeClock()
        let container = try await Self.makeContainer(clock: clock)
        defer { await container.shutdown() }
        let context = container.testBaseContext()
        let environment = context.makeSQLTemporalEnvironment()
        let execution = ReadExecutionContext(monotonicClock: container.monotonicClock,
            temporalEnvironment: environment)
        let derived = execution.withoutExternalPageWindow()
        try await context.withSQLTemporalStatement(environment: environment) {
            try await context.withTransaction { transaction in
                #expect(transaction.transactionInstant == environment.transactionInstant)
                try await context.withReadSnapshot(workMeter: execution.workMeter) { snapshot in
                    #expect(ActiveDatabaseTransactionContext.binding?.databaseTransaction == nil)
                    #expect(ActiveDatabaseTransactionContext.binding?.transactionInstant == environment.transactionInstant)
                    let session = try snapshot.session.admittingRDFDatasetRead()
                    #expect(session.temporalEnvironment.transactionInstant == environment.transactionInstant)
                    #expect(session.temporalEnvironment.statementInstant == environment.statementInstant)
                    let response = try await session.executeCanonical(Self.clockQuery(), execution: derived)
                    #expect(try Self.instant(response.rows[0].values[0]) == environment.transactionInstant)
                    #expect(try Self.instant(response.rows[0].values[2]) == environment.statementInstant)
                    let nested = try await context.query(Self.clockQuery())
                    #expect(try Self.instant(nested.rows[0].values[2]) == environment.statementInstant)
                }
            }
        }
        #expect(ActiveSQLTemporalStatement.binding == nil)
    }

    @Test("Statement bindings are container-local and are removed on operation failure")
    func containerIsolationAndFailureCleanup() async throws {
        let first = try await Self.makeContainer(clock: AdvancingSQLScopeClock(start: 1_704_067_200))
        defer { await first.shutdown() }
        let second = try await Self.makeContainer(clock: AdvancingSQLScopeClock(start: 1_893_456_000))
        defer { await second.shutdown() }
        let firstContext = first.testBaseContext()
        let secondContext = second.testBaseContext()
        let environment = firstContext.makeSQLTemporalEnvironment()
        do {
            try await firstContext.withSQLTemporalStatement(environment: environment) {
                let inherited = try await firstContext.query(Self.clockQuery())
                #expect(try Self.instant(inherited.rows[0].values[2]) == environment.statementInstant)
                let foreign = try await secondContext.query(Self.clockQuery())
                #expect(try Self.instant(foreign.rows[0].values[0]) == Self.instant(foreign.rows[0].values[2]))
                #expect(try Self.instant(foreign.rows[0].values[2]) != environment.statementInstant)
                throw ClockScopeFailure.rejected
            }
            Issue.record("Expected the scoped operation to fail")
        } catch ClockScopeFailure.rejected {}
        #expect(ActiveSQLTemporalStatement.binding == nil)
        #expect(ActiveDatabaseTransactionContext.binding == nil)
        let fresh = try await firstContext.query(Self.clockQuery())
        #expect(try Self.instant(fresh.rows[0].values[2]) > environment.statementInstant)
    }

    #if MultiBase
    @Test("Composition members inherit one snapshot clock and supplied zone policy")
    func compositionMemberSnapshotEnvironment() async throws {
        let clock = AdvancingSQLScopeClock()
        let configuration = try SQLTemporalConfiguration(timeZone: .fixed(offsetSeconds: 19_800),
            abbreviations: [.init(name: "Custom", offsetSeconds: .min)],
            limits: .init(maximumAbbreviations: 1, maximumRetainedBytes: 15))
        let container = try await Self.makeContainer(clock: clock, sqlTemporal: configuration)
        defer { await container.shutdown() }
        let firstBaseID = try TestBaseEnvironment.id()
        let secondBaseID = try Base.ID("sql-temporal-second")
        _ = try await container.provisionBase(secondBaseID,
            placementID: Base.Placement.ID("sql-temporal-clock"), initialGrants: [
                Security.Grant(subject: .principal("test-runner"), resource: .base(secondBaseID), access: .all),
            ], expectedRevision: 0)
        let source = try container.session(authorization: TestBaseEnvironment.authorization)
            .composition(bases: [firstBaseID, secondBaseID])
        try await source.withReadSnapshot { snapshot in
            #expect(snapshot.members.count == 2)
            let environment = snapshot.temporalEnvironment
            #expect(environment.transactionInstant == environment.statementInstant)
            var selectedValues: [[QueryValue]] = []
            for member in snapshot.members {
                let sampled = clock.now
                #expect(sampled > environment.statementInstant)
                let meter = DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
                let values = try await source.withMemberReadSession(member, in: snapshot, workMeter: meter) { session in
                    #expect(ActiveDatabaseTransactionContext.binding?.transactionInstant == environment.transactionInstant)
                    #expect(session.temporalEnvironment.transactionInstant == environment.transactionInstant)
                    #expect(session.temporalEnvironment.statementInstant == environment.statementInstant)
                    #expect(try session.temporalEnvironment.configuration.fixedOffset(
                        forAbbreviation: "CUSTOM", workMeter: meter) == .min)
                    let query = SelectQuery(projection: .items([
                        ProjectionItem(.function(FunctionCall(name: "now", arguments: []))),
                        ProjectionItem(.function(FunctionCall(name: "statement_timestamp", arguments: []))),
                        ProjectionItem(.currentValue(kind: .time, precision: nil)),
                    ]), source: .values([[]], columnNames: []))
                    let response = try await session.executeCanonical(query,
                        execution: ReadExecutionContext(monotonicClock: container.monotonicClock, workMeter: meter))
                    let row = try #require(response.rows.first)
                    #expect(try Self.instant(row.values[0]) == environment.transactionInstant)
                    #expect(try Self.instant(row.values[1]) == environment.statementInstant)
                    guard case .timeWithTimeZone(let time) = row.values[2] else {
                        throw ClockScopeFailure.expectedTimestamp
                    }
                    #expect(time.utcOffsetSeconds == 19_800)
                    return row.values
                }
                selectedValues.append(values)
            }
            #expect(selectedValues.count == 2)
            #expect(selectedValues[0] == selectedValues[1])
        }
        #expect(ActiveSQLTemporalStatement.binding == nil)
        #expect(ActiveDatabaseTransactionContext.binding == nil)
    }
    #endif

    private static func clockQuery(rows: Int = 1) -> SelectQuery {
        SelectQuery(projection: .items(["now", "transaction_timestamp", "statement_timestamp", "clock_timestamp"].map {
            ProjectionItem(.function(FunctionCall(name: $0, arguments: [])))
        } + [ProjectionItem(.currentValue(kind: .timestamp, precision: nil))]), source: .values(Array(repeating: [], count: rows), columnNames: []))
    }

    private static func instant(_ value: QueryValue) throws -> Timestamp {
        guard case .primitive(.timestamp(let instant)) = value else {
            throw ClockScopeFailure.expectedTimestamp
        }
        return instant
    }

    private static func makeContainer(clock: AdvancingSQLScopeClock,
        sqlTemporal: SQLTemporalConfiguration = .utc) async throws -> DBContainer {
        let engine = InMemoryEngine()
        let configuration: DBConfiguration
        #if MultiBase
        let domainID = try DatabaseStorageDomain.ID("sql-temporal-clock")
        let placementID = try Base.Placement.ID("sql-temporal-clock")
        let topology = try DatabaseStorageTopology(controlDomainID: domainID,
            domains: [try DatabaseStorageDomain(id: domainID,
                rootPath: testingDatabaseRootPath(databaseIdentifier: "sql-temporal-clock"), storageEngine: engine)],
            placements: [DatabaseStoragePlacement(id: placementID, domainID: domainID)],
            defaultPlacementID: placementID)
        configuration = DBConfiguration(testingName: "sql-temporal-clock", storageTopology: topology,
            monotonicClock: TestProcessMonotonicClock(), wallClock: clock, sqlTemporal: sqlTemporal,
            testingBaseID: try TestBaseEnvironment.id(), testingPrincipal: Principal(identifier: "test-runner", roles: ["test-runner", "admin"]))
        #else
        configuration = try DBConfiguration(storageEngine: engine,
            monotonicClock: TestProcessMonotonicClock(), wallClock: clock, sqlTemporal: sqlTemporal)
        #endif
        return try await DBContainer.open(testing: Schema(entities: []), configuration: configuration,
            runtimeConfiguration: DatabaseRuntimeConfiguration(executionIdentity:
                DatabaseExecutionRuntimeIdentity(identifier: "sql-temporal-clock-tests", revision: 1)),
            security: .testingDisabled)
    }
}

private enum ClockScopeFailure: Error {
    case rejected
    case expectedTimestamp
}

private final class SQLTransactionInstantProbe: Sendable {
    private let storage = Mutex<[Timestamp]>([])

    func record(_ instant: Timestamp) -> Int {
        storage.withLock { values in
            values.append(instant)
            return values.count
        }
    }

    var values: [Timestamp] {
        storage.withLock { $0 }
    }
}

private final class AdvancingSQLScopeClock: WallClock, Sendable {
    private let seconds: Mutex<Int64>

    init(start: Int64 = 1_704_067_200) { seconds = Mutex(start) }

    var now: Timestamp {
        seconds.withLock { value in
            let result = Timestamp(secondsSinceUnixEpoch: value)
            value += 1
            return result
        }
    }
}
