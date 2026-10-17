#if !os(WASI)
@_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import StorageKit
import TestSupport
import Testing

@Persistable
private struct S12StagingEntity {
    #Directory<S12StagingEntity>("s12-staging")
    var id: String = ""
    var value: String = ""
}

private enum S12StagingFailure: Error, Equatable {
    case rejected
}

@Suite("S12 transaction-owned SQL catalog staging", .timeLimit(.minutes(1)))
struct S12SchemaStagingTests {
    @Test("SC01 candidate names are request-local until authoritative commit")
    func candidateVisibilityAndCommit() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "candidate_items")
        let meter = makeMeter()
        let result = try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: meter) { transaction in
            let result = try await transaction.stageSQLCatalog(catalog,
                expectedGeneration: old.generation, idempotencyKey: "visibility")
            #expect(container.acquirePublishedSchemaLease().sqlCatalog.catalog == old.sqlCatalog.catalog)
            try await transaction.withStatementScope {
                let candidate = container.acquireActiveSchemaLease()
                #expect(candidate.sqlCatalog.catalog == catalog)
                #expect(candidate.schema == old.schema)
                #expect(candidate.indexPhysicalFingerprint == old.indexPhysicalFingerprint)
                #expect(candidate.executionRuntimeFingerprint != old.executionRuntimeFingerprint)
                let relation = try candidate.sqlCatalog.resolveRelation(name: "candidate_items", namespace: "public",
                    searchPath: .default, currentUser: nil)
                #expect(relation.entity.name == S12StagingEntity.persistableType)
            }
            #expect(meter.retainedIntermediateBytes > 0)
            return result
        }
        #expect(container.sqlCatalog == catalog)
        #expect(container.schemaGeneration == result.generation)
        #expect(old.sqlCatalog.catalog != catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == catalog)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("SC02 nested rollback restores candidate and idempotency while releasing abandoned claims")
    func nestedRollbackRestoresCandidate() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let first = try renamed(old.sqlCatalog.catalog, to: "first_candidate")
        let discarded = try renamed(first, to: "discarded_candidate_with_more_bytes")
        let final = try renamed(first, to: "final_candidate")
        let meter = makeMeter()
        try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: meter) { transaction in
            let firstResult = try await transaction.stageSQLCatalog(first,
                expectedGeneration: old.generation, idempotencyKey: "first")
            let marker = try await transaction.createCheckpoint()
            let second = try await transaction.stageSQLCatalog(discarded,
                expectedGeneration: firstResult.generation, idempotencyKey: "reusable")
            let nested = try await transaction.createCheckpoint()
            let before = meter.retainedIntermediateBytes
            _ = try await transaction.stageSQLCatalog(final,
                expectedGeneration: second.generation, idempotencyKey: "discarded-final")
            #expect(meter.retainedIntermediateBytes > before)
            try await transaction.rollback(to: nested)
            #expect(meter.retainedIntermediateBytes == before)
            try await transaction.withStatementScope {
                #expect(container.sqlCatalog == discarded)
            }
            try await transaction.rollback(to: marker)
            #expect(meter.retainedIntermediateBytes < before)
            try await transaction.withStatementScope {
                #expect(container.sqlCatalog == first)
            }
            _ = try await transaction.stageSQLCatalog(final,
                expectedGeneration: firstResult.generation, idempotencyKey: "reusable")
            try await transaction.release(marker)
        }
        #expect(container.sqlCatalog == final)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == final)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("SC03 callback failure rolls back the complete staged aggregate")
    func wholeRollbackDiscardsCandidate() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "rolled_back")
        let meter = makeMeter()
        await #expect(throws: S12StagingFailure.rejected) {
            try await container.testBaseContext().withCheckpointTransaction(
                checkpointLimits: limits(), workMeter: meter) { transaction in
                _ = try await transaction.stageSQLCatalog(catalog,
                    expectedGeneration: old.generation, idempotencyKey: "whole-rollback")
                throw S12StagingFailure.rejected
            }
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == old.sqlCatalog.catalog)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("SC03 failed outer commit does not publish")
    func failedCommitDoesNotPublish() async throws {
        let control = StorageTransactionControl()
        let container = try await makeContainer(engine: ControlledStorageEngine(base: InMemoryEngine(), control: control))
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "failed_commit")
        let failure = StorageError(code: .backendFailure, operation: .commit, backend: .inMemory,
            message: "Injected staged aggregate commit failure")
        let meter = makeMeter()
        control.failNextMutatingCommit(with: failure)
        await #expect(throws: failure) {
            try await container.testBaseContext().withCheckpointTransaction(
                configuration: TransactionConfiguration(maximumAttempts: 1),
                checkpointLimits: limits(), workMeter: meter) { transaction in
                try await transaction.stageSQLCatalog(catalog,
                    expectedGeneration: old.generation, idempotencyKey: "failed-commit")
            }
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == old.sqlCatalog.catalog)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("SC03 unknown commit needs idempotent confirmation before publication")
    func unknownCommitDoesNotPublish() async throws {
        let engine = CommitOutcomeUnknownEngine()
        let container = try await makeContainer(engine: engine)
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "unknown_commit")
        let meter = makeMeter()
        engine.reportNextCommitAsUnknown()
        do {
            _ = try await container.testBaseContext().withCheckpointTransaction(
                checkpointLimits: limits(), workMeter: meter) { transaction in
                try await transaction.stageSQLCatalog(catalog,
                    expectedGeneration: old.generation, idempotencyKey: "unknown-commit")
            }
            Issue.record("Expected an unknown outer commit outcome")
        } catch let error as StorageError {
            #expect(error.code == .commitUnknownResult)
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == catalog)
        #expect(meter.retainedIntermediateBytes == 0)
        _ = try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: makeMeter()) { transaction in
            try await transaction.stageSQLCatalog(catalog,
                expectedGeneration: old.generation, idempotencyKey: "unknown-commit")
        }
        #expect(container.sqlCatalog == catalog)
    }

    @Test("SC03 cancellation before commit discards the candidate")
    func cancellationDoesNotPublish() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "cancelled_candidate")
        let meter = makeMeter()
        let task = Task {
            try await container.testBaseContext().withCheckpointTransaction(
                checkpointLimits: limits(), workMeter: meter) { transaction in
                _ = try await transaction.stageSQLCatalog(catalog,
                    expectedGeneration: old.generation, idempotencyKey: "cancelled")
                withUnsafeCurrentTask { $0?.cancel() }
                try Task.checkCancellation()
            }
        }
        await #expect(throws: CancellationError.self) { try await task.value }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == old.sqlCatalog.catalog)
        #expect(meter.retainedIntermediateBytes == 0)
    }

    @Test("SC04 stale generation fails and checkpoint recovery restores the attempt")
    func staleGenerationFails() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "stale_candidate")
        try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: makeMeter()) { transaction in
            let marker = try await transaction.createCheckpoint()
            await #expect(throws: DatabaseSchemaPublicationError.generationConflict(
                expected: old.generation + 1, actual: old.generation)) {
                try await transaction.stageSQLCatalog(catalog,
                    expectedGeneration: old.generation + 1, idempotencyKey: "stale")
            }
            try await transaction.rollback(to: marker)
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == old.sqlCatalog.catalog)
    }

    @Test("SC04 an active transition is checked inside the staging transaction")
    func transitionGuardUsesCallerTransaction() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "blocked_transition")
        try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: makeMeter()) { transaction in
            let marker = try await transaction.createCheckpoint()
            try transaction.executionStorageAccess.setValue(ByteString([1]), for:
                container.metadataSubspace.subspace("schema").subspace("applications").pack(Tuple("active")))
            await #expect(throws: DatabaseSchemaPublicationError.transitionInProgress) {
                try await transaction.stageSQLCatalog(catalog,
                    expectedGeneration: old.generation, idempotencyKey: "transition")
            }
            try await transaction.rollback(to: marker)
            _ = try await transaction.stageSQLCatalog(catalog,
                expectedGeneration: old.generation, idempotencyKey: "transition")
        }
        #expect(container.sqlCatalog == catalog)
    }

    @Test("SC04 compiled relations cannot be removed through staging")
    func compiledProtectionSurvivesStaging() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let relation = try #require(old.sqlCatalog.catalog.relations.first)
        let catalog = try SQLCatalog(namespaces: old.sqlCatalog.catalog.namespaces, relations: [])
        await #expect(throws: SQLCatalogBindingError.protectedRelationRemoved(relation.id)) {
            try await container.testBaseContext().withCheckpointTransaction(
                checkpointLimits: limits(), workMeter: makeMeter()) { transaction in
                try await transaction.stageSQLCatalog(catalog,
                    expectedGeneration: old.generation, idempotencyKey: "protected")
            }
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == old.sqlCatalog.catalog)
    }

    @Test("SC04 old idempotent replay leaves a newer candidate current")
    func oldReplayPreservesCurrentCandidate() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let first = try renamed(old.sqlCatalog.catalog, to: "recorded_candidate")
        let final = try renamed(first, to: "newest_candidate")
        try await container.testBaseContext().withCheckpointTransaction(
            checkpointLimits: limits(), workMeter: makeMeter()) { transaction in
            let firstResult = try await transaction.stageSQLCatalog(first,
                expectedGeneration: old.generation, idempotencyKey: "replay")
            _ = try await transaction.stageSQLCatalog(final,
                expectedGeneration: firstResult.generation, idempotencyKey: "latest")
            let replay = try await transaction.stageSQLCatalog(first,
                expectedGeneration: old.generation, idempotencyKey: "replay")
            #expect(replay.generation == firstResult.generation)
            try await transaction.withStatementScope { #expect(container.sqlCatalog == final) }
        }
        #expect(container.sqlCatalog == final)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == final)
    }

    @Test("Candidate budget failure happens before aggregate writes and releases its claim")
    func candidateBudgetRejectsBeforeWrites() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "over_budget")
        let meter = makeMeter(bytes: 1)
        await #expect(throws: DatabaseWorkLimitError.self) {
            try await StorageTransactionExecutor(engine: aggregateEngine(container)).withTransaction(
                configuration: .batch, clock: TestProcessMonotonicClock()) { transaction in
                try await container.stageSQLCatalog(catalog, expectedGeneration: old.generation,
                    idempotencyKey: "budget", authorization: TestBaseEnvironment.authorization,
                    sourceLease: old, transaction: transaction, workMeter: meter)
            }
        }
        #expect(meter.retainedIntermediateBytes == 0)
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == old.sqlCatalog.catalog)
    }

    @Test("An unrelated storage domain cannot stage aggregate metadata")
    func foreignDomainFails() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let other = InMemoryEngine()
        defer { other.requestShutdown(); await other.waitUntilShutdown() }
        let old = container.acquirePublishedSchemaLease()
        let catalog = try renamed(old.sqlCatalog.catalog, to: "foreign_domain")
        await #expect(throws: StorageError.self) {
            try await StorageTransactionExecutor(engine: other).withTransaction(
                configuration: .batch, clock: TestProcessMonotonicClock()) { access in
                try await container.stageSQLCatalog(catalog, expectedGeneration: old.generation,
                    idempotencyKey: "foreign", authorization: TestBaseEnvironment.authorization,
                    sourceLease: old, transaction: access, workMeter: makeMeter())
            }
        }
        #expect(container.sqlCatalog == old.sqlCatalog.catalog)
        let persisted = try await persistedCatalog(container)
        #expect(persisted?.catalog == old.sqlCatalog.catalog)
    }

    private func limits() throws -> TransactionCheckpointLimits {
        try TransactionCheckpointLimits(maximumEntries: 10_000, maximumDepth: 16,
            maximumRetainedBytes: 4 * 1_024 * 1_024,
            maximumWorkUnits: 100_000, maximumReadPageRows: 128)
    }

    private func makeMeter(bytes: UInt64 = 4 * 1_024 * 1_024) -> DatabaseWorkMeter {
        DatabaseWorkMeter(budget: ExecutionBudget(maximumIntermediateBytes: bytes),
            monotonicClock: TestProcessMonotonicClock())
    }

    private func makeContainer(engine: any StorageEngine = InMemoryEngine()) async throws -> DBContainer {
        let schema = try Schema(entities: [S12StagingEntity.schemaEntity])
        return try await DBContainer.open(for: schema, configuration: .testing(storageEngine: engine),
            runtimeConfiguration: DatabaseFrameworkRuntime.configuration(
                executionIdentity: .init(identifier: "s12-staging", revision: 1),
                entityRuntimes: [DatabaseFrameworkRuntime.entity(S12StagingEntity.self)]),
            security: .testingDisabled)
    }

    private func renamed(_ catalog: SQLCatalog, to name: String) throws -> SQLCatalog {
        let relation = try #require(catalog.relations.first)
        return try SQLCatalog(namespaces: catalog.namespaces, relations: [SQLRelation(
            id: relation.id, namespaceID: relation.namespaceID, name: name, entityName: relation.entityName,
            columns: relation.columns, primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
            primaryKeyConstraint: relation.primaryKeyConstraint, constraints: relation.constraints)])
    }

    private func persistedCatalog(_ container: DBContainer) async throws -> SQLCatalogStoredState? {
        try await StorageTransactionExecutor(engine: aggregateEngine(container)).withTransaction(
            configuration: .readOnly, clock: TestProcessMonotonicClock()) { transaction in
            try await SQLCatalogStore(root: container.metadataSubspace).load(transaction: transaction)
        }
    }

    private func aggregateEngine(_ container: DBContainer) -> any StorageEngine {
        #if DATABASE_MULTI_BASE
        container.storageTopology.controlDomain.engine
        #else
        container.engine
        #endif
    }
}
#endif
