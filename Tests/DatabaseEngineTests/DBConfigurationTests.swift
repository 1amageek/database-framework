import DatabaseKit
import DatabaseRuntime
import StorageKit
import Synchronization
import TestHeartbeat
import TestSupport
import Testing
@testable import DatabaseEngine

@Suite("DBConfiguration ownership", .heartbeat)
struct DBConfigurationOwnershipTests {
    private struct RuntimeIndexConfiguration: IndexRuntimeConfiguration {
        static let kindIdentifier = IndexDefinition
            .vector(dimensions: 1)
            .identifier

        let fieldName = "embedding"
        let entityName = "ConfigurationEntity"
    }

    @Persistable
    struct ConfigurationEntity {
        #Directory<ConfigurationEntity>("configuration_tests", "entities")

        var id: String = ""
    }

    private final class ShutdownRecordingEngine: StorageEngine, Sendable {
        struct Configuration: Sendable {}

        typealias TransactionType = InMemoryTransaction

        private let base = InMemoryEngine()
        private struct ShutdownState: Sendable {
            var requestCount = 0
            var completionCount = 0
        }
        private let shutdownState = Mutex(ShutdownState())
        private let completionGate: ShutdownCompletionGate?

        init(completionGate: ShutdownCompletionGate? = nil) {
            self.completionGate = completionGate
        }

        init(configuration: Configuration) async throws {
            self.completionGate = nil
        }

        var shutdownCount: Int {
            shutdownState.withLock { $0.requestCount }
        }

        var shutdownCompletionCount: Int {
            shutdownState.withLock { $0.completionCount }
        }

        var namespaceResolver: any NamespaceResolver {
            base.namespaceResolver
        }

        var namespaceCatalog: (any NamespaceCatalog)? {
            base.namespaceCatalog
        }

        func createTransaction() throws -> InMemoryTransaction {
            try base.createTransaction()
        }

        func requestShutdown() {
            let isFirstRequest = shutdownState.withLock { state in
                guard state.requestCount == 0 else { return false }
                state.requestCount = 1
                return true
            }
            if isFirstRequest {
                base.requestShutdown()
            }
        }

        func waitUntilShutdown() async {
            requestShutdown()
            if let completionGate {
                await completionGate.waitForRelease()
            }
            await base.waitUntilShutdown()
            shutdownState.withLock { state in
                if state.completionCount == 0 {
                    state.completionCount = 1
                }
            }
        }
    }

    @Test("Configuration stores its runtime policies")
    func storesRuntimePolicies() throws {
        let engine = ShutdownRecordingEngine()
        let configuration = try DBConfiguration.testing(
            name: "test-configuration",
            storageEngine: engine,
            indexConfigurations: [RuntimeIndexConfiguration()]
        )

        #expect(configuration.name == "test-configuration")
        #expect(configuration.indexConfigurations.count == 1)
        #expect(configuration.debugDescription.contains("indexConfigs: 1"))
    }

    @Test("Topology rejects assigning one engine to multiple domains")
    func topologyRejectsDuplicateEngineOwnership() throws {
        let engine = ShutdownRecordingEngine()
        let firstDomainID = try DatabaseStorageDomain.ID("primary")
        let secondDomainID = try DatabaseStorageDomain.ID("secondary")
        let placementID = try Base.Placement.ID("default")
        let firstDomain = try DatabaseStorageDomain(
            id: firstDomainID,
            namespacePath: ["database", "primary"],
            storageEngine: engine
        )
        let secondDomain = try DatabaseStorageDomain(
            id: secondDomainID,
            namespacePath: ["database", "secondary"],
            storageEngine: engine
        )
        let placement = try DatabaseStoragePlacement(
            id: placementID,
            domainID: firstDomainID,
            path: ["bases"]
        )

        #expect(
            throws: DatabaseStorageTopologyError.duplicateStorageEngine(
                first: firstDomainID,
                second: secondDomainID
            )
        ) {
            _ = try DatabaseStorageTopology(
                controlDomainID: firstDomainID,
                domains: [firstDomain, secondDomain],
                placements: [placement],
                defaultPlacementID: placementID
            )
        }
    }

    @Test("Container shutdown releases its engine exactly once")
    func containerShutdownIsExactlyOnce() async throws {
        let engine = ShutdownRecordingEngine()
        let schema = try Schema(
            entities: [try ConfigurationEntity.schemaEntity]
        )
        let runtime = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    ConfigurationEntity.self
                ),
            ]
        )
        let container = try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: runtime,
            security: .testingDisabled,
            initializeIndexes: false
        )

        await container.shutdown()
        await container.shutdown()
        #expect(engine.shutdownCount == 1)
    }

    @Test("Opening failure releases its engine exactly once")
    func openingFailureShutsDownEngine() async throws {
        let engine = ShutdownRecordingEngine()
        let schema = try Schema(
            entities: [try ConfigurationEntity.schemaEntity]
        )
        let runtime = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: []
        )

        do {
            _ = try await DBContainer.open(
                testing: schema,
                configuration: .testing(storageEngine: engine),
                runtimeConfiguration: runtime,
                security: .testingDisabled,
                initializeIndexes: false
            )
            Issue.record("Expected a missing entity runtime to fail")
        } catch let error as DatabaseRuntimeConfigurationError {
            guard case .missingCompiledEntityType(let entityName) = error else {
                Issue.record("Expected a missing entity runtime error")
                return
            }
            #expect(entityName == ConfigurationEntity.persistableType)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }

        #expect(engine.shutdownCount == 1)
    }

    @Test("Container shutdown awaits backend completion")
    func containerShutdownAwaitsBackendCompletion() async throws {
        let completionGate = ShutdownCompletionGate()
        let engine = ShutdownRecordingEngine(completionGate: completionGate)
        let container = try await makeContainer(engine: engine)

        let shutdown = Task {
            await container.shutdown()
        }
        await completionGate.waitUntilObserved()

        #expect(engine.shutdownCount == 1)
        #expect(engine.shutdownCompletionCount == 0)

        completionGate.release()
        await shutdown.value
        #expect(engine.shutdownCompletionCount == 1)
    }

    @Test("Direct transaction cursor keeps shutdown pending")
    func directTransactionCursorKeepsShutdownPending() async throws {
        let engine = ShutdownRecordingEngine()
        let container = try await makeContainer(engine: engine)
        var transaction: (any Transaction)? = try container.engine
            .createOwnedTransaction()
        var cursor = try #require(transaction).rangeCursor(
            from: .firstGreaterOrEqual([]),
            to: .firstGreaterOrEqual([0xff]),
            limit: 0,
            reverse: false,
            snapshot: true,
            streamingMode: .wantAll
        )
        transaction = nil

        let shutdown = Task {
            await container.shutdown()
        }
        try await waitUntilAdmissionCloses(container)
        #expect(engine.shutdownCount == 0)

        try await cursor.finish()
        await shutdown.value
        #expect(engine.shutdownCount == 1)
    }

    @Test("Transaction executor cursor keeps shutdown pending")
    func transactionExecutorCursorKeepsShutdownPending() async throws {
        let engine = ShutdownRecordingEngine()
        let container = try await makeContainer(engine: engine)
        var cursor = try await container.transactionExecutor
            .withTransaction { transaction in
                transaction.rangeCursor(
                    from: .firstGreaterOrEqual([]),
                    to: .firstGreaterOrEqual([0xff]),
                    limit: 0,
                    reverse: false,
                    snapshot: true,
                    streamingMode: .wantAll
                )
            }

        let shutdown = Task {
            await container.shutdown()
        }
        try await waitUntilAdmissionCloses(container)
        #expect(engine.shutdownCount == 0)

        try await cursor.finish()
        await shutdown.value
        #expect(engine.shutdownCount == 1)
    }

    @Test("Namespace operations reject bare and foreign container transactions")
    func namespaceOperationsRequireOwnedTransaction() async throws {
        let firstEngine = ShutdownRecordingEngine()
        let secondEngine = ShutdownRecordingEngine()
        let firstContainer = try await makeContainer(engine: firstEngine)
        let secondContainer = try await makeContainer(engine: secondEngine)
        let resolver = firstContainer.engine.namespaceResolver

        var ownedTransaction: (any Transaction)? = try firstContainer.engine
            .createOwnedTransaction()
        _ = try await resolver.resolveOrCreate(
            path: ["owned"],
            transaction: try #require(ownedTransaction)
        )

        var foreignTransaction: (any Transaction)? = try secondContainer.engine
            .createOwnedTransaction()
        await #expect(throws: StorageError.self) {
            _ = try await resolver.resolveOrCreate(
                path: ["foreign"],
                transaction: try #require(foreignTransaction)
            )
        }

        var bareTransaction: (any Transaction)? = try firstEngine
            .createTransaction()
        await #expect(throws: StorageError.self) {
            _ = try await resolver.resolveOrCreate(
                path: ["bare"],
                transaction: try #require(bareTransaction)
            )
        }

        try await #require(ownedTransaction).cancel()
        try await #require(foreignTransaction).cancel()
        try await #require(bareTransaction).cancel()
        ownedTransaction = nil
        foreignTransaction = nil
        bareTransaction = nil
        await firstContainer.shutdown()
        await secondContainer.shutdown()
    }

    private func makeContainer(
        engine: ShutdownRecordingEngine
    ) async throws -> DBContainer {
        let schema = try Schema(
            entities: [try ConfigurationEntity.schemaEntity]
        )
        let runtime = try DatabaseFrameworkRuntime.configuration(
            entityRuntimes: [
                try DatabaseFrameworkRuntime.entity(
                    ConfigurationEntity.self
                ),
            ]
        )
        return try await DBContainer.open(
            testing: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: runtime,
            security: .testingDisabled,
            initializeIndexes: false
        )
    }

    private func waitUntilAdmissionCloses(
        _ container: DBContainer
    ) async throws {
        for _ in 0..<1_000 {
            do {
                let transaction = try container.engine
                    .createOwnedTransaction()
                try await transaction.cancel()
            } catch let error as DatabaseContainerLifecycleError {
                switch error {
                case .shuttingDown, .shutdown:
                    return
                case .configurationAlreadyUsed, .operationLimitExceeded:
                    throw error
                }
            }
            await Task.yield()
        }
        throw ShutdownObservationError.admissionDidNotClose
    }
}

private enum ShutdownObservationError: Error {
    case admissionDidNotClose
}

private final class ShutdownCompletionGate: Sendable {
    private struct State: Sendable {
        var isObserved = false
        var isReleased = false
        var observationWaiters: [CheckedContinuation<Void, Never>] = []
        var releaseWaiters: [CheckedContinuation<Void, Never>] = []
    }

    private let state = Mutex(State())

    func waitForRelease() async {
        let observationWaiters = state.withLock { state in
            state.isObserved = true
            let waiters = state.observationWaiters
            state.observationWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in observationWaiters {
            waiter.resume()
        }

        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                guard !state.isReleased else { return true }
                state.releaseWaiters.append(continuation)
                return false
            }
            if resumeImmediately {
                continuation.resume()
            }
        }
    }

    func waitUntilObserved() async {
        await withCheckedContinuation { continuation in
            let resumeImmediately = state.withLock { state in
                guard !state.isObserved else { return true }
                state.observationWaiters.append(continuation)
                return false
            }
            if resumeImmediately {
                continuation.resume()
            }
        }
    }

    func release() {
        let releaseWaiters = state.withLock { state in
            guard !state.isReleased else {
                return [CheckedContinuation<Void, Never>]()
            }
            state.isReleased = true
            let waiters = state.releaseWaiters
            state.releaseWaiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in releaseWaiters {
            waiter.resume()
        }
    }
}
