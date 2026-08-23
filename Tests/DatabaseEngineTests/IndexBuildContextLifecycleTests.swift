import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import ScalarIndex
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

private actor IndexBuildOperationSuspension {
    private var started = false
    private var released = false
    private var startWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func suspendAfterStarting() async {
        started = true
        let waiters = startWaiters
        startWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters { waiter.resume() }
        guard !released else { return }
        await withCheckedContinuation { continuation in
            releaseWaiters.append(continuation)
        }
    }

    func waitUntilStarted() async {
        guard !started else { return }
        await withCheckedContinuation { continuation in
            startWaiters.append(continuation)
        }
    }

    func release() {
        released = true
        let waiters = releaseWaiters
        releaseWaiters.removeAll(keepingCapacity: false)
        for waiter in waiters { waiter.resume() }
    }
}

private actor IndexBuildContextObservation {
    private var escapedContext: IndexBuildContext?
    private var operationTask: Task<Void, any Error>?
    private var sessionFinished = false

    func store(context: IndexBuildContext) {
        escapedContext = context
    }

    func context() -> IndexBuildContext? { escapedContext }

    func store(task: Task<Void, any Error>) {
        operationTask = task
    }

    func task() -> Task<Void, any Error>? { operationTask }

    func markSessionFinished() { sessionFinished = true }

    func didFinishSession() -> Bool { sessionFinished }
}

@Suite("Index build context lifecycle", .heartbeat)
struct IndexBuildContextLifecycleTests {
    @Test(
        "A copied custom-build context is revoked after its callback",
        .timeLimit(.minutes(1))
    )
    func copiedContextIsRevoked() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let observation = IndexBuildContextObservation()

        try await fixture.context.withDataOperation {
            let buildContext = try fixture.makeBuildContext()
            try await buildContext.withCustomStrategySession { context in
                await observation.store(context: context)
                try await context.withIndexTransaction { access in
                    try access.setValue(
                        [0x01],
                        for: context.indexSubspace.pack(Tuple("entry"))
                    )
                }
            }

            let escaped = try #require(await observation.context())
            await #expect(throws: IndexBuildContextError.sessionClosed) {
                try await escaped.withIndexTransaction { _ in () }
            }
        }
    }

    @Test(
        "Session close drains an admitted transaction before returning",
        .timeLimit(.minutes(1))
    )
    func closeDrainsAdmittedTransaction() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }
        let observation = IndexBuildContextObservation()
        let suspension = IndexBuildOperationSuspension()

        try await fixture.context.withDataOperation {
            let buildContext = try fixture.makeBuildContext()
            let sessionTask = Task {
                try await buildContext.withCustomStrategySession { context in
                    await observation.store(context: context)
                    let operationTask = Task {
                        try await context.withIndexTransaction { _ in
                            await suspension.suspendAfterStarting()
                        }
                    }
                    await observation.store(task: operationTask)
                    await suspension.waitUntilStarted()
                }
                await observation.markSessionFinished()
            }

            await suspension.waitUntilStarted()
            for _ in 0..<20 { await Task.yield() }
            #expect(!(await observation.didFinishSession()))

            await suspension.release()
            try await sessionTask.value
            try await #require(await observation.task()).value
            #expect(await observation.didFinishSession())

            let escaped = try #require(await observation.context())
            await #expect(throws: IndexBuildContextError.sessionClosed) {
                try await escaped.withIndexTransaction { _ in () }
            }
        }
    }

    @Test(
        "Item pages stop before their materialized byte bound",
        .timeLimit(.minutes(1))
    )
    func itemPagesRespectMaterializedByteBound() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }

        try await fixture.context.withDataOperation {
            let buildContext = try fixture.makeBuildContext()
            try await buildContext.withTransaction { transaction in
                let storage = try fixture.makeItemStorageWriter(
                    transaction: transaction
                )
                try await storage.write(
                    ByteString(repeating: 0x41, count: 1_024),
                    for: buildContext.itemSubspace.pack(Tuple("first"))
                )
                try await storage.write(
                    ByteString(repeating: 0x42, count: 1_024),
                    for: buildContext.itemSubspace.pack(Tuple("second"))
                )
            }

            do {
                _ = try await buildContext.readItems(
                    limit: 2,
                    maximumBytes: 1_500
                )
                Issue.record("Expected the decode peak to exceed the page bound")
            } catch IndexBuildContextError.itemExceedsPageByteLimit(
                let actual,
                let maximum
            ) {
                #expect(actual > maximum)
                #expect(maximum == 1_500)
            }

            let pageByteLimit = 2_500
            let firstPage = try await buildContext.readItems(
                limit: 2,
                maximumBytes: pageByteLimit
            )
            #expect(firstPage.items.count == 1)
            #expect(firstPage.materializedByteCount <= pageByteLimit)
            let continuation = try #require(firstPage.continuation)

            let secondPage = try await buildContext.readItems(
                after: continuation,
                limit: 2,
                maximumBytes: pageByteLimit
            )
            #expect(secondPage.items.count == 1)
            #expect(secondPage.continuation == nil)
            #expect(secondPage.materializedByteCount <= pageByteLimit)
        }
    }

    @Test(
        "Small inline pages charge complete backing owners",
        .timeLimit(.minutes(1))
    )
    func inlinePagesChargeCompleteBackingOwners() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }

        try await fixture.context.withDataOperation {
            let buildContext = try fixture.makeBuildContext()
            try await buildContext.withTransaction { transaction in
                let storage = try fixture.makeItemStorageWriter(
                    transaction: transaction
                )
                for index in 0..<64 {
                    try await storage.write(
                        ByteString([UInt8(index)]),
                        for: buildContext.itemSubspace.pack(
                            Tuple(Int32(index))
                        )
                    )
                }
            }

            let firstPage = try await buildContext.readItems(
                limit: 16,
                maximumBytes: IndexBuildContext.maximumPageBytes
            )
            let retainedContentByteCount = try firstPage.items.reduce(0) {
                total,
                item in
                let primaryKeyBytes = try #require(
                    item.primaryKey.retainedByteCount
                )
                let payloadBytes = try #require(item.payload.retainedByteCount)
                return total + primaryKeyBytes + payloadBytes
            }
            let firstPageByteCount = retainedContentByteCount
                + MemoryLayout<[IndexBuildItem]>.stride
                + firstPage.items.capacity
                    * MemoryLayout<IndexBuildItem>.stride

            #expect(firstPage.items.count == 16)
            #expect(firstPage.materializedByteCount == firstPageByteCount)
            #expect(firstPage.continuation != nil)

            let exactEndPage = try await buildContext.readItems(
                limit: 64,
                maximumBytes: IndexBuildContext.maximumPageBytes
            )
            #expect(exactEndPage.items.count == 64)
            #expect(exactEndPage.continuation == nil)

            let page = try await buildContext.readItems(
                limit: 64,
                maximumBytes: firstPageByteCount
            )
            #expect(!page.items.isEmpty)
            #expect(page.items.count < firstPage.items.count)
            #expect(page.materializedByteCount <= firstPageByteCount)
            #expect(page.continuation != nil)
        }
    }

    @Test("Invalid item-page byte bounds fail explicitly")
    func invalidItemPageByteBoundFails() async throws {
        let fixture = try await makeFixture()
        defer { await fixture.container.shutdown() }

        try await fixture.context.withDataOperation {
            let buildContext = try fixture.makeBuildContext()
            await #expect(
                throws: IndexBuildContextError.invalidPageByteLimit(0)
            ) {
                _ = try await buildContext.readItems(
                    limit: 1,
                    maximumBytes: 0
                )
            }
        }
    }

    private func makeFixture() async throws -> IndexBuildContextFixture {
        let index = try PlayerIdentifierIndexDefinition.make(
            name: "index_build_context_lifecycle"
        )
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: Player.self,
                    including: [index.descriptor]
                )
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: InMemoryEngine()),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "index-build-context-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        Player.self,
                        including: [index.descriptor]
                    )
                ]
            ),
            security: .testingDisabled
        )
        return IndexBuildContextFixture(
            container: container,
            context: container.testBaseContext(),
            index: index
        )
    }
}

private struct IndexBuildContextFixture: Sendable {
    let container: DBContainer
    let context: DatabaseContext
    let index: ResolvedIndex

    func makeBuildContext() throws -> IndexBuildContext {
        let root = try buildRoot()
        return try IndexBuildContext(
            authority: .databaseContext(context),
            container: container,
            itemSubspace: root.subspace("items").subspace(Player.persistableType),
            blobsSubspace: root.subspace("blobs"),
            indexSubspace: root.subspace("indexes"),
            itemType: Player.persistableType,
            index: index
        )
    }

    func makeItemStorageWriter(
        transaction: any TransactionAccess
    ) throws -> ItemStorageWriter {
        container.itemStorageFactory.makeWriter(
            transaction: transaction,
            blobsSubspace: try buildRoot().subspace("blobs")
        )
    }

    func makeItemStorageReader(
        transaction: any TransactionReadAccess
    ) throws -> ItemStorageReader {
        container.itemStorageFactory.makeReader(
            transaction: transaction,
            blobsSubspace: try buildRoot().subspace("blobs")
        )
    }

    private func buildRoot() throws -> Subspace {
        try context.operationDataRoot().subspace("index-build-context")
    }
}
