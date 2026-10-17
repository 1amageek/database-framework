import DatabaseTypes
import StorageKit
import Testing
@testable import DatabaseEngine

@Suite("Transaction operation propagation", .timeLimit(.minutes(1)))
struct TransactionOperationPropagationTests {
    private enum Failure: Error, Equatable { case requested }

    @Test("Container and attenuated wrappers preserve compound scope and failures")
    func wrappersForwardOperationScopes() async throws {
        let engine = InMemoryEngine()
        let lifecycle = DatabaseStorageLifecycle(storageEngine: engine)
        _ = try lifecycle.claimStorageEngine()
        try lifecycle.finishOpening()
        let lease = try lifecycle.beginOperation()
        let probe = TransactionOperationProbe(transaction: try engine.createTransaction())
        let owned = ContainerTransaction(transaction: probe, operationLease: lease)
        let borrowed = ContainerTransactionAccess(transaction: probe, operationLease: lease)
        let authorized = ReadAuthorizedTransactionAccess.admittedReadAccess(borrowed)
        let read = DatabaseReadTransaction(storageAccess: authorized)
        let wrappers: [any TransactionReadAccess] = [owned, borrowed, authorized, read]
        do {
            try owned.configureCheckpoints(limits: TransactionCheckpointLimits(
                maximumEntries: 64, maximumDepth: 8, maximumRetainedBytes: 65_536,
                maximumWorkUnits: 4_096, maximumReadPageRows: 16))
            let ownedCheckpoints = try #require(owned.checkpoints)
            let borrowedCheckpoints = try #require(borrowed.checkpoints)
            let checkpoint = try await ownedCheckpoints.createCheckpoint()
            let key = ByteString(utf8: "wrapper-checkpoint")
            try borrowed.setValue(ByteString([1]), for: key)
            #expect(try await owned.getValue(for: key) == ByteString([1]))
            try await borrowedCheckpoints.rollback(to: checkpoint)
            #expect(try await owned.getValue(for: key) == nil)
            for wrapper in wrappers {
                let count = probe.operationCount
                try await wrapper.performOperation {
                    #expect(probe.depth == 1)
                }
                #expect(probe.operationCount == count + 1)
                await #expect(throws: Failure.requested) {
                    try await wrapper.performOperation {
                        #expect(probe.depth == 1)
                        throw Failure.requested
                    }
                }
                #expect(probe.depth == 0)
            }
            #expect(authorized.checkpoints == nil)
            try await probe.cancel()
        } catch {
            try await probe.cancel()
            lease.finish()
            await lifecycle.shutdown()
            throw error
        }
        lease.finish()
        await lifecycle.shutdown()
    }

    @Test("Directory binding and layout retain an outer scope across node walks")
    func directoryCompoundsKeepOuterScope() async throws {
        let engine = InMemoryEngine()
        let probe = TransactionOperationProbe(transaction: try engine.createTransaction())
        do {
            let root = try await DatabaseDirectoryLayout.openOrInitializeDatabaseRoot(
                path: ["operation", "root"], access: engine.directoryAccess, transaction: probe)
            #expect(try #require(probe.minimumReadDepth) >= 2)
            let tenant = try await DatabaseDirectoryLayout.openOrCreateDefaultTenant(
                in: root, access: engine.directoryAccess, transaction: probe)
            let created = try await DatabaseDirectoryBinding.openOrCreate(
                ["parent", "child"], layers: [.default, .default], below: tenant.data,
                access: engine.directoryAccess, transaction: probe)
            probe.resetReadDepth()
            let opened = try await DatabaseDirectoryBinding.open(
                ["parent", "child"], layers: [.default, .default], below: tenant.data,
                access: engine.directoryAccess, transaction: probe)
            #expect(opened?.keyspacePrefix == created.keyspacePrefix)
            #expect(try #require(probe.minimumReadDepth) >= 2)
            probe.resetReadDepth()
            let unverified = try await DatabaseDirectoryBinding.openUnverified(
                ["parent", "child"], below: tenant.data,
                access: engine.directoryAccess, transaction: probe)
            #expect(unverified?.keyspacePrefix == created.keyspacePrefix)
            #expect(try #require(probe.minimumReadDepth) >= 2)
            probe.resetReadDepth()
            let reopened = try await DatabaseDirectoryLayout.openDatabaseRoot(
                path: ["operation", "root"], access: engine.directoryAccess, transaction: probe)
            #expect(reopened?.keyspacePrefix == root.keyspacePrefix)
            #expect(try #require(probe.minimumReadDepth) >= 2)
            #expect(probe.depth == 0)
            try await probe.cancel()
        } catch {
            try await probe.cancel()
            await engine.shutdown()
            throw error
        }
        await engine.shutdown()
    }
}
