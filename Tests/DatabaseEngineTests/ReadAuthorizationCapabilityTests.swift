import DatabaseKit
import DatabaseRuntime
import StorageKit
import TestSupport
import Testing
@_spi(DatabaseExecution) @testable import DatabaseEngine

@Suite("Read authorization capability")
struct ReadAuthorizationCapabilityTests {
    @Persistable
    struct Anchor {
        var id: String = ""
    }

    @Test("Read transaction access rejects persistent mutations")
    func readTransactionRejectsMutations() async throws {
        let engine = InMemoryEngine()
        let key = ByteString(utf8: "read-authorization-capability")

        try await engine.withTransaction { rawTransaction in
            let transaction = DataRootTransactionAccess.admitted(
                rawTransaction,
                dataRoot: Subspace()
            )
            #expect(transaction.compaction == nil)
            let value = try await transaction.getValue(
                for: key,
                snapshot: true
            )
            #expect(value == nil)
            #expect(throws: DatabaseReadTransactionError.self) {
                try transaction.setValue(ByteString(utf8: "value"), for: key)
            }
            #expect(throws: DatabaseReadTransactionError.self) {
                try transaction.clear(key: key)
            }
            #expect(throws: DatabaseReadTransactionError.self) {
                try transaction.clearRange(
                    beginKey: key,
                    endKey: ByteString(
                        utf8: "read-authorization-capability~"
                    )
                )
            }
        }
    }

    @Test("Read transaction access rejects transaction control")
    func readTransactionRejectsTransactionControl() async throws {
        let engine = InMemoryEngine()

        try await engine.withTransaction { rawTransaction in
            let transaction = DataRootTransactionAccess.admitted(
                rawTransaction,
                dataRoot: Subspace()
            )
            #expect(transaction.capabilities == .none)
            #expect(
                throws: DatabaseReadTransactionError
                    .transactionControlUnavailable
            ) {
                try transaction.setReadVersion(1)
            }
            await #expect(
                throws: DatabaseReadTransactionError
                    .transactionControlUnavailable
            ) {
                try await transaction.getReadVersion()
            }
            #expect(
                throws: DatabaseReadTransactionError
                    .transactionControlUnavailable
            ) {
                try transaction.setOption(forOption: .priorityBatch)
            }
            let versionstamp = transaction.requestVersionstamp()
            await #expect(
                throws: DatabaseReadTransactionError
                    .versionstampUnavailable
            ) {
                try await versionstamp.value
            }
        }
    }

    @Test("Data-root transactions reject namespace metadata access")
    func dataRootTransactionsRejectNamespaceMetadataAccess() async throws {
        let engine = InMemoryEngine()
        let container = try await makeContainer(storageEngine: engine)
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        await #expect(throws: StorageError.self) {
            try await context.withWriteStorageAccess(
            requiredAccess: .write
        ) { transaction in
                try await container.engine.namespaceResolver.resolveOrCreate(
                path: ["write-root-must-not-borrow-namespace"],
                transaction: transaction
            )
        }
        }
        await #expect(throws: StorageError.self) {
            try await context.withReadStorageAccess { transaction in
                try await container.engine.namespaceResolver.resolveExisting(
                    path: ["read-root-must-not-borrow-namespace"],
                    transaction: transaction
                )
            }
        }
    }

    @Test("Read transaction access cannot be recovered as write access")
    func readTransactionErasesWriteCapability() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        try await context.withReadStorageAccess { transaction in
            #expect(((transaction as Any) is any TransactionAccess) == false)
        }
    }

    @Test("Server execution read projection cannot recover write access")
    func executionReadProjectionErasesWriteCapability() async throws {
        let container = try await makeContainer()
        defer { await container.shutdown() }
        let context = container.testBaseContext()

        try await context.withExecutionTransaction { transaction in
            let readAccess = transaction.executionReadTransaction.storageAccess
            #expect(((readAccess as Any) is any TransactionAccess) == false)
        }
    }

    @Test("Write transaction access cannot cross its admitted data root")
    func writeTransactionCannotCrossDataRoot() async throws {
        let engine = InMemoryEngine()
        let rootA = Subspace(prefix: ByteString(utf8: "root-a/"))
        let rootB = Subspace(prefix: ByteString(utf8: "root-b/"))
        let keyA = rootA.pack(Tuple("item"))
        let nextKeyA = rootA.pack(Tuple("next-item"))
        let keyB = rootB.pack(Tuple("item"))
        let value = ByteString(utf8: "value")
        let (_, rootAUpperBound) = rootA.range()

        try await engine.withTransaction { rawTransaction in
            try rawTransaction.setValue(value, for: rootAUpperBound)
            let transaction = DataRootTransactionAccess.admitted(
                rawTransaction,
                dataRoot: rootA,
                accessMode: .readWrite
            )
            #expect(transaction.compaction == nil)
            try transaction.setValue(value, for: keyA)
            try transaction.setValue(value, for: nextKeyA)
            #expect(
                try await transaction.getValue(
                    for: keyA,
                    snapshot: false
                ) == value
            )
            #expect(try await transaction.getKey(
                selector: .firstGreaterOrEqual(rootAUpperBound),
                snapshot: true
            ) == nil)
            #expect(try await transaction.getKey(
                selector: .firstGreaterThan(rootAUpperBound),
                snapshot: true
            ) == nil)
            #expect(try await transaction.getKey(
                selector: .lastLessThan(rootAUpperBound),
                snapshot: true
            ) == nextKeyA)
            var continuationCursor = transaction.rangeCursor(
                from: .firstGreaterThan(keyA),
                to: .firstGreaterOrEqual(rootAUpperBound),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            #expect(try await continuationCursor.next()?.0 == nextKeyA)
            #expect(try await continuationCursor.next() == nil)
            try await continuationCursor.finish()

            var rejectedContinuation = transaction.rangeCursor(
                from: .firstGreaterThan(rootAUpperBound),
                to: .firstGreaterOrEqual(rootAUpperBound),
                limit: 0,
                reverse: false,
                snapshot: true,
                streamingMode: .wantAll
            )
            await #expect(
                throws: DatabaseReadTransactionError.rangeOutsideDataRoot
            ) {
                _ = try await rejectedContinuation.next()
            }
            try await rejectedContinuation.finish()
            #expect(throws: DatabaseReadTransactionError.keyOutsideDataRoot) {
                try transaction.setValue(value, for: keyB)
            }
            #expect(throws: DatabaseReadTransactionError.keyOutsideDataRoot) {
                try transaction.clear(key: keyB)
            }
            #expect(throws: DatabaseReadTransactionError.rangeOutsideDataRoot) {
                try transaction.clearRange(
                    beginKey: rootB.prefix,
                    endKey: try strinc(rootB.prefix)
                )
            }
            #expect(throws: DatabaseReadTransactionError.keyOutsideDataRoot) {
                try transaction.atomicOp(
                    key: keyB,
                    param: ByteString(repeating: 0, count: 8),
                    mutationType: .add
                )
            }
            #expect(throws: DatabaseReadTransactionError.rangeOutsideDataRoot) {
                try transaction.addConflictRange(
                    beginKey: rootB.prefix,
                    endKey: try strinc(rootB.prefix),
                    type: .write
                )
            }
            await #expect(
                throws: DatabaseReadTransactionError.keyOutsideDataRoot
            ) {
                _ = try await transaction.getValue(
                    for: keyB,
                    snapshot: false
                )
            }
            #expect(
                throws: DatabaseReadTransactionError
                    .transactionControlUnavailable
            ) {
                try transaction.setOption(forOption: .priorityBatch)
            }
        }

        #expect(try await engine.withTransaction { transaction in
            try await transaction.getValue(for: keyA) == value
        })
        #expect(try await engine.withTransaction { transaction in
            try await transaction.getValue(for: keyB) == nil
        })
    }

    @Test("Versionstamped keys cannot replace the admitted data root")
    func versionstampedKeyCannotReplaceDataRoot() async throws {
        let engine = InMemoryEngine()
        let root = Subspace(prefix: ByteString(utf8: "root-a/"))
        let value = ByteString(utf8: "value")

        let rawTransaction = try engine.createTransaction()
        let transaction = DataRootTransactionAccess.admitted(
            rawTransaction,
            dataRoot: root,
            accessMode: .readWrite
        )

        #expect(throws: VersionstampedMutationOperandError.self) {
            try transaction.atomicOp(
                key: self.versionstampedOperand(
                    payloadPrefix: root.prefix,
                    replacementOffset: 0
                ),
                param: value,
                mutationType: .setVersionstampedKey
            )
        }
        #expect(throws: VersionstampedMutationOperandError.self) {
            try transaction.atomicOp(
                key: root.prefix.appending(contentsOf: [0xFF, 0xFF]),
                param: value,
                mutationType: .setVersionstampedKey
            )
        }
        #expect(throws: VersionstampedMutationOperandError.self) {
            try transaction.atomicOp(
                key: self.versionstampedOperand(
                    payloadPrefix: root.prefix,
                    replacementOffset: UInt32.max
                ),
                param: value,
                mutationType: .setVersionstampedKey
            )
        }

        let validPrefix = root.pack(Tuple("version"))
        let validKey = self.versionstampedOperand(
            payloadPrefix: validPrefix,
            replacementOffset: UInt32(validPrefix.count)
        )
        #expect(throws: Never.self) {
            try transaction.atomicOp(
                key: validKey,
                param: value,
                mutationType: .setVersionstampedKey
            )
        }
        transaction.revoke()
        try await rawTransaction.cancel()
    }

    private func versionstampedOperand(
        payloadPrefix: ByteString,
        replacementOffset: UInt32
    ) -> ByteString {
        ByteString.copying(count: payloadPrefix.count + 14) { destination in
            payloadPrefix.withUnsafeBytes { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[..<source.count]
                ).copyMemory(from: source)
            }
            UnsafeMutableRawBufferPointer(
                rebasing: destination[
                    payloadPrefix.count..<(payloadPrefix.count + 10)
                ]
            ).initializeMemory(as: UInt8.self, repeating: 0xFF)
            var offset = replacementOffset.littleEndian
            withUnsafeBytes(of: &offset) { source in
                UnsafeMutableRawBufferPointer(
                    rebasing: destination[(payloadPrefix.count + 10)...]
                ).copyMemory(from: source)
            }
        }
    }

    private func makeContainer(
        storageEngine: InMemoryEngine = InMemoryEngine()
    ) async throws -> DBContainer {
        try await DBContainer.open(
            testing: try Schema(
                entities: [try Anchor.schemaEntity],
                version: Schema.Version(1, 0, 0)
            ),
            configuration: .testing(storageEngine: storageEngine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(Anchor.self)
                ]
            ),
            security: .testingDisabled
        )
    }
}
