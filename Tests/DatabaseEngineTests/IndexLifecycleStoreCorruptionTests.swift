import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import ScalarIndex
import StorageKit
import Testing
@testable import DatabaseEngine

@Persistable
private struct CatalogEntry {
    var id: String
}

@Persistable
private struct IndexedCatalogEntry {
    #Index(
        .scalar,
        fields: [\IndexedCatalogEntry.value],
        name: "IndexedCatalogEntry_value"
    )

    var id: String
    var value: String
}

@Suite("Index Lifecycle Store Corruption")
struct IndexLifecycleStoreCorruptionTests {
    @Test("State decoding distinguishes missing, valid, and corrupt values")
    func stateDecodingIsStrict() async throws {
        let context = try await makeContext()

        let missingState = try await context.lifecycleStore.state(
            of: context.indexName
        )
        #expect(missingState == .disabled)

        for (bytes, expectedState) in [
            (ByteString([IndexState.readable.rawValue]), IndexState.readable),
            (ByteString([IndexState.disabled.rawValue]), IndexState.disabled),
            (ByteString([IndexState.writeOnly.rawValue]), IndexState.writeOnly),
        ] {
            try await context.store(bytes)
            let state = try await context.lifecycleStore.state(
                of: context.indexName
            )
            #expect(state == expectedState)
        }

        for (bytes, expectedError) in [
            (
                ByteString(),
                IndexStateError.invalidPersistedStateSize(
                    index: context.indexName,
                    byteCount: 0
                )
            ),
            (
                ByteString([IndexState.readable.rawValue, 0x00]),
                IndexStateError.invalidPersistedStateSize(
                    index: context.indexName,
                    byteCount: 2
                )
            ),
            (
                ByteString([0xFF]),
                IndexStateError.unknownPersistedStateValue(
                    index: context.indexName,
                    value: 0xFF
                )
            ),
        ] {
            try await context.store(bytes)
            await expectIndexStateError(expectedError) {
                _ = try await context.lifecycleStore.state(
                    of: context.indexName
                )
            }
            try await context.engine.withTransaction { transaction in
                await expectIndexStateError(expectedError) {
                    _ = try await context.lifecycleStore.state(
                        of: context.indexName,
                        transaction: transaction
                    )
                }
            }
        }
    }

    @Test("Transitions never overwrite corrupt state")
    func transitionsPreserveCorruptState() async throws {
        let context = try await makeContext()
        let corruptBytes = ByteString([
            IndexState.readable.rawValue,
            IndexState.disabled.rawValue,
        ])
        let expectedError = IndexStateError.invalidPersistedStateSize(
            index: context.indexName,
            byteCount: corruptBytes.count
        )
        try await context.store(corruptBytes)

        for transition in IndexStateTransition.allCases {
            try await context.engine.withTransaction { transaction in
                await expectIndexStateError(expectedError) {
                    switch transition {
                    case .enable:
                        try await context.lifecycleStore.enable(
                            context.indexName,
                            transaction: transaction
                        )
                    case .makeReadable:
                        try await context.lifecycleStore.makeReadable(
                            context.indexName,
                            transaction: transaction
                        )
                    case .disable:
                        try await context.lifecycleStore.disable(
                            context.indexName,
                            transaction: transaction
                        )
                    }
                }
                let persistedBytes = try await transaction.getValue(
                    for: context.stateKey,
                    snapshot: false
                )
                #expect(persistedBytes == corruptBytes)
            }
        }
    }

    @Test("Batch state lookup is total and rejects any corrupt value")
    func batchStateLookupIsStrict() async throws {
        let context = try await makeContext()
        let validIndex = "valid"
        let missingIndex = "missing"
        let corruptIndex = "corrupt"
        try await context.store(
            [IndexState.readable.rawValue],
            indexName: validIndex
        )

        let validAndMissing = try await context.lifecycleStore.states(
            of: [validIndex, missingIndex]
        )
        #expect(validAndMissing.count == 2)
        #expect(validAndMissing[validIndex] == .readable)
        #expect(validAndMissing[missingIndex] == .disabled)

        try await context.store([0xFF], indexName: corruptIndex)
        await expectIndexStateError(
            .unknownPersistedStateValue(index: corruptIndex, value: 0xFF)
        ) {
            _ = try await context.lifecycleStore.states(
                of: [validIndex, missingIndex, corruptIndex]
            )
        }
    }

    @Test("Convergence never treats corrupt state as missing")
    func convergenceRejectsCorruptState() async throws {
        let context = try await makeContext()
        let corruptBytes = ByteString()
        let expectedError = IndexStateError.invalidPersistedStateSize(
            index: context.indexName,
            byteCount: 0
        )
        try await context.store(corruptBytes)

        await expectIndexStateError(expectedError) {
            try await context.lifecycleStore.ensureReadable(
                [context.indexName],
                entityRange: context.entityRange
            )
        }
        try await context.engine.withTransaction { transaction in
            await expectIndexStateError(expectedError) {
                try await context.lifecycleStore.initializeMissingStates(
                    [context.indexName],
                    entityRange: context.entityRange,
                    transaction: transaction
                )
            }
            await expectIndexStateError(expectedError) {
                try await context.lifecycleStore.validateReadableForRead(
                    [context.indexName],
                    transaction: transaction
                )
            }
            let persistedBytes = try await transaction.getValue(
                for: context.stateKey,
                snapshot: false
            )
            #expect(persistedBytes == corruptBytes)
        }
    }

    @Test("Read admission rejects a missing lifecycle state")
    func readAdmissionRejectsMissingState() async throws {
        let context = try await makeContext()

        try await context.engine.withTransaction { transaction in
            await expectIndexStateError(
                .missingPersistedState(index: context.indexName)
            ) {
                try await context.lifecycleStore.validateReadableForRead(
                    [context.indexName],
                    transaction: transaction
                )
            }
        }
    }

    @Test("Container reads fail when an existing index loses its state")
    func containerReadRejectsMissingState() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: IndexedCatalogEntry.self)
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        IndexedCatalogEntry.self
                    )
                ]
            ),
            security: .disabled
        )
        let dataStore = try await container.store(
            for: IndexedCatalogEntry.self
        )
        let indexName = "IndexedCatalogEntry_value"
        let stateKey = dataStore.subspace
            .subspace("state")
            .pack(Tuple(indexName))
        try await engine.withTransaction { transaction in
            try transaction.clear(key: stateKey)
        }

        await expectIndexStateError(
            .missingPersistedState(index: indexName)
        ) {
            _ = try await container.newContext()
                .indexQueryContext.withReadableIndex(
                    named: indexName,
                    kindIdentifier: IndexDefinition.scalar.identifier,
                    for: IndexedCatalogEntry.self
                ) { _, _ in
                    ()
                }
        }
    }

    @Test("Entity and index mutation rolls back when state is corrupt")
    func mutationRollsBackForCorruptState() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [
                try Schema.Entity(
                    from: IndexedCatalogEntry.self
                )
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        IndexedCatalogEntry.self
                    )
                ]
            ),
            security: .disabled
        )
        let dataStore = try await container.store(
            for: IndexedCatalogEntry.self
        )
        let indexName = "IndexedCatalogEntry_value"
        let stateKey = dataStore.subspace
            .subspace("state")
            .pack(Tuple(indexName))
        let corruptBytes = ByteString([0xFF])
        try await engine.withTransaction { transaction in
            try transaction.setValue(corruptBytes, for: stateKey)
        }

        await expectIndexStateError(
            .unknownPersistedStateValue(index: indexName, value: 0xFF)
        ) {
            try await dataStore.save([
                IndexedCatalogEntry(
                    id: "entity",
                    value: "value"
                )
            ])
        }

        try await engine.withTransaction { transaction in
            let itemRange = dataStore.subspace
                .subspace(SubspaceKey.items)
                .range()
            let indexRange = dataStore.subspace
                .subspace(SubspaceKey.indexes)
                .subspace(indexName)
                .range()
            let itemRows = try await transaction.collectRange(
                begin: itemRange.begin,
                end: itemRange.end,
                snapshot: true
            )
            let indexRows = try await transaction.collectRange(
                begin: indexRange.begin,
                end: indexRange.end,
                snapshot: true
            )
            let persistedState = try await transaction.getValue(
                for: stateKey,
                snapshot: true
            )
            #expect(itemRows.isEmpty)
            #expect(indexRows.isEmpty)
            #expect(persistedState == corruptBytes)
        }
    }

    private func makeContext() async throws -> IndexStateScenario {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [
                try Schema.Entity(from: CatalogEntry.self)
            ]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        CatalogEntry.self
                    )
                ]
            ),
            security: .disabled
        )
        let root = Subspace(
            prefix: Tuple("index-lifecycle-store-corruption").pack()
        )
        return IndexStateScenario(
            engine: engine,
            lifecycleStore: IndexLifecycleStore(
                container: container,
                subspace: root
            ),
            root: root,
            indexName: "test_index"
        )
    }

    private func expectIndexStateError(
        _ expected: IndexStateError,
        operation: () async throws -> Void
    ) async {
        do {
            try await operation()
            Issue.record("Expected index state operation to fail")
        } catch let error as IndexStateError {
            #expect(error == expected)
        } catch {
            Issue.record("Unexpected error: \(error)")
        }
    }
}

private enum IndexStateTransition: CaseIterable, Sendable {
    case enable
    case makeReadable
    case disable
}

private struct IndexStateScenario: Sendable {
    let engine: InMemoryEngine
    let lifecycleStore: IndexLifecycleStore
    let root: Subspace
    let indexName: String

    var stateKey: ByteString {
        stateKey(for: indexName)
    }

    var entityRange: (begin: ByteString, end: ByteString) {
        root.subspace("entities").range()
    }

    func store(
        _ bytes: ByteString,
        indexName: String? = nil
    ) async throws {
        let key = stateKey(for: indexName ?? self.indexName)
        try await engine.withTransaction { transaction in
            try transaction.setValue(bytes, for: key)
        }
    }

    private func stateKey(for indexName: String) -> ByteString {
        root.subspace("state").pack(Tuple(indexName))
    }
}
