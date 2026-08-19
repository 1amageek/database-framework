import DatabaseKit
import DatabaseRuntime
import DatabaseTypes
import ScalarIndex
import StorageKit
import TestSupport
import Testing

@_spi(DatabaseExecution) @testable import DatabaseEngine

@Persistable
private struct CatalogEntry {
    #Index(.ordered(name: "test_index", keys: [.ascending(\CatalogEntry.id)]))
    #Index(.ordered(name: "valid", keys: [.ascending(\CatalogEntry.id)]))
    #Index(.ordered(name: "missing", keys: [.ascending(\CatalogEntry.id)]))
    #Index(.ordered(name: "corrupt", keys: [.ascending(\CatalogEntry.id)]))

    var id: String
}

@Persistable
private struct IndexedCatalogEntry {
    #Index(
        .ordered(
            name: "IndexedCatalogEntry_value", keys: [.ascending(\IndexedCatalogEntry.value)],
            unique: false))

    var id: String
    var value: String
}

@Suite("Index Lifecycle Store Corruption")
struct IndexLifecycleStoreCorruptionTests {
    @Test("Layout-only transition replaces exactly one physical generation")
    func layoutOnlyTransitionUsesExactPhysicalIdentities() throws {
        let schema = try Schema(entities: [try CatalogEntry.schemaEntity])
        let firstLayout = try IndexPhysicalLayout(
            name: "test.ordered",
            revision: 1
        )
        let secondLayout = try IndexPhysicalLayout(
            name: "test.ordered",
            revision: 2
        )
        var currentLayouts = Dictionary(
            uniqueKeysWithValues: schema.indexDescriptors.map {
                ($0.name, firstLayout.fingerprint)
            }
        )
        var targetLayouts = Dictionary(
            uniqueKeysWithValues: schema.indexDescriptors.map {
                ($0.name, firstLayout)
            }
        )
        currentLayouts["test_index"] = firstLayout.fingerprint
        targetLayouts["test_index"] = secondLayout

        let plan = try DatabaseIndexTransitionPlan(
            currentSchema: schema,
            currentLayoutFingerprints: currentLayouts,
            targetSchema: schema,
            targetPhysicalLayouts: targetLayouts
        )

        #expect(plan.builds.count == 1)
        #expect(plan.retirements.count == 1)
        let build = try #require(plan.builds.first)
        let retirement = try #require(plan.retirements.first)
        #expect(build.identity.name == "test_index")
        #expect(build.identity.layoutFingerprint == secondLayout.fingerprint)
        #expect(retirement.identity.name == "test_index")
        #expect(
            retirement.identity.layoutFingerprint == firstLayout.fingerprint
        )
    }

    @Test("Directory transition rebuilds and retires the scoped generation")
    func directoryTransitionRetainsSourceAndTargetScopes() throws {
        let compiled = try CatalogEntry.schemaEntity
        let descriptor = try #require(
            compiled.indexDescriptors.first { $0.name == "test_index" }
        )
        let sourceEntity = try Schema.Entity(
            name: compiled.name,
            identifierType: compiled.identifierType,
            fields: compiled.fields,
            directoryComponents: [.staticPath("source")],
            indexes: [descriptor]
        )
        let targetEntity = try Schema.Entity(
            name: compiled.name,
            identifierType: compiled.identifierType,
            fields: compiled.fields,
            directoryComponents: [.staticPath("target")],
            indexes: [descriptor]
        )
        let sourceSchema = try Schema(entities: [sourceEntity])
        let targetSchema = try Schema(entities: [targetEntity])
        let layout = try IndexPhysicalLayout(
            name: "test.ordered",
            revision: 1
        )

        let plan = try DatabaseIndexTransitionPlan(
            currentSchema: sourceSchema,
            currentLayoutFingerprints: [
                descriptor.name: layout.fingerprint
            ],
            targetSchema: targetSchema,
            targetPhysicalLayouts: [descriptor.name: layout]
        )

        let build = try #require(plan.builds.first)
        let retirement = try #require(plan.retirements.first)
        #expect(plan.builds.count == 1)
        #expect(plan.retirements.count == 1)
        #expect(build.identity == retirement.identity)
        #expect(
            build.scope
                == .entity(
                    name: compiled.name,
                    directoryComponents: [.staticPath("target")]
                )
        )
        #expect(
            retirement.scope
                == .entity(
                    name: compiled.name,
                    directoryComponents: [.staticPath("source")]
                )
        )
    }

    @Test("Physical scope admits only its own partition contract")
    func physicalScopeFiltersPartitionCatalogEntries() throws {
        let scope = DatabaseIndexStorageScope.entity(
            name: "PartitionedEntry",
            directoryComponents: [
                .staticPath("accounts"),
                .dynamicField(fieldName: "tenant"),
                .dynamicField(fieldName: "region"),
            ]
        )
        #expect(
            scope.accepts(
                partitions: try FieldObject([
                    ("region", .string("east")),
                    ("tenant", .string("one")),
                ])))
        #expect(
            !scope.accepts(
                partitions: try FieldObject([
                    ("tenant", .string("one"))
                ])))
        #expect(
            !scope.accepts(
                partitions: try FieldObject([
                    ("region", .string("east")),
                    ("tenant", .string("one")),
                    ("workspace", .string("primary")),
                ])))
        #expect(
            DatabaseIndexStorageScope.polymorphicGroup(
                identifier: "Document",
                directoryPath: ["documents"]
            ).accepts(partitions: FieldObject())
        )
    }

    @Test("Build completion rejects a different physical generation")
    func buildCompletionRequiresExactPhysicalIdentity() async throws {
        let engine = InMemoryEngine()
        let entity = try CatalogEntry.schemaEntity
        let schema = try Schema(entities: [entity])
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(CatalogEntry.self)
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }
        let descriptor = try #require(
            entity.indexDescriptors.first { $0.name == "test_index" }
        )
        let mismatchedLayout = try IndexPhysicalLayout(
            name: "test.ordered",
            revision: 99
        )
        let mismatchedTarget = try DatabaseIndexTransitionPlan.Target(
            scope: .entity(
                name: entity.name,
                directoryComponents: entity.directoryComponents
            ),
            identity: try DatabaseIndexStorageIdentity(
                name: descriptor.name,
                definitionFingerprint:
                    try DatabaseIndexStorageIdentity
                    .definitionFingerprint(
                        named: descriptor.name,
                        in: schema
                    ),
                layoutFingerprint: mismatchedLayout.fingerprint
            )
        )

        try await engine.withTransaction { transaction in
            #expect(throws: DatabaseSchemaPublicationError.self) {
                try container.completeSchemaIndexBuild(
                    mismatchedTarget,
                    transaction: transaction
                )
            }
        }
    }

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
                    for: try context.stateKey,
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
                for: try context.stateKey,
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

    @Test("Provider layout changes use independent lifecycle state")
    func providerLayoutsHaveIndependentState() async throws {
        let engine = InMemoryEngine()
        let schema = try Schema(
            entities: [try Schema.Entity(from: CatalogEntry.self)]
        )
        let container = try await DBContainer.open(
            for: schema,
            configuration: .testing(storageEngine: engine),
            runtimeConfiguration: try DatabaseFrameworkRuntime.configuration(
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(CatalogEntry.self)
                ]
            ),
            security: .testingDisabled
        )
        defer { await container.shutdown() }

        let root = Subspace(
            prefix: Tuple("index-layout-generation-isolation").pack()
        )
        let firstLayout = try IndexPhysicalLayout(
            name: "test.layout",
            revision: 1
        )
        let secondLayout = try IndexPhysicalLayout(
            name: "test.layout",
            revision: 2
        )
        let first = IndexLifecycleStore(
            container: container,
            subspace: root,
            schema: schema,
            indexPhysicalLayouts: ["test_index": firstLayout]
        )
        let second = IndexLifecycleStore(
            container: container,
            subspace: root,
            schema: schema,
            indexPhysicalLayouts: ["test_index": secondLayout]
        )

        try await engine.withTransaction { transaction in
            try await first.enable("test_index", transaction: transaction)
            #expect(
                try await first.state(
                    of: "test_index",
                    transaction: transaction
                ) == .writeOnly
            )
            #expect(
                try await second.state(
                    of: "test_index",
                    transaction: transaction
                ) == .disabled
            )
        }
        #expect(
            try first.indexSubspace(for: "test_index")
                != second.indexSubspace(for: "test_index")
        )
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
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        IndexedCatalogEntry.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        let dataStore = try await container.testBaseStore(
            for: IndexedCatalogEntry.self
        )
        let indexName = "IndexedCatalogEntry_value"
        let identity = try IndexLifecycleStore(
            container: container,
            subspace: dataStore.subspace
        ).storageIdentity(for: indexName)
        let stateKey = dataStore.subspace
            .subspace("state")
            .subspace(identity.name)
            .pack(
                Tuple(
                    identity.definitionFingerprint.bytes,
                    identity.layoutFingerprint
                )
            )
        try await engine.withTransaction { transaction in
            try transaction.clear(key: stateKey)
        }

        await expectIndexStateError(
            .missingPersistedState(index: indexName)
        ) {
            _ = try await container.testBaseContext()
                .indexQueryContext.withReadableIndex(
                    named: indexName,
                    indexType: .ordered,
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
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        IndexedCatalogEntry.self
                    )
                ]
            ),
            security: .testingDisabled
        )
        let dataStore = try await container.testBaseStore(
            for: IndexedCatalogEntry.self
        )
        let indexName = "IndexedCatalogEntry_value"
        let identity = try IndexLifecycleStore(
            container: container,
            subspace: dataStore.subspace
        ).storageIdentity(for: indexName)
        let stateKey = dataStore.subspace
            .subspace("state")
            .subspace(identity.name)
            .pack(
                Tuple(
                    identity.definitionFingerprint.bytes,
                    identity.layoutFingerprint
                )
            )
        let corruptBytes = ByteString([0xFF])
        try await engine.withTransaction { transaction in
            try transaction.setValue(corruptBytes, for: stateKey)
        }

        await expectIndexStateError(
            .unknownPersistedStateValue(index: indexName, value: 0xFF)
        ) {
            try await container.withTestBaseOperation {
                try await dataStore.save([
                    IndexedCatalogEntry(
                        id: "entity",
                        value: "value"
                    )
                ])
            }
        }

        try await engine.withTransaction { transaction in
            let itemRange = dataStore.subspace
                .subspace(SubspaceKey.items)
                .range()
            let indexRange = dataStore.subspace
                .subspace(SubspaceKey.indexes)
                .subspace(identity.name)
                .subspace(identity.definitionFingerprint.bytes)
                .subspace(identity.layoutFingerprint)
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
                executionIdentity: DatabaseExecutionRuntimeIdentity(
                    identifier: "database-tests",
                    revision: 1
                ),
                entityRuntimes: [
                    try DatabaseFrameworkRuntime.entity(
                        CatalogEntry.self
                    )
                ]
            ),
            security: .testingDisabled
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
        get throws {
            try stateKey(for: indexName)
        }
    }

    var entityRange: (begin: ByteString, end: ByteString) {
        root.subspace("entities").range()
    }

    func store(
        _ bytes: ByteString,
        indexName: String? = nil
    ) async throws {
        let key = try stateKey(for: indexName ?? self.indexName)
        try await engine.withTransaction { transaction in
            try transaction.setValue(bytes, for: key)
        }
    }

    private func stateKey(for indexName: String) throws -> ByteString {
        let identity = try lifecycleStore.storageIdentity(for: indexName)
        return
            root
            .subspace("state")
            .subspace(identity.name)
            .pack(
                Tuple(
                    identity.definitionFingerprint.bytes,
                    identity.layoutFingerprint
                )
            )
    }
}
