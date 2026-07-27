import DatabaseEngine
import DatabaseTypes
import DatabaseKit
import DatabaseWire
import StorageKit
import TestHeartbeat
import Testing
@testable import GraphIndex

@Suite("Property graph definition catalog", .heartbeat)
struct PropertyGraphDefinitionCatalogTests {
    private enum ExpectedFailure: Error {
        case rollback
    }

    @Test("Creation persists the complete canonical definition")
    func createAndReadDefinition() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let requested = makeDefinition(
            named: "calendar",
            ifNotExists: true
        )

        try await engine.withTransaction(configuration: .batch) { transaction in
            let creation = try await catalog.create(
                requested,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(creation == .created)

            let stored = try await catalog.definition(
                named: requested.graphName,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(stored == canonicalDefinition(requested))
            #expect(stored?.ifNotExists == false)
        }
    }

    @Test("Duplicate creation fails and preserves the original definition")
    func duplicateCreationFails() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let original = makeDefinition(named: "calendar")
        let replacement = CreateGraphStatement(
            graphName: original.graphName,
            vertexTables: [
                VertexTableDefinition(
                    tableName: "replacement_events",
                    keyColumns: ["id"]
                ),
            ],
            edgeTables: []
        )

        try await create(
            original,
            in: catalog,
            using: engine
        )

        await #expect(
            throws: PropertyGraphDefinitionCatalogError.graphAlreadyExists(
                original.graphName
            )
        ) {
            try await engine.withTransaction(
                configuration: .batch
            ) { transaction in
                _ = try await catalog.create(
                    replacement,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
            }
        }

        let stored = try await read(
            original.graphName,
            from: catalog,
            using: engine
        )
        #expect(stored == original)
    }

    @Test("IF NOT EXISTS retains a valid existing definition")
    func conditionalCreationRetainsExistingDefinition() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let original = makeDefinition(named: "calendar")
        let conditionalReplacement = CreateGraphStatement(
            graphName: original.graphName,
            ifNotExists: true,
            vertexTables: [],
            edgeTables: []
        )

        try await create(
            original,
            in: catalog,
            using: engine
        )

        let creation = try await engine.withTransaction(
            configuration: .batch
        ) { transaction in
            try await catalog.create(
                conditionalReplacement,
                transaction: transaction,
                workMeter: makeMeter()
            )
        }
        #expect(creation == .retainedExistingDefinition)

        let stored = try await read(
            original.graphName,
            from: catalog,
            using: engine
        )
        #expect(stored == original)
    }

    @Test("IF NOT EXISTS never hides a corrupt existing definition")
    func conditionalCreationRejectsCorruptExistingDefinition() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let definition = makeDefinition(
            named: "calendar",
            ifNotExists: true
        )

        try await writeStoredDefinition(
            definition,
            named: definition.graphName,
            using: engine
        )

        await #expect(
            throws: PropertyGraphDefinitionCatalogError.invalidStoredDefinition(
                graphName: definition.graphName,
                violation: .containsCreationCondition
            )
        ) {
            try await engine.withTransaction(
                configuration: .batch
            ) { transaction in
                _ = try await catalog.create(
                    definition,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
            }
        }
    }

    @Test("Drop removes only the definition and missing drop fails")
    func dropDefinitionLifecycle() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let definition = makeDefinition(named: "calendar")

        try await create(
            definition,
            in: catalog,
            using: engine
        )
        try await engine.withTransaction(configuration: .batch) { transaction in
            try await catalog.dropDefinition(
                named: definition.graphName,
                transaction: transaction,
                workMeter: makeMeter()
            )
        }
        let removed = try await read(
            definition.graphName,
            from: catalog,
            using: engine
        )
        #expect(removed == nil)

        await #expect(
            throws: PropertyGraphDefinitionCatalogError.graphNotFound(
                definition.graphName
            )
        ) {
            try await engine.withTransaction(
                configuration: .batch
            ) { transaction in
                try await catalog.dropDefinition(
                    named: definition.graphName,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
            }
        }
    }

    @Test("Malformed stored bytes are reported as catalog corruption")
    func malformedStoredDefinitionFails() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let graphName = "calendar"
        let storage = makeStorage()
        let key = try storage.key(for: graphName)

        try await engine.withTransaction(configuration: .batch) { transaction in
            try transaction.setValue([255], for: key)
        }

        await #expect(
            throws: PropertyGraphDefinitionCatalogError.invalidStoredDefinition(
                graphName: graphName,
                violation: .decodingFailed(.truncated)
            )
        ) {
            _ = try await read(
                graphName,
                from: catalog,
                using: engine
            )
        }
    }

    @Test("Stored graph name must match its catalog key")
    func storedGraphNameMismatchFails() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let expectedName = "calendar"
        let actualName = "contacts"

        try await writeStoredDefinition(
            makeDefinition(named: actualName),
            named: expectedName,
            using: engine
        )

        await #expect(
            throws: PropertyGraphDefinitionCatalogError.invalidStoredDefinition(
                graphName: expectedName,
                violation: .graphNameMismatch(actual: actualName)
            )
        ) {
            _ = try await read(
                expectedName,
                from: catalog,
                using: engine
            )
        }
    }

    @Test("Stored definitions are decoded under configured bounds")
    func boundedDecodeRejectsOversizedFrame() async throws {
        let engine = InMemoryEngine()
        let graphName = "calendar"
        let definition = makeDefinition(named: graphName)
        let encoded = try PropertyGraphDefinitionStorageFormat.encode(
            definition
        )
        let maximumFrameBytes = encoded.count - 1
        let limits = try StorageFrameLimits(
            maximumFrameBytes: maximumFrameBytes,
            maximumStringBytes: 1_024,
            maximumByteStringBytes: 1_024,
            maximumCollectionCount: 1_024,
            maximumNestingDepth: 64
        )
        let catalog = CanonicalPropertyGraphDefinitionCatalog(
            storageLimits: limits
        )
        let key = try makeStorage().key(for: graphName)

        try await engine.withTransaction(configuration: .batch) { transaction in
            try transaction.setValue(encoded, for: key)
        }

        await #expect(
            throws: PropertyGraphDefinitionCatalogError.invalidStoredDefinition(
                graphName: graphName,
                violation: .decodingFailed(
                    .frameTooLarge(
                        actual: encoded.count,
                        maximum: maximumFrameBytes
                    )
                )
            )
        ) {
            _ = try await read(
                graphName,
                from: catalog,
                using: engine
            )
        }
    }

    @Test("Caller transaction rollback removes catalog mutation")
    func callerTransactionRollbackIsAtomic() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let definition = makeDefinition(named: "calendar")

        await #expect(throws: ExpectedFailure.self) {
            try await engine.withTransaction(
                configuration: .batch
            ) { transaction in
                _ = try await catalog.create(
                    definition,
                    transaction: transaction,
                    workMeter: makeMeter()
                )
                throw ExpectedFailure.rollback
            }
        }

        let stored = try await read(
            definition.graphName,
            from: catalog,
            using: engine
        )
        #expect(stored == nil)
    }

    @Test("Property graph and RDF graph namespaces remain independent")
    func rdfGraphNamespaceIsIndependent() async throws {
        let engine = InMemoryEngine()
        let catalog = CanonicalPropertyGraphDefinitionCatalog()
        let rdfStore = CanonicalRDFGraphStore()
        let sharedName = "https://example.com/graphs/calendar"
        let definition = makeDefinition(named: sharedName)
        let rdfGraph = try RDFGraphName(iri: sharedName)

        try await engine.withTransaction(configuration: .batch) { transaction in
            _ = try await catalog.create(
                definition,
                transaction: transaction,
                workMeter: makeMeter()
            )
            try await rdfStore.createGraph(
                rdfGraph,
                transaction: transaction,
                workMeter: makeMeter()
            )
            try await catalog.dropDefinition(
                named: sharedName,
                transaction: transaction,
                workMeter: makeMeter()
            )

            let propertyDefinition = try await catalog.definition(
                named: sharedName,
                transaction: transaction,
                workMeter: makeMeter()
            )
            let rdfGraphExists = try await rdfStore.containsGraph(
                rdfGraph,
                readMode: .serializable,
                transaction: transaction,
                workMeter: makeMeter()
            )
            #expect(propertyDefinition == nil)
            #expect(rdfGraphExists)
        }
    }

    private func create(
        _ definition: CreateGraphStatement,
        in catalog: CanonicalPropertyGraphDefinitionCatalog,
        using engine: InMemoryEngine
    ) async throws {
        try await engine.withTransaction(configuration: .batch) { transaction in
            _ = try await catalog.create(
                definition,
                transaction: transaction,
                workMeter: makeMeter()
            )
        }
    }

    private func read(
        _ graphName: String,
        from catalog: CanonicalPropertyGraphDefinitionCatalog,
        using engine: InMemoryEngine
    ) async throws -> CreateGraphStatement? {
        try await engine.withTransaction(configuration: .default) { transaction in
            try await catalog.definition(
                named: graphName,
                transaction: transaction,
                workMeter: makeMeter()
            )
        }
    }

    private func writeStoredDefinition(
        _ definition: CreateGraphStatement,
        named graphName: String,
        using engine: InMemoryEngine
    ) async throws {
        let encoded = try PropertyGraphDefinitionStorageFormat.encode(
            definition
        )
        let key = try makeStorage().key(for: graphName)
        try await engine.withTransaction(configuration: .batch) { transaction in
            try transaction.setValue(encoded, for: key)
        }
    }

    private func makeStorage() -> PropertyGraphDefinitionCatalogStorage {
        PropertyGraphDefinitionCatalogStorage(
            subspace: CanonicalPropertyGraphDefinitionCatalog
                .defaultRootSubspace
                .subspace(Int64(1)),
            limits: .default
        )
    }

    private func makeDefinition(
        named graphName: String,
        ifNotExists: Bool = false
    ) -> CreateGraphStatement {
        CreateGraphStatement(
            graphName: graphName,
            ifNotExists: ifNotExists,
            vertexTables: [
                VertexTableDefinition(
                    tableName: "events",
                    alias: "event",
                    keyColumns: ["id"],
                    labelExpression: .single("Event"),
                    propertiesSpec: .columns(["title", "startsAt"])
                ),
            ],
            edgeTables: [
                EdgeTableDefinition(
                    tableName: "event_relations",
                    alias: "relation",
                    keyColumns: ["id"],
                    sourceVertex: VertexReference(
                        tableName: "events",
                        keyColumns: [
                            KeyColumnMapping(
                                source: "sourceEventID",
                                target: "id"
                            ),
                        ]
                    ),
                    destinationVertex: VertexReference(
                        tableName: "events",
                        keyColumns: [
                            KeyColumnMapping(
                                source: "destinationEventID",
                                target: "id"
                            ),
                        ]
                    ),
                    labelExpression: .column("kind"),
                    propertiesSpec: .allExcept(["sourceEventID"])
                ),
            ]
        )
    }

    private func canonicalDefinition(
        _ definition: CreateGraphStatement
    ) -> CreateGraphStatement {
        CreateGraphStatement(
            graphName: definition.graphName,
            vertexTables: definition.vertexTables,
            edgeTables: definition.edgeTables
        )
    }

    private func makeMeter() -> DatabaseWorkMeter {
        DatabaseWorkMeter(
            budget: ExecutionBudget(
                maximumRows: 10_000,
                maximumWorkUnits: 100_000,
                timeoutMilliseconds: 30_000
            )
        )
    }
}
