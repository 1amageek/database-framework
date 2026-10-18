import DatabaseKit
import DatabaseTypes
import StorageKit

/// Transaction-bound SQL key leaves. No ordering or seek capability is exposed.
enum SQLIndexRuntime {
    static func storageName(_ index: SQLIndex) -> String { "sql-index:" + index.id }

    static func physicalLayout() throws -> IndexPhysicalLayout {
        try IndexPhysicalLayout(name: "sql.semantic", revision: 2)
    }

    static func identity(_ index: SQLIndex) throws -> DatabaseIndexStorageIdentity {
        let bytes = try StorageFrameEncoder.encode { writer throws(StorageFrameError) in
            try SQLCatalogCodec.writeIndex(index, into: &writer)
        }
        var digest = SHA256Accumulator()
        digest.update(bytes)
        return try DatabaseIndexStorageIdentity(name: storageName(index),
            definitionFingerprint: SchemaFingerprint(digest.finalize()), layoutFingerprint: physicalLayout().fingerprint)
    }

    static func identities(catalog: SQLCatalog) throws -> [DatabaseIndexStorageIdentity] {
        try catalog.indexes.map(identity)
    }

    static func generation(_ index: SQLIndex, storeSubspace: Subspace) throws -> Subspace {
        let identity = try identity(index)
        return storeSubspace.subspace(SubspaceKey.indexes).subspace(identity.name)
            .subspace(identity.definitionFingerprint.bytes).subspace(identity.layoutFingerprint)
    }

    static func maintain(oldModel: PersistedModel?, newModel: PersistedModel?, catalog: SQLCatalog,
        physicalSchema: Schema, storeSubspace: Subspace, container: DBContainer,
        transaction: any TransactionAccess, workMeter: DatabaseWorkMeter? = nil) async throws {
        guard let model = newModel ?? oldModel,
              let relation = catalog.relations.first(where: { SQLConstraintCompiler.same($0.entityName, model.entity) }),
              let entity = physicalSchema.entities.first(where: { SQLConstraintCompiler.same($0.name, model.entity) }) else { return }
        let indexes = catalog.indexes.filter { SQLConstraintCompiler.same($0.relationID, relation.id) }
        guard !indexes.isEmpty else { return }
        let meter = workMeter ?? DatabaseWorkMeter(budget: ExecutionBudget(), monotonicClock: container.monotonicClock)
        let environment = temporalEnvironment(container)
        for index in indexes {
            try meter.consume(at: .indexScan)
            let compiled = try SQLIndexCompiler.compile(index, relation: relation, physicalSchema: physicalSchema, workMeter: meter)
            let root = try generation(index, storeSubspace: storeSubspace)
            let entries = root.subspace("entries")
            let storage = container.itemStorageFactory.make(transaction: transaction, blobsSubspace: root.subspace("blobs"))
            if let oldModel {
                let id = try identifier(oldModel, entity: entity)
                try await storage.delete(for: entries.pack(id))
            }
            guard let newModel else { continue }
            let row = try await sourceRow(newModel, relation: relation,
                physicalSchema: physicalSchema, environment: environment, workMeter: meter)
            guard let key = try await compiled.key(row: row, environment: environment, workMeter: meter) else { continue }
            let id = try identifier(newModel, entity: entity)
            if index.unique, index.nulls == .notDistinct || !key.hasNull {
                let range = entries.range()
                try await storage.consumeRetainedScan(begin: range.begin, end: range.end,
                    snapshot: false, limit: try meter.storageReadLimitWithSentinel(at: .indexScan),
                    workMeter: meter, stage: .indexScan) { otherID, bytes in
                    try meter.consume(at: .indexScan)
                    guard otherID != entries.pack(id) else { return }
                    let other = try await decodeKey(bytes, compiled: compiled, physicalSchema: physicalSchema,
                        environment: environment, workMeter: meter)
                    if try key.equal(to: other, workMeter: meter) {
                        throw SQLConstraintError.violation(kind: .unique, relation: relation, constraint: index.name, catalog: catalog)
                    }
                }
            }
            // Immutable expression evaluation consumes the canonical source
            // model. Duplicating its DBRC bytes is the required leaf persistence
            // boundary; ItemStorage chunks them within this exact generation.
            let byteCount = try PersistableStorageCodec.encodedByteCount(newModel)
            let claim = try meter.reserveIntermediate(bytes: UInt64(byteCount), at: .indexScan)
            defer { claim.release() }
            try DatabaseByteProcessingMeter.consume(byteCount: byteCount, passes: 2, workMeter: meter, stage: .indexScan)
            let payload = try PersistableStorageCodec.encode(newModel)
            try await storage.write(payload, for: entries.pack(id))
        }
    }

    static func rebuild(sourceCatalog: SQLCatalog, targetCatalog: SQLCatalog, physicalSchema: Schema,
        storeSubspace: Subspace, container: DBContainer, runtime: EntityRuntimeRegistration,
        transaction: any TransactionAccess, workMeter: DatabaseWorkMeter, skipBackfill: Bool = false) async throws {
        let sourceRelation = sourceCatalog.relations.first { SQLConstraintCompiler.same($0.entityName, runtime.entity.name) }
        let targetRelation = targetCatalog.relations.first { SQLConstraintCompiler.same($0.entityName, runtime.entity.name) }
        let source = sourceCatalog.indexes.filter { SQLConstraintCompiler.same($0.relationID, sourceRelation?.id ?? "") }
        let target = targetCatalog.indexes.filter { SQLConstraintCompiler.same($0.relationID, targetRelation?.id ?? "") }
        for index in source where !target.contains(index) {
            try workMeter.consume(at: .indexScan)
            let identity = try identity(index)
            try IndexStorageRetirer.retire(indexName: identity.name,
                selection: .physicalGeneration(definitionFingerprint: identity.definitionFingerprint,
                    layoutFingerprint: identity.layoutFingerprint), storeSubspace: storeSubspace, transaction: transaction)
        }
        let additions = target.filter { !source.contains($0) }
        guard !skipBackfill, !additions.isEmpty else { return }
        // Build only additions. Existing leaf generations retain their exact bytes.
        let buildCatalog = try SQLCatalog(namespaces: targetCatalog.namespaces, relations: targetCatalog.relations,
            sequences: targetCatalog.sequences, views: targetCatalog.views,
            materializedViews: targetCatalog.materializedViews, indexes: additions)
        let range = storeSubspace.subspace(SubspaceKey.items).subspace(runtime.entity.name).range()
        let storage = container.itemStorageFactory.make(transaction: transaction,
            blobsSubspace: storeSubspace.subspace(SubspaceKey.blobs))
        try await storage.consumeRetainedScan(begin: range.begin, end: range.end, snapshot: false,
            limit: try workMeter.storageReadLimitWithSentinel(at: .storageRow), workMeter: workMeter, stage: .storageRow) { _, bytes in
            try workMeter.consume(at: .storageRow)
            let retained = try DatabaseRetainedStoredModel.decode(bytes, entity: runtime.entity.name,
                runtime: runtime, workMeter: workMeter, stage: .storageRow)
            var model: PersistedModel?
            retained.withModel { model = copy $0 }
            guard let model else { throw StorageFrameError.invalidValue }
            try await maintain(oldModel: nil, newModel: model, catalog: buildCatalog,
                physicalSchema: physicalSchema, storeSubspace: storeSubspace, container: container,
                transaction: transaction, workMeter: workMeter)
            withExtendedLifetime(retained) {}
        }
    }

    static func primaryKeys(index: SQLIndex, storeSubspace: Subspace,
        transaction: any TransactionReadAccess, workMeter: DatabaseWorkMeter) async throws -> DatabaseRetainedPrimaryKeys {
        let entries = try generation(index, storeSubspace: storeSubspace).subspace("entries")
        let range = entries.range()
        var keys = try DatabaseRetainedArrayBuilder<DatabaseRetainedPrimaryKey>(workMeter: workMeter,
            stage: .indexScan, layout: DatabaseRetainedArrayLayout.forElement(DatabaseRetainedPrimaryKey.self))
        var cursor = transaction.rangeCursor(from: .firstGreaterOrEqual(range.begin), to: .firstGreaterOrEqual(range.end),
            limit: try workMeter.storageReadLimitWithSentinel(at: .indexScan), reverse: false,
            snapshot: true, streamingMode: .small)
        try await cursor.consume { key, _ in
            try workMeter.consume(at: .indexScan)
            let claim = try workMeter.reserveIntermediate(bytes: UInt64(key.count), at: .indexScan)
            var tupleCursor = try entries.tupleCursor(for: key)
            let id = try tupleCursor.remainingTuple { byteCount in
                try claim.reserveAdditional(bytes: UInt64(byteCount), at: .indexScan)
            }
            guard !id.isEmpty else { throw CanonicalReadError.corruptedIndexEntry(indexName: index.name, reason: "SQL index has no primary identifier") }
            let retained = DatabaseRetainedPrimaryKey(value: id, reservation: claim)
            try keys.append(footprint: DatabaseIntermediateFootprint(rows: 1), make: { retained })
        }
        return try DatabaseRetainedPrimaryKeys(buffer: keys.finish())
    }

    private static func identifier(_ model: PersistedModel, entity: Schema.Entity) throws -> Tuple {
        try PersistableIdentifierKeyCodec.tuple(for: EntityRuntimeDefinition.identity(for: model, entity: entity),
            expectedType: entity.identifierType)
    }

    private static func fields(_ model: PersistedModel) throws -> FieldObject {
        try FieldObject(model.fields.map { (key: $0.name, value: $0.value) })
    }

    private static func sourceRow(_ model: PersistedModel, relation: SQLRelation, physicalSchema: Schema,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) async throws -> CanonicalSourceRow {
        let metadata = try DatabaseIntermediateCollectionMeter.arrayFootprint(count: model.fields.count,
            element: (key: String, value: FieldValue).self).multiplied(by: 2)
        let claim = try workMeter.reserveIntermediate(bytes: metadata.bytes, at: .indexScan)
        defer { claim.release() }
        return try await SQLConstraintRows.row(fields: fields(model), relation: relation,
            physicalSchema: physicalSchema, environment: environment, workMeter: workMeter)
    }

    static func temporalEnvironment(_ container: DBContainer) -> SQLTemporalEnvironment {
        if let binding = ActiveSQLTemporalStatement.binding, binding.container === container { return binding.environment }
        let instant = container.wallClock.now
        return SQLTemporalEnvironment(transactionInstant: instant, statementInstant: instant,
            wallClock: container.wallClock, configuration: container.configuration.sqlTemporal)
    }

    private static func decodeKey(_ bytes: ByteString, compiled: SQLCompiledIndex, physicalSchema: Schema,
        environment: SQLTemporalEnvironment, workMeter: DatabaseWorkMeter) async throws -> SQLIndexKey {
        try DatabaseByteProcessingMeter.consume(byteCount: bytes.count, passes: 2, workMeter: workMeter, stage: .indexScan)
        let size = try PersistableStorageCodec.decodedFootprint(bytes, expectedEntity: compiled.relation.entityName)
        let claim = try workMeter.reserveIntermediate(bytes: try DatabaseIntermediateFootprint(bytes: size.retainedByteCount)
            .adding(DatabaseIntermediateFootprint(bytes: size.transientByteCount)).bytes, at: .indexScan)
        defer { claim.release() }
        let decoded = try PersistableStorageCodec.decodePersistedModel(from: bytes, expectedEntity: compiled.relation.entityName)
        let row = try await sourceRow(decoded, relation: compiled.relation,
            physicalSchema: physicalSchema, environment: environment, workMeter: workMeter)
        guard let key = try await compiled.key(row: row, environment: environment, workMeter: workMeter) else {
            throw CanonicalReadError.corruptedIndexEntry(indexName: compiled.declaration.name,
                reason: "SQL index input is outside its declared partial predicate")
        }
        return key
    }
}
