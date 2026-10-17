import DatabaseKit
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing
@testable import DatabaseEngine

@Persistable
private struct S13ProtectedCatalogItem {
    #Directory<S13ProtectedCatalogItem>("s13-protected-catalog")
    var id: String = ""
    var value: String = ""
}

@Suite("S13 SQL catalog metadata persistence and protection")
struct S13SQLCatalogTests {
    @Test("All constraint, policy, backing and sequence declarations round trip")
    func completeRoundTrip() throws {
        let fixture = try fixture()
        let state = try state(fixture.catalog, schema: fixture.schema)
        let decoded = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(state))
        #expect(decoded.catalog == fixture.catalog)
        #expect(decoded.digest == state.digest)
        let snapshot = try decoded.snapshot(physicalSchema: fixture.schema,
            runtimeConfiguration: runtime(fixture.schema))
        let relation = try snapshot.resolveRelation(name: "items", namespace: "public",
            searchPath: .default, currentUser: nil)
        let virtual = try #require(relation.columns.first { $0.declaration.id == "virtual" })
        #expect(virtual.field == nil)
        #expect(virtual.type == .sql(.integer))
        #expect(virtual.isNullable)
        #expect(relation.columns.compactMap { $0.field?.fieldNumber } == [1, 2, 3])
        #expect(try SQLCatalogCodec.encode(snapshot) == SQLCatalogCodec.encode(state))
    }

    @Test("SQL metadata changes catalog identity without changing the physical fingerprint")
    func digestSensitivity() throws {
        let fixture = try fixture()
        let original = try state(fixture.catalog, schema: fixture.schema)
        let relation = fixture.catalog.relations[0]
        var columns = relation.columns
        columns[1] = try SQLColumn(id: columns[1].id, name: columns[1].name,
            backing: columns[1].backing, policy: .input(defaultExpression: .literal(.int(8))))
        let policies = try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
            name: relation.name, entityName: relation.entityName, columns: columns,
            primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
            primaryKeyConstraint: relation.primaryKeyConstraint, constraints: relation.constraints)
        let changedPolicy = try SQLCatalog(namespaces: fixture.catalog.namespaces,
            relations: [policies], sequences: fixture.catalog.sequences)
        let sequence = fixture.catalog.sequences[0]
        let changedSequence = try SQLSequence(id: sequence.id, namespaceID: sequence.namespaceID,
            name: sequence.name, start: 2, increment: sequence.increment,
            minimum: sequence.minimum, maximum: sequence.maximum, cycle: sequence.cycle,
            ownedBy: sequence.ownedBy)
        let changedSequences = try SQLCatalog(namespaces: fixture.catalog.namespaces,
            relations: fixture.catalog.relations, sequences: [changedSequence])
        let key = try SQLConstraintAttributes(id: "pk", name: "renamed_pk")
        let changedKey = try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
            name: relation.name, entityName: relation.entityName, columns: relation.columns,
            primaryKeyColumnIDs: relation.primaryKeyColumnIDs, primaryKeyConstraint: key,
            constraints: relation.constraints)
        for catalog in [changedPolicy, changedSequences,
            try SQLCatalog(namespaces: fixture.catalog.namespaces,
                relations: [changedKey], sequences: fixture.catalog.sequences)] {
            let changed = try state(catalog, schema: fixture.schema)
            #expect(changed.digest != original.digest)
            #expect(changed.physicalSchemaFingerprint == original.physicalSchemaFingerprint)
        }
    }

    @Test("Every truncated rich-catalog prefix is rejected")
    func truncatedRichFrames() throws {
        let fixture = try fixture()
        let bytes = try SQLCatalogCodec.encode(state(fixture.catalog, schema: fixture.schema))
        for end in 0..<bytes.count {
            #expect(throws: (any Error).self) {
                _ = try SQLCatalogCodec.decode(bytes[bytes.startIndex..<(bytes.startIndex + end)])
            }
        }
    }

    @Test("Version, backing tags and frame budgets reject invalid rich frames")
    func rejectedRichFrames() throws {
        let fixture = try fixture()
        let state = try state(fixture.catalog, schema: fixture.schema)
        let bytes = try SQLCatalogCodec.encode(state)
        var changed = Array(bytes)
        changed[4] = 1
        #expect(throws: StorageFrameError.unsupportedVersion(1)) {
            _ = try SQLCatalogCodec.decode(ByteString(changed))
        }
        var reader = try StorageFrameDecoder(bytes)
        _ = try reader.readUInt32(); _ = try reader.readUInt16(); _ = try reader.readBytes()
        let namespaces = try reader.readCount()
        for _ in 0..<namespaces { _ = try reader.readString(); _ = try reader.readString() }
        _ = try reader.readCount()
        for _ in 0..<4 { _ = try reader.readString() }
        _ = try reader.readCount(); _ = try reader.readString(); _ = try reader.readString()
        changed = Array(bytes)
        changed[bytes.count - reader.remainingCount] = 255
        #expect(throws: StorageFrameError.invalidValueTag(255)) {
            _ = try SQLCatalogCodec.decode(ByteString(changed))
        }
        let limits = try StorageFrameLimits(maximumFrameBytes: bytes.count - 1,
            maximumStringBytes: bytes.count, maximumByteStringBytes: bytes.count,
            maximumCollectionCount: 100, maximumNestingDepth: 64)
        #expect(throws: StorageFrameError.self) { _ = try SQLCatalogCodec.decode(bytes, limits: limits) }
        #expect(throws: StorageFrameError.self) { _ = try SQLCatalogCodec.encode(state, limits: limits) }
    }

    @Test("Persistence, restart and application reconciliation preserve SQL metadata")
    func reconciliationAndRestart() async throws {
        let fixture = try fixture()
        let snapshot = try SQLCatalogSnapshot(catalog: fixture.catalog, physicalSchema: fixture.schema,
            runtimeConfiguration: runtime(fixture.schema))
        let engine = InMemoryEngine()
        defer { await engine.shutdown() }
        let store = SQLCatalogStore(root: Subspace(prefix: ByteString([0x13, 0xCA])))
        try await engine.withTransaction { transaction in
            try store.persist(snapshot, transaction: transaction)
        }
        let stored = try await engine.withTransaction { transaction in
            let loaded = try await store.load(transaction: transaction)
            return try #require(loaded)
        }
        let reopened = try stored.snapshot(physicalSchema: fixture.schema,
            runtimeConfiguration: runtime(fixture.schema))
        #expect(reopened.catalog == fixture.catalog)
        let oldEntity = fixture.schema.entities[0]
        let targetEntity = try Schema.Entity(name: oldEntity.name, identifierType: oldEntity.identifierType,
            fields: oldEntity.fields + [FieldSchema(name: "added", fieldNumber: 4, type: .string)],
            directoryComponents: oldEntity.directoryComponents)
        let target = try Schema(entities: [targetEntity], version: .init(1, 1, 0))
        let reconciled = try stored.reconciledForApplication(physicalSchema: fixture.schema,
            targetPhysicalSchema: target, runtimeConfiguration: runtime(target))
        #expect(reconciled.catalog.sequences == fixture.catalog.sequences)
        #expect(reconciled.catalog.relations[0].constraints == fixture.catalog.relations[0].constraints)
        #expect(reconciled.catalog.relations[0].primaryKeyConstraint == fixture.catalog.relations[0].primaryKeyConstraint)
        #expect(Array(reconciled.catalog.relations[0].columns.prefix(4)) == fixture.catalog.relations[0].columns)
        let migration = try stored.reconciledForMigration(physicalSchema: fixture.schema,
            targetPhysicalSchema: target)
        #expect(try SQLCatalogCodec.decode(SQLCatalogCodec.encode(migration)).catalog == reconciled.catalog)
    }

    @Test("Compiled protection retains authoritative SQL policy and allows true virtual additions")
    func protectedPoliciesAndVirtualAddition() throws {
        let definition = try EntityRuntimeDefinition(S13ProtectedCatalogItem.self)
        let schema = try Schema(entities: [definition.entity], version: .init(1, 0, 0))
        let runtime = try DatabaseRuntimeConfiguration(executionIdentity: .init(identifier: "s13-protected", revision: 1),
            entityRuntimes: [definition.registration()])
        let derived = try SQLCatalogSnapshot.derivedCatalog(physicalSchema: schema)
        let old = derived.relations[0]
        let value = try #require(old.columns.first { $0.name == "value" })
        let columns = try old.columns.map { column in
            try SQLColumn(id: column.id, name: column.name, backing: column.backing,
                policy: column.id == value.id ? .input(defaultExpression: .literal(.string("initial"))) : column.policy)
        }
        let declared = try SQLRelation(id: old.id, namespaceID: old.namespaceID, name: old.name,
            entityName: old.entityName, columns: columns, primaryKeyColumnIDs: old.primaryKeyColumnIDs,
            primaryKeyConstraint: old.primaryKeyConstraint,
            constraints: [SQLConstraint(attributes: SQLConstraintAttributes(id: "protected-check", name: "valid"),
                kind: .check(.literal(.bool(true))))])
        let catalog = try SQLCatalog(namespaces: derived.namespaces, relations: [declared])
        let snapshot = try SQLCatalogSnapshot(catalog: catalog, physicalSchema: schema, runtimeConfiguration: runtime)
        let persisted = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(snapshot))
        let dynamic = try self.runtime(schema)
        let restored = try persisted.snapshot(physicalSchema: schema, runtimeConfiguration: dynamic)
        let virtual = try SQLColumn(id: "virtual", name: "computed", backing: .virtual(type: .sql(.text)),
            policy: .generated(.literal(.string("computed"))))
        let addition = try SQLRelation(id: declared.id, namespaceID: declared.namespaceID, name: declared.name,
            entityName: declared.entityName, columns: declared.columns + [virtual],
            primaryKeyColumnIDs: declared.primaryKeyColumnIDs, primaryKeyConstraint: declared.primaryKeyConstraint,
            constraints: declared.constraints)
        let replaced = try restored.replacingCatalog(SQLCatalog(namespaces: catalog.namespaces, relations: [addition]),
            physicalSchema: schema, runtimeConfiguration: dynamic)
        #expect(replaced.physicalSchemaFingerprint == restored.physicalSchemaFingerprint)
        #expect(replaced.protectedContracts == restored.protectedContracts)
        let rebound = try replaced.resolveRelation(name: declared.name, namespace: "public", searchPath: .default, currentUser: nil)
        #expect(try #require(rebound.columns.last).field == nil)
        let reset = try SQLRelation(id: declared.id, namespaceID: declared.namespaceID, name: declared.name,
            entityName: declared.entityName, columns: old.columns,
            primaryKeyColumnIDs: declared.primaryKeyColumnIDs, primaryKeyConstraint: declared.primaryKeyConstraint,
            constraints: declared.constraints)
        #expect(throws: SQLCatalogBindingError.protectedColumnPolicyChanged(relation: declared.id, column: value.id)) {
            _ = try restored.replacingCatalog(SQLCatalog(namespaces: catalog.namespaces, relations: [reset]),
                physicalSchema: schema, runtimeConfiguration: dynamic)
        }
        let changedConstraint = try SQLConstraint(attributes: declared.constraints[0].attributes,
            kind: .check(.literal(.bool(false))))
        let constrained = try SQLRelation(id: declared.id, namespaceID: declared.namespaceID, name: declared.name,
            entityName: declared.entityName, columns: declared.columns,
            primaryKeyColumnIDs: declared.primaryKeyColumnIDs, primaryKeyConstraint: declared.primaryKeyConstraint,
            constraints: [changedConstraint])
        #expect(throws: SQLCatalogBindingError.protectedConstraintsChanged(declared.id)) {
            _ = try restored.replacingCatalog(SQLCatalog(namespaces: catalog.namespaces, relations: [constrained]),
                physicalSchema: schema, runtimeConfiguration: dynamic)
        }
    }

    private func fixture() throws -> (schema: Schema, catalog: SQLCatalog) {
        let entity = try Schema.Entity(name: "S13Item", identifierType: .int64, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .int64),
            FieldSchema(name: "value", fieldNumber: 2, type: .int64, isOptional: true),
            FieldSchema(name: "stored", fieldNumber: 3, type: .int64)],
            directoryComponents: [.staticPath("s13-items")])
        let schema = try Schema(entities: [entity], version: .init(1, 0, 0))
        let columns = try [
            SQLColumn(id: "id", name: "id", backing: .field(number: 1),
                policy: .identity(SQLIdentityColumn(sequenceID: "sequence", generation: .byDefault))),
            SQLColumn(id: "value", name: "value", backing: .field(number: 2),
                policy: .input(defaultExpression: .literal(.int(7)))),
            SQLColumn(id: "stored", name: "stored", backing: .field(number: 3), policy: .generated(.literal(.int(11)))),
            SQLColumn(id: "virtual", name: "virtual", backing: .virtual(type: .sql(.integer)), policy: .generated(.literal(.int(12))))]
        let constraints = try [
            SQLConstraint(attributes: SQLConstraintAttributes(id: "unique", name: "value_unique"),
                kind: .unique(columnIDs: ["value"], nulls: .notDistinct)),
            SQLConstraint(attributes: SQLConstraintAttributes(id: "not-null", name: "stored_not_null"), kind: .notNull(columnID: "stored")),
            SQLConstraint(attributes: SQLConstraintAttributes(id: "check", name: "valid"), kind: .check(.literal(.bool(true)))),
            SQLConstraint(attributes: SQLConstraintAttributes(id: "foreign", name: "self_reference",
                timing: .deferrable(initiallyDeferred: true)),
                kind: .foreignKey(SQLForeignKey(columnIDs: ["value"], referencedRelationID: "items",
                    referencedColumnIDs: ["id"], match: .full, onUpdate: .cascade,
                    onDelete: .setDefault, onDeleteColumnIDs: ["value"])))]
        let relation = try SQLRelation(id: "items", namespaceID: "public", name: "items", entityName: entity.name,
            columns: columns, primaryKeyColumnIDs: ["id"],
            primaryKeyConstraint: SQLConstraintAttributes(id: "pk", name: "items_pk"), constraints: constraints)
        let sequence = try SQLSequence(id: "sequence", namespaceID: "public", name: "item_sequence",
            start: 1, increment: 1, minimum: Int64.min, maximum: Int64.max,
            ownedBy: SQLSequenceOwner(relationID: "items", columnID: "id"))
        return try (schema, SQLCatalog(namespaces: [SQLNamespace(id: "public", name: "public")],
            relations: [relation], sequences: [sequence]))
    }

    private func state(_ catalog: SQLCatalog, schema: Schema) throws -> SQLCatalogStoredState {
        let fingerprint = try SchemaManifest(schema: schema).fingerprint()
        let bytes = try SQLCatalogCodec.canonicalBytes(catalog, fingerprint: fingerprint, protectedContracts: [])
        var hash = SHA256Accumulator()
        hash.update(bytes)
        return SQLCatalogStoredState(catalog: catalog, physicalSchemaFingerprint: fingerprint,
            protectedContracts: [], digest: hash.finalize())
    }

    private func runtime(_ schema: Schema) throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(executionIdentity: .init(identifier: "s13-catalog", revision: 1),
            entityRuntimes: schema.entities.map { EntityRuntimeDefinition(schemaDriven: $0).registration() })
    }
}
