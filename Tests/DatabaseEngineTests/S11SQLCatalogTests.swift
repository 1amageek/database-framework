import DatabaseKit
import DatabaseTypes
import DatabaseWire
import StorageKit
import Testing
@testable import DatabaseEngine

@Persistable
private struct S11CompiledCatalogEntity {
    #Directory<S11CompiledCatalogEntity>("s11-compiled-catalog")
    var id: String = ""
    var value: String = ""
}

@Suite("S11 SQL catalog binding and durable format")
struct S11SQLCatalogTests {
    @Test("Default mappings preserve physical names, numbers and visible identity")
    func defaultMapping() throws {
        let fixture = try fixture()
        let relation = try fixture.snapshot.resolveRelation(name: "Item", namespace: nil,
            searchPath: .default, currentUser: nil)
        #expect(relation.entity.name == "Item")
        #expect(relation.columns.map(\.field.name) == ["id", "value"])
        #expect(relation.columns.map(\.declaration.fieldNumber) == [1, 2])
        #expect(relation.declaration.primaryKeyColumnIDs == ["field:1"])
        #expect(relation.entity.directoryComponents == [.staticPath("s11-items")])
    }

    @Test("Current user, missing schemas and qualified names select exact namespaces")
    func searchPath() throws {
        let first = try entity("Item")
        let second = try entity("Other")
        let schema = try Schema(entities: [first, second], version: .init(1, 0, 0))
        let columns = try [SQLColumn(id: "v", name: "value", fieldNumber: 2)]
        let catalog = try SQLCatalog(namespaces: [SQLNamespace(id: "u", name: "alice"),
            SQLNamespace(id: "p", name: "public")], relations: [
                SQLRelation(id: "a", namespaceID: "u", name: "items", entityName: "Item", columns: columns),
                SQLRelation(id: "b", namespaceID: "p", name: "items", entityName: "Other", columns: columns)])
        let snapshot = try SQLCatalogSnapshot(catalog: catalog, physicalSchema: schema,
            runtimeConfiguration: runtime(schema))
        #expect(try snapshot.resolveRelation(name: "items", namespace: nil,
            searchPath: .default, currentUser: "alice").entity.name == "Item")
        #expect(try snapshot.resolveRelation(name: "items", namespace: nil,
            searchPath: .default, currentUser: nil).entity.name == "Other")
        let path = try SQLSearchPath(entries: [.schema("missing"), .schema("public")])
        #expect(try snapshot.resolveRelation(name: "items", namespace: nil,
            searchPath: path, currentUser: nil).entity.name == "Other")
        #expect(throws: SQLCatalogBindingError.missingNamespace("missing")) {
            _ = try snapshot.resolveRelation(name: "items", namespace: "missing",
                searchPath: .default, currentUser: nil)
        }
        #expect(throws: SQLCatalogBindingError.missingRelation(namespace: "public", name: "Item")) {
            _ = try snapshot.resolveRelation(name: "Item", namespace: "public",
                searchPath: .default, currentUser: nil)
        }
    }

    @Test("Canonically equivalent Unicode SQL names retain distinct byte identities")
    func exactUnicode() throws {
        let fixture = try fixture()
        let composed = "\u{00E9}"
        let decomposed = "e\u{0301}"
        let catalog = try SQLCatalog(namespaces: [SQLNamespace(id: composed, name: composed),
            SQLNamespace(id: decomposed, name: decomposed)], relations: [
                SQLRelation(id: composed, namespaceID: composed, name: composed, entityName: "Item",
                    columns: [SQLColumn(id: composed, name: composed, fieldNumber: 2)])])
        let snapshot = try SQLCatalogSnapshot(catalog: catalog, physicalSchema: fixture.schema,
            runtimeConfiguration: fixture.runtime)
        #expect(try snapshot.resolveRelation(name: composed, namespace: composed,
            searchPath: .default, currentUser: nil).declaration.name.utf8.elementsEqual(composed.utf8))
        #expect(throws: SQLCatalogBindingError.self) {
            _ = try snapshot.resolveRelation(name: decomposed, namespace: composed,
                searchPath: .default, currentUser: nil)
        }
        #expect(throws: SQLCatalogBindingError.self) {
            _ = try snapshot.resolveRelation(name: composed, namespace: decomposed,
                searchPath: .default, currentUser: nil)
        }
    }

    @Test("Durable round trip preserves every declared array and ordered key")
    func exactRoundTrip() throws {
        let fixture = try fixture()
        let catalog = try SQLCatalog(namespaces: [SQLNamespace(id: "z", name: "z"),
            SQLNamespace(id: "a", name: "a")], relations: [
                SQLRelation(id: "r", namespaceID: "a", name: "items", entityName: "Item",
                    columns: [SQLColumn(id: "v", name: "text", fieldNumber: 2),
                        SQLColumn(id: "i", name: "key", fieldNumber: 1)], primaryKeyColumnIDs: ["v", "i"])])
        let snapshot = try SQLCatalogSnapshot(catalog: catalog, physicalSchema: fixture.schema,
            runtimeConfiguration: fixture.runtime)
        let encoded = try SQLCatalogCodec.encode(snapshot)
        let decoded = try SQLCatalogCodec.decode(encoded)
        #expect(decoded.catalog == catalog)
        #expect(decoded.digest == snapshot.digest)
        #expect(try SQLCatalogCodec.encode(decoded.snapshot(physicalSchema: fixture.schema,
            runtimeConfiguration: fixture.runtime)) == encoded)
        let reordered = try SQLCatalog(namespaces: catalog.namespaces.reversed(), relations: catalog.relations)
        let other = try SQLCatalogSnapshot(catalog: reordered, physicalSchema: fixture.schema,
            runtimeConfiguration: fixture.runtime)
        #expect(other.digest != snapshot.digest)
        #expect(other.physicalSchemaFingerprint == snapshot.physicalSchemaFingerprint)
    }

    @Test("Every truncated prefix is rejected without reserving unproven collections")
    func truncatedPrefixes() throws {
        let encoded = try SQLCatalogCodec.encode(fixture().snapshot)
        for end in 0..<encoded.count {
            let prefix = encoded[encoded.startIndex..<(encoded.startIndex + end)]
            #expect(throws: (any Error).self) { _ = try SQLCatalogCodec.decode(prefix) }
        }
    }

    @Test("Unknown headers, tags, invalid UTF8, trailing bytes and corrupt digest fail")
    func malformedFrames() throws {
        let snapshot = try fixture().snapshot
        let encoded = try SQLCatalogCodec.encode(snapshot)
        var bytes = Array(encoded)
        bytes[0] ^= 1
        #expect(throws: StorageFrameError.invalidMagic) { _ = try SQLCatalogCodec.decode(ByteString(bytes)) }
        bytes = Array(encoded)
        bytes[4] = 255
        #expect(throws: StorageFrameError.unsupportedVersion(255)) { _ = try SQLCatalogCodec.decode(ByteString(bytes)) }
        bytes = Array(encoded)
        let marker = Array("public".utf8)
        let offset = try #require((0...(bytes.count - marker.count)).first { Array(bytes[$0..<($0 + marker.count)]) == marker })
        bytes[offset] = 255
        #expect(throws: StorageFrameError.invalidUTF8) { _ = try SQLCatalogCodec.decode(ByteString(bytes)) }
        bytes = Array(encoded)
        bytes.append(0)
        #expect(throws: StorageFrameError.trailingBytes) { _ = try SQLCatalogCodec.decode(ByteString(bytes)) }
        bytes = Array(encoded)
        bytes[bytes.count - 1] ^= 1
        #expect(throws: SQLCatalogBindingError.digestMismatch) { _ = try SQLCatalogCodec.decode(ByteString(bytes)) }
        let unknown = try StorageFrameEncoder.encode { writer throws(StorageFrameError) in
            writer.writeUInt32(0x434C_5153)
            writer.writeUInt16(1)
            try writer.writeBytes(snapshot.physicalSchemaFingerprint.bytes)
            try writer.writeCount(0)
            try writer.writeCount(0)
            try writer.writeCount(1)
            writer.writeUInt8(255)
            try writer.writeString("r")
            try writer.writeString("Item")
            try writer.writeBytes(snapshot.digest)
            try writer.writeBytes(snapshot.digest)
        }
        #expect(throws: StorageFrameError.invalidValueTag(255)) { _ = try SQLCatalogCodec.decode(unknown) }
    }

    @Test("Exact frame budget succeeds and one-byte-lower bound rejects encode and decode")
    func frameBounds() throws {
        let fixture = try fixture()
        let encoded = try SQLCatalogCodec.encode(fixture.snapshot)
        let exact = try limits(frame: encoded.count)
        #expect(try SQLCatalogCodec.encode(fixture.snapshot, limits: exact) == encoded)
        #expect(try SQLCatalogCodec.decode(encoded, limits: exact).catalog == fixture.snapshot.catalog)
        let short = try limits(frame: encoded.count - 1)
        #expect(throws: StorageFrameError.self) { _ = try SQLCatalogCodec.encode(fixture.snapshot, limits: short) }
        #expect(throws: StorageFrameError.self) { _ = try SQLCatalogCodec.decode(encoded, limits: short) }
        let noCollections = try StorageFrameLimits(maximumFrameBytes: encoded.count,
            maximumStringBytes: encoded.count, maximumByteStringBytes: encoded.count,
            maximumCollectionCount: 0, maximumNestingDepth: 0)
        #expect(throws: StorageFrameError.self) { _ = try SQLCatalogCodec.decode(encoded, limits: noCollections) }
        #expect(throws: StorageFrameError.self) {
            _ = try SQLCatalogSnapshot(catalog: fixture.snapshot.catalog, physicalSchema: fixture.schema,
                runtimeConfiguration: fixture.runtime, limits: noCollections)
        }
    }

    @Test("Physical mappings, runtime contracts and restored schema identity are checked")
    func backingFailures() throws {
        let fixture = try fixture()
        let missing = try SQLRelation(id: "r", namespaceID: "public", name: "x", entityName: "Missing", columns: [])
        #expect(throws: SQLCatalogBindingError.missingEntity("Missing")) {
            _ = try SQLCatalogSnapshot(catalog: SQLCatalog(namespaces: fixture.snapshot.catalog.namespaces,
                relations: [missing]), physicalSchema: fixture.schema, runtimeConfiguration: fixture.runtime)
        }
        let invalid = try SQLRelation(id: "r", namespaceID: "public", name: "x", entityName: "Item",
            columns: [SQLColumn(id: "bad", name: "bad", fieldNumber: 999)])
        #expect(throws: SQLCatalogBindingError.missingField(entity: "Item", number: 999)) {
            _ = try SQLCatalogSnapshot(catalog: SQLCatalog(namespaces: fixture.snapshot.catalog.namespaces,
                relations: [invalid]), physicalSchema: fixture.schema, runtimeConfiguration: fixture.runtime)
        }
        let wrong = try Schema(entities: [entity("Item", valueType: .int64)], version: .init(1, 0, 0))
        #expect(throws: SQLCatalogBindingError.runtimeContractMismatch("Item")) {
            _ = try SQLCatalogSnapshot(catalog: fixture.snapshot.catalog, physicalSchema: wrong,
                runtimeConfiguration: fixture.runtime)
        }
        let stored = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(fixture.snapshot))
        #expect(throws: SQLCatalogBindingError.physicalFingerprintMismatch) {
            _ = try stored.snapshot(physicalSchema: wrong, runtimeConfiguration: runtime(wrong))
        }
    }

    @Test("Compiled protection survives schema-driven restore and permits pure logical rename")
    func durableCompiledProtection() throws {
        let fixture = try compiledFixture()
        #expect(fixture.snapshot.protectedContracts.count == 1)
        let stored = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(fixture.snapshot))
        let dynamic = try runtime(fixture.schema)
        let restored = try stored.snapshot(physicalSchema: fixture.schema, runtimeConfiguration: dynamic)
        #expect(restored.protectedContracts == fixture.snapshot.protectedContracts)
        let relation = try #require(restored.catalog.relations.first)
        let renamed = try SQLRelation(id: relation.id, namespaceID: "renamed", name: "renamed",
            entityName: relation.entityName, columns: relation.columns.map {
                try SQLColumn(id: $0.id, name: "renamed_\($0.fieldNumber)", fieldNumber: $0.fieldNumber)
            }, primaryKeyColumnIDs: relation.primaryKeyColumnIDs)
        let candidate = try SQLCatalog(namespaces: [SQLNamespace(id: "renamed", name: "renamed")], relations: [renamed])
        let replacement = try restored.replacingCatalog(candidate, physicalSchema: fixture.schema,
            runtimeConfiguration: dynamic)
        #expect(replacement.protectedContracts == restored.protectedContracts)
        #expect(replacement.digest != restored.digest)
    }

    @Test("Protected relation removal, column omission, field retarget and key changes fail")
    func protectedTransitions() throws {
        let fixture = try compiledFixture()
        let old = try #require(fixture.snapshot.catalog.relations.first)
        let columns = old.columns
        let variants = try [
            SQLRelation(id: old.id, namespaceID: old.namespaceID, name: old.name, entityName: old.entityName,
                columns: Array(columns.dropLast()), primaryKeyColumnIDs: nil),
            SQLRelation(id: old.id, namespaceID: old.namespaceID, name: old.name, entityName: old.entityName,
                columns: columns.reversed().enumerated().map { index, column in
                    try SQLColumn(id: columns[index].id, name: column.name, fieldNumber: column.fieldNumber)
                }, primaryKeyColumnIDs: old.primaryKeyColumnIDs),
            SQLRelation(id: old.id, namespaceID: old.namespaceID, name: old.name, entityName: old.entityName,
                columns: columns, primaryKeyColumnIDs: nil)]
        for variant in variants {
            #expect(throws: SQLCatalogBindingError.self) {
                _ = try fixture.snapshot.replacingCatalog(SQLCatalog(namespaces: fixture.snapshot.catalog.namespaces,
                    relations: [variant]), physicalSchema: fixture.schema, runtimeConfiguration: fixture.runtime)
            }
        }
        #expect(throws: SQLCatalogBindingError.protectedRelationRemoved(old.id)) {
            _ = try fixture.snapshot.replacingCatalog(SQLCatalog(namespaces: fixture.snapshot.catalog.namespaces,
                relations: []), physicalSchema: fixture.schema, runtimeConfiguration: fixture.runtime)
        }
    }

    @Test("Surviving dynamic IDs cannot change physical meaning but removal is permitted")
    func dynamicIdentity() throws {
        let fixture = try fixture()
        let old = try #require(fixture.snapshot.catalog.relations.first)
        let retarget = try SQLRelation(id: old.id, namespaceID: old.namespaceID, name: old.name,
            entityName: old.entityName, columns: [SQLColumn(id: old.columns[0].id, name: "value", fieldNumber: 2)])
        #expect(throws: SQLCatalogBindingError.columnRetargeted(relation: old.id, column: old.columns[0].id)) {
            _ = try fixture.snapshot.replacingCatalog(SQLCatalog(namespaces: fixture.snapshot.catalog.namespaces,
                relations: [retarget]), physicalSchema: fixture.schema, runtimeConfiguration: fixture.runtime)
        }
        let removed = try fixture.snapshot.replacingCatalog(SQLCatalog(namespaces: fixture.snapshot.catalog.namespaces,
            relations: []), physicalSchema: fixture.schema, runtimeConfiguration: fixture.runtime)
        #expect(removed.catalog.relations.isEmpty)
    }

    @Test("Application reconciliation preserves hidden fields and rejects implicit key loss")
    func reconciliation() throws {
        let fixture = try fixture()
        let relation = try SQLRelation(id: "logical", namespaceID: "public", name: "items", entityName: "Item",
            columns: [SQLColumn(id: "logical-value", name: "text", fieldNumber: 2)], primaryKeyColumnIDs: ["logical-value"])
        let snapshot = try SQLCatalogSnapshot(catalog: SQLCatalog(namespaces: fixture.snapshot.catalog.namespaces,
            relations: [relation]), physicalSchema: fixture.schema, runtimeConfiguration: fixture.runtime)
        let targetEntity = try Schema.Entity(name: "Item", identifierType: .string,
            fields: fixture.schema.entities[0].fields + [FieldSchema(name: "extra", fieldNumber: 3, type: .string)],
            directoryComponents: [.staticPath("s11-items")])
        let target = try Schema(entities: [targetEntity], version: .init(1, 1, 0))
        let reconciled = try snapshot.reconciledForApplication(physicalSchema: target, runtimeConfiguration: runtime(target))
        #expect(reconciled.catalog.relations[0].id == "logical")
        #expect(reconciled.catalog.relations[0].columns.map(\.name) == ["text", "extra"])
        let removedEntity = try Schema.Entity(name: "Item", identifierType: .string,
            fields: [fixture.schema.entities[0].fields[0]], directoryComponents: [.staticPath("s11-items")])
        let removed = try Schema(entities: [removedEntity], version: .init(2, 0, 0))
        #expect(throws: SQLCatalogBindingError.primaryKeyColumnRemoved(relation: "logical", column: "logical-value")) {
            _ = try snapshot.reconciledForApplication(physicalSchema: removed, runtimeConfiguration: runtime(removed))
        }
    }

    @Test("Store reads and writes participate in the caller transaction and rollback")
    func transactionOwnedStore() async throws {
        enum Rollback: Error { case requested }
        let fixture = try fixture()
        let engine = InMemoryEngine()
        let store = SQLCatalogStore(root: Subspace().subspace("s11-test-metadata"))
        do {
            try await engine.withTransaction { transaction in
                let existing = try await store.load(transaction: transaction)
                #expect(existing == nil)
            }
            await #expect(throws: Rollback.requested) {
                try await engine.withTransaction { transaction in
                    try store.persist(fixture.snapshot, transaction: transaction)
                    let value = try await store.load(transaction: transaction)
                    let loaded = try #require(value)
                    #expect(loaded.digest == fixture.snapshot.digest)
                    throw Rollback.requested
                }
            }
            try await engine.withTransaction { transaction in
                let existing = try await store.load(transaction: transaction)
                #expect(existing == nil)
                try store.persist(fixture.snapshot, transaction: transaction)
            }
            try await engine.withTransaction { transaction in
                let value = try await store.load(transaction: transaction)
                let loaded = try #require(value)
                #expect(loaded.catalog == fixture.snapshot.catalog)
                #expect(try loaded.snapshot(physicalSchema: fixture.schema,
                    runtimeConfiguration: fixture.runtime).digest == fixture.snapshot.digest)
            }
        } catch {
            await engine.shutdown()
            throw error
        }
        await engine.shutdown()
    }

    @Test("Application upgrade validates old metadata with only the actual target compiled runtime")
    func compiledApplicationUpgrade() throws {
        let fixture = try compiledFixture()
        let original = fixture.schema.entities[0]
        let field = try #require(original.fields.first(where: { $0.name == "value" }))
        let index = try IndexDescriptor(entityName: original.name,
            declaration: .ordered(name: "s11_added_value_index",
                keys: [.ascending(FieldIdentity(name: field.name, number: field.fieldNumber))]),
            fieldSchemas: original.fields)
        let definition = try EntityRuntimeDefinition(S11CompiledCatalogEntity.self, including: [index])
        let targetSchema = try Schema(entities: [definition.entity], version: .init(1, 1, 0))
        let targetRuntime = try DatabaseRuntimeConfiguration(executionIdentity: .init(identifier: "s11-upgrade", revision: 2),
            entityRuntimes: [definition.registration()])
        let state = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(fixture.snapshot))
        #expect(throws: SQLCatalogBindingError.runtimeContractMismatch(original.name)) {
            _ = try state.snapshot(physicalSchema: fixture.schema, runtimeConfiguration: targetRuntime)
        }
        let upgraded = try state.reconciledForApplication(physicalSchema: fixture.schema,
            targetPhysicalSchema: targetSchema, runtimeConfiguration: targetRuntime)
        #expect(upgraded.catalog == state.catalog)
        #expect(upgraded.physicalSchemaFingerprint != state.physicalSchemaFingerprint)
        #expect(upgraded.protectedContracts[0].entityDigest != state.protectedContracts[0].entityDigest)
        #expect(throws: SQLCatalogBindingError.invalidProtection(state.catalog.relations[0].id)) {
            _ = try state.reconciledForApplication(physicalSchema: fixture.schema,
                targetPhysicalSchema: targetSchema, runtimeConfiguration: runtime(targetSchema))
        }
        let corrupt = SQLCatalogStoredState(catalog: state.catalog,
            physicalSchemaFingerprint: state.physicalSchemaFingerprint,
            protectedContracts: state.protectedContracts,
            digest: ByteString(repeating: 0, count: 32))
        #expect(throws: SQLCatalogBindingError.digestMismatch) {
            _ = try corrupt.reconciledForApplication(physicalSchema: fixture.schema,
                targetPhysicalSchema: targetSchema, runtimeConfiguration: targetRuntime)
        }
    }

    private struct Fixture: Sendable {
        let schema: Schema
        let runtime: DatabaseRuntimeConfiguration
        let snapshot: SQLCatalogSnapshot
    }

    private func fixture() throws -> Fixture {
        let schema = try Schema(entities: [entity("Item")], version: .init(1, 0, 0))
        let runtime = try runtime(schema)
        return try Fixture(schema: schema, runtime: runtime, snapshot: SQLCatalogSnapshot(
            catalog: SQLCatalogSnapshot.derivedCatalog(physicalSchema: schema), physicalSchema: schema,
            runtimeConfiguration: runtime))
    }

    private func compiledFixture() throws -> Fixture {
        let definition = try EntityRuntimeDefinition(S11CompiledCatalogEntity.self)
        let schema = try Schema(entities: [definition.entity], version: .init(1, 0, 0))
        let runtime = try DatabaseRuntimeConfiguration(executionIdentity: .init(identifier: "s11-compiled", revision: 1),
            entityRuntimes: [definition.registration()])
        return try Fixture(schema: schema, runtime: runtime, snapshot: SQLCatalogSnapshot(
            catalog: SQLCatalogSnapshot.derivedCatalog(physicalSchema: schema), physicalSchema: schema,
            runtimeConfiguration: runtime))
    }

    private func entity(_ name: String, valueType: FieldSchemaType = .string) throws -> Schema.Entity {
        try Schema.Entity(name: name, identifierType: .string, fields: [
            FieldSchema(name: "id", fieldNumber: 1, type: .string),
            FieldSchema(name: "value", fieldNumber: 2, type: valueType)],
            directoryComponents: [.staticPath("s11-items")])
    }

    private func runtime(_ schema: Schema) throws -> DatabaseRuntimeConfiguration {
        try DatabaseRuntimeConfiguration(executionIdentity: .init(identifier: "s11-dynamic", revision: 1),
            entityRuntimes: schema.entities.map { EntityRuntimeDefinition(schemaDriven: $0).registration() })
    }

    private func limits(frame: Int) throws -> StorageFrameLimits {
        try StorageFrameLimits(maximumFrameBytes: frame, maximumStringBytes: frame,
            maximumByteStringBytes: frame, maximumCollectionCount: 100, maximumNestingDepth: 0)
    }
}
