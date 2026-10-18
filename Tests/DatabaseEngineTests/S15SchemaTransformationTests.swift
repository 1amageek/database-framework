#if !os(WASI)
@_spi(DatabaseExecution) @testable import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import Testing

@Persistable
private struct S15ProtectedSchemaItem {
    #Directory<S15ProtectedSchemaItem>("s15-protected-items")
    var id: String = ""
    var value: String = ""
}

@Suite("S15 schema transformation admission", .timeLimit(.minutes(1)))
struct S15SchemaTransformationTests {
    @Test("Persisted compiled protection cannot become a dynamic physical contract")
    func staticProtectionRejectsPhysicalChange() throws {
        let definition = try EntityRuntimeDefinition(S15ProtectedSchemaItem.self)
        let sourceSchema = try Schema(entities: [definition.entity])
        let sourceRuntime = try DatabaseRuntimeConfiguration(
            executionIdentity: .init(identifier: "s15-protected", revision: 1),
            entityRuntimes: [definition.registration()])
        let source = try SQLCatalogSnapshot(catalog: SQLCatalogSnapshot.derivedCatalog(physicalSchema: sourceSchema),
            physicalSchema: sourceSchema, runtimeConfiguration: sourceRuntime)
        #expect(throws: SQLCatalogBindingError.invalidSQLOwnership(definition.entity.name)) {
            try SQLCatalogSnapshot(catalog: source.catalog, physicalSchema: sourceSchema,
                runtimeConfiguration: sourceRuntime, sqlOwnedEntityNames: [definition.entity.name])
        }
        let targetEntity = try Schema.Entity(name: definition.entity.name, identifierType: .string,
            fields: definition.entity.fields.filter { $0.name == "id" },
            directoryComponents: definition.entity.directoryComponents,
            directoryLayer: definition.entity.directoryLayer)
        let targetSchema = try Schema(entities: [targetEntity])
        let targetRuntime = try sourceRuntime.replacingPhysicalSchema(targetSchema)
        #expect(throws: SQLCatalogBindingError.self) {
            try source.replacingPhysicalCatalog(SQLCatalogSnapshot.derivedCatalog(physicalSchema: targetSchema),
                physicalSchema: targetSchema, runtimeConfiguration: targetRuntime)
        }
    }

    @Test("Dropped field numbers cannot become reusable after a catalog replacement")
    func highWaterCannotDecrease() throws {
        let entity = try Schema.Entity(name: "S15HighWater", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string)],
            directoryComponents: [.staticPath("s15-high-water")])
        let schema = try Schema(entities: [entity])
        let runtime = try DatabaseRuntimeConfiguration(
            executionIdentity: .init(identifier: "s15-high-water", revision: 1),
            entityRuntimes: [EntityRuntimeDefinition(schemaDriven: entity).registration()])
        let namespace = try SQLNamespace(id: "public", name: "public")
        let column = try SQLColumn(id: "id", name: "id", fieldNumber: 1)
        let sourceRelation = try SQLRelation(id: "item", namespaceID: namespace.id, name: "items",
            entityName: entity.name, columns: [column], fieldNumberHighWaterMark: 9)
        let snapshot = try SQLCatalogSnapshot(catalog: SQLCatalog(namespaces: [namespace], relations: [sourceRelation]),
            physicalSchema: schema, runtimeConfiguration: runtime)
        let lowered = try SQLRelation(id: "item", namespaceID: namespace.id, name: "items",
            entityName: entity.name, columns: [column], fieldNumberHighWaterMark: 1)
        #expect(throws: DatabaseSchemaPublicationError.self) {
            try snapshot.replacingCatalog(SQLCatalog(namespaces: [namespace], relations: [lowered]),
                physicalSchema: schema, runtimeConfiguration: runtime)
        }
        let restored = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(snapshot))
        #expect(restored.catalog.relations.first?.fieldNumberHighWaterMark == 9)
        let owned = try SQLCatalogSnapshot(catalog: snapshot.catalog, physicalSchema: schema,
            runtimeConfiguration: runtime, sqlOwnedEntityNames: [entity.name])
        let ownershipState = try SQLCatalogCodec.decode(SQLCatalogCodec.encode(owned))
        #expect(ownershipState.sqlOwnedEntityNames == [entity.name])
        #expect(ownershipState.digest != restored.digest)
        let emptyApplication = try Schema(entities: [])
        #expect(try ownershipState.applicationSchema(physicalSchema: schema,
            declaredSchema: emptyApplication).entities == [entity])
        #expect(try restored.applicationSchema(physicalSchema: schema,
            declaredSchema: emptyApplication).entities.isEmpty)
    }

    @Test("Generated policy and CHECK renames retain their stable column meaning")
    func logicalColumnMeaningUsesStableIDs() throws {
        let oldColumn = try SQLColumn(id: "value", name: "before", fieldNumber: 2)
        let newColumn = try SQLColumn(id: "value", name: "after", fieldNumber: 2)
        let old = try SQLRelation(id: "items", namespaceID: "public", name: "old_items", entityName: "Item", columns: [oldColumn])
        let new = try SQLRelation(id: "items", namespaceID: "public", name: "new_items", entityName: "Item", columns: [newColumn])
        let oldPolicy = SQLColumnPolicy.generated(.column(ColumnRef(table: "old_items", column: "before")))
        let newPolicy = SQLColumnPolicy.generated(.column(ColumnRef(table: "new_items", column: "after")))
        #expect(try SQLCatalogSemanticContract.policy(oldPolicy, relation: old)
            == SQLCatalogSemanticContract.policy(newPolicy, relation: new))
    }
}
#endif
