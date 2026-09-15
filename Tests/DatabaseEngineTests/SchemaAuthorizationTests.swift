#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import Testing
@testable import DatabaseEngine

@Suite("Schema-driven entity authorization")
struct SchemaAuthorizationTests {
    @Test func decisionsPreserveIdentityAndDefaultDeny() throws {
        let entity = try Schema.Entity(
            name: "Asset", identifierType: .string,
            fields: [FieldSchema(name: "id", fieldNumber: 1, type: .string),
                     FieldSchema(name: "tenant", fieldNumber: 2, type: .string)],
            directoryComponents: [.staticPath("assets"), .dynamicField(fieldName: "tenant")]
        )
        let handler = AuthorizationPolicyHandler(schemaDriven: entity) { operation, context in
            context.principal?.roles.contains("editor") == true && operation != .delete
        }
        let editor = AuthorizationContext.authenticated(Principal(identifier: "user", roles: ["editor"]))
        let row = try PersistedModel(entity: "Asset", fields: [
            PersistableField(number: 1, name: "id", value: .string("one")),
            PersistableField(number: 2, name: "tenant", value: .string("factory-a"))
        ])
        #expect(handler.permitsQuery(SecurityQuery(), context: editor))
        #expect(!handler.permitsQuery(SecurityQuery(), context: .anonymous))
        #expect(try handler.permitsRead(row, context: editor).isPermitted)
        let created = try handler.permitsCreate(row, context: editor)
        #expect(created.isPermitted)
        #expect(created.resource == (try EntityReference(entity: "Asset", id: .string("one"),
                                                    partitions: FieldObject([("tenant", .string("factory-a"))]))))
        #expect(try handler.permitsUpdate(from: row, to: row, context: editor).isPermitted)
        #expect(try !handler.permitsDelete(row, context: editor).isPermitted)
        #expect(try !handler.permitsCreate(row, context: .anonymous).isPermitted)
        let foreign = try PersistedModel(entity: "Other", fields: row.fields)
        #expect(throws: AuthorizationPolicyHandlerError.self) {
            try handler.permitsRead(foreign, context: editor)
        }
        let missingPartition = try PersistedModel(entity: "Asset", fields: [row.fields[0]])
        #expect(throws: (any Error).self) {
            try handler.permitsCreate(missingPartition, context: editor)
        }
        let invalidIdentifier = try PersistedModel(entity: "Asset", fields: [
            PersistableField(number: 1, name: "id", value: .int64(1)), row.fields[1]
        ])
        #expect(throws: (any Error).self) {
            try handler.permitsUpdate(from: invalidIdentifier, to: row, context: editor)
        }
        #expect(try !handler.permitsRead(row, context: .authenticated(
            Principal(identifier: "other", roles: ["reader"])
        )).isPermitted)
        let delegate = RequestSecurityPolicyDelegate(
            policies: try AuthorizationPolicyRegistry(handlers: []),
            schema: try Schema(entities: [entity], version: Schema.Version(1, 0, 0))
        )
        #expect(throws: SecurityError.self) { try delegate.evaluateCreate(row) }
    }
}
#endif
