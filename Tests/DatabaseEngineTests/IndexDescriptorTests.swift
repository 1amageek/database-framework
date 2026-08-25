#if !os(WASI)
import DatabaseKit
import DatabaseTypes
import TestHeartbeat
import Testing

@Suite("Index descriptor semantics", .heartbeat)
struct IndexDescriptorTests {
    private let fields = [
        FieldSchema(name: "email", fieldNumber: 1, type: .string),
        FieldSchema(name: "status", fieldNumber: 2, type: .string),
        FieldSchema(name: "amount", fieldNumber: 3, type: .int64),
    ]

    @Test("Ordered descriptors retain keys, covering fields, and uniqueness")
    func orderedDescriptor() throws {
        let descriptor = try IndexDescriptor(
            entityName: "Account",
            declaration: .ordered(
                name: "account_email",
                keys: [
                    .ascending(FieldIdentity(name: "email", number: 1))
                ],
                includedFields: [
                    FieldIdentity(name: "status", number: 2)
                ],
                unique: true
            ),
            fieldSchemas: fields
        )

        #expect(descriptor.name == "account_email")
        #expect(descriptor.type == .ordered)
        #expect(descriptor.fieldNames == ["email"])
        #expect(descriptor.includedFieldNames == ["status"])
        #expect(descriptor.isUnique)
        #expect(descriptor.isCovering)
    }

    @Test("Aggregate descriptors retain their complete semantic definition")
    func aggregateDescriptor() throws {
        let definition = IndexDefinition<FieldIdentity>.aggregate(
            function: .sum,
            groupBy: [
                .ascending(FieldIdentity(name: "status", number: 2))
            ],
            value: FieldIdentity(name: "amount", number: 3)
        )
        let descriptor = try IndexDescriptor(
            entityName: "Account",
            declaration: IndexDeclaration(
                name: "account_amount_by_status",
                definition: definition
            ),
            fieldSchemas: fields
        )

        #expect(descriptor.type == .aggregate(.sum))
        #expect(descriptor.declaration.definition == definition)
        #expect(descriptor.fieldNames == ["status", "amount"])
    }

    @Test("Unknown field identities fail explicitly")
    func unknownFieldFails() {
        #expect(throws: IndexDeclarationError.self) {
            _ = try IndexDescriptor(
                entityName: "Account",
                declaration: .ordered(
                    name: "account_missing",
                    keys: [
                        .ascending(
                            FieldIdentity(name: "missing", number: 99)
                        )
                    ]
                ),
                fieldSchemas: fields
            )
        }
    }

    @Test("Description exposes logical identity and semantic type")
    func description() throws {
        let descriptor = try IndexDescriptor(
            entityName: "Account",
            declaration: .bitmap(
                name: "account_status",
                field: FieldIdentity(name: "status", number: 2)
            ),
            fieldSchemas: fields
        )

        #expect(descriptor.description.contains("Account"))
        #expect(descriptor.description.contains("account_status"))
        #expect(descriptor.description.contains("bitmap"))
    }
}
#endif
