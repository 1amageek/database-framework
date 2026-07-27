#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import Testing

@Suite("Typed relationship schema", .heartbeat)
struct RelationshipReferenceMacroTests {
    @Test("Relationship fields compile to reference schema without scalar indexes")
    func referenceSchema() throws {
        let descriptor = try #require(
            RelationshipOptionalOwner.relationshipDescriptors.first
        )
        let field = try #require(
            RelationshipOptionalOwner.fieldSchemas.first {
                $0.name == "target"
            }
        )

        #expect(RelationshipOptionalOwner.relationshipDescriptors.count == 1)
        #expect(try RelationshipOptionalOwner.indexDescriptors.isEmpty)
        #expect(descriptor.ownerTypeName == RelationshipOptionalOwner.persistableType)
        #expect(descriptor.propertyName == "target")
        #expect(descriptor.propertyFieldNumber == UInt32(field.fieldNumber))
        #expect(descriptor.relatedTypeName == RelationshipTarget.persistableType)
        #expect(descriptor.cardinality == .optionalToOne)
        #expect(descriptor.deleteRule == .nullify)
        #expect(field.type == .reference)
        #expect(field.referenceTargetEntity == RelationshipTarget.persistableType)
        #expect(field.isOptional)
        #expect(!field.isArray)
    }

    @Test("To-many relationship preserves reference cardinality")
    func toManySchema() throws {
        let descriptor = try #require(
            RelationshipArrayOwner.relationshipDescriptors.first
        )
        let field = try #require(
            RelationshipArrayOwner.fieldSchemas.first {
                $0.name == "targets"
            }
        )

        #expect(descriptor.cardinality == .toMany)
        #expect(descriptor.deleteRule == .nullify)
        #expect(field.type == .reference)
        #expect(field.referenceTargetEntity == RelationshipTarget.persistableType)
        #expect(!field.isOptional)
        #expect(field.isArray)
    }
}
#endif
#endif
