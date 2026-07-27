#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseEngine

enum IndexProjectionEntityFactory {
    static let storedFields = ["name", "age", "nickname", "tags", "target"]

    static func descriptor(
        includesStoredFields: Bool = true
    ) throws -> IndexDescriptor {
        try IndexDescriptor(
            name: "IndexProjectionEntity_email",
            definition: .scalar,
            fields: [IndexProjectionEntity.fields.email.ascending],
            storedFields: includesStoredFields
                ? [
                    IndexProjectionEntity.fields.name.ascending,
                    IndexProjectionEntity.fields.age.ascending,
                    IndexProjectionEntity.fields.nickname.ascending,
                    IndexProjectionEntity.fields.tags.ascending,
                    IndexProjectionEntity.fields.target.ascending,
                ]
                : []
        )
    }

    static func runtimeIndex(
        from descriptor: IndexDescriptor,
        storedFieldNames: [String]? = nil
    ) -> Index {
        Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: KeyExpressionFactory.from(keyPaths: descriptor.fieldNames),
            isUnique: descriptor.isUnique,
            storedFieldNames: storedFieldNames
                ?? descriptor.storedFieldNames
        )
    }

    static func entity() throws -> IndexProjectionEntity {
        var entity = IndexProjectionEntity(
            email: "owner@example.com",
            name: "Owner",
            age: 42,
            nickname: nil,
            tags: ["calendar", "graph"],
            target: try RelationshipReferenceFactory.make(
                RelationshipTarget.self,
                id: "target-1"
            )
        )
        entity.id = "owner-1"
        return entity
    }
}
#endif
#endif
