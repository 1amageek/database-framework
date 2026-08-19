#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseEngine

enum IndexProjectionEntityFactory {
    static let includedFields = ["name", "age", "nickname", "tags", "target"]

    static func descriptor(
        includesStoredFields: Bool = true
    ) throws -> IndexDescriptor {
        try IndexDescriptor(
            entityName: IndexProjectionEntity.persistableType,
            declaration: .ordered(
                name: "IndexProjectionEntity_email",
                keys: [
                    .ascending(IndexProjectionEntity.fields.email.identity)
                ],
                includedFields: includesStoredFields
                ? [
                    IndexProjectionEntity.fields.name.identity,
                        IndexProjectionEntity.fields.age.identity,
                        IndexProjectionEntity.fields.nickname.identity,
                        IndexProjectionEntity.fields.tags.identity,
                        IndexProjectionEntity.fields.target.identity,
                    ]
                    : []
            ),
            fieldSchemas: IndexProjectionEntity.fieldSchemas
        )
    }

    static func runtimeIndex(
        from descriptor: IndexDescriptor
    ) -> ResolvedIndex {
        ResolvedIndex(
            descriptor: descriptor,
            rootExpression: KeyExpressionFactory.from(keyPaths: descriptor.fieldNames
            )
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
