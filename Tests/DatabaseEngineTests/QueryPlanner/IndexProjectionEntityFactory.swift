#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseEngine

enum IndexProjectionEntityFactory {
    static let storedFields = ["name", "age", "nickname", "tags", "target"]

    static func descriptor(
        storedFields: [String] = storedFields
    ) -> IndexDescriptor {
        IndexDescriptor(
            name: "IndexProjectionEntity_email",
            keyPaths: [\IndexProjectionEntity.email],
            kind: ScalarIndexKind<IndexProjectionEntity>(fields: [\.email]),
            storedFieldNames: storedFields
        )
    }

    static func runtimeIndex(
        from descriptor: IndexDescriptor
    ) -> Index {
        Index(
            name: descriptor.name,
            kind: descriptor.kind,
            rootExpression: KeyExpressionFactory.from(keyPaths: descriptor.fieldNames),
            isUnique: descriptor.isUnique,
            storedFieldNames: descriptor.storedFieldNames
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
