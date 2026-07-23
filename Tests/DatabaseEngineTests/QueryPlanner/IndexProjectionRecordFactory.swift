#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseEngine

enum IndexProjectionRecordFactory {
    static let storedFields = ["name", "age", "nickname", "tags", "target"]

    static func descriptor(
        storedFields: [String] = storedFields
    ) -> IndexDescriptor {
        IndexDescriptor(
            name: "IndexProjectionRecord_email",
            keyPaths: [\IndexProjectionRecord.email],
            kind: ScalarIndexKind<IndexProjectionRecord>(fields: [\.email]),
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

    static func record() throws -> IndexProjectionRecord {
        var record = IndexProjectionRecord(
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
        record.id = "owner-1"
        return record
    }
}
#endif
#endif
