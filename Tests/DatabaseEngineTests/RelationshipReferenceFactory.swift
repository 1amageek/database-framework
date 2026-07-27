#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes

enum RelationshipReferenceFactory {
    static func make<Target: Persistable>(
        _ type: Target.Type,
        id: String,
        partitions: FieldObject = FieldObject()
    ) throws -> PersistableReference<Target> {
        try PersistableReference(
            identity: EntityReference(
                entity: Target.persistableType,
                id: .string(id),
                partitions: partitions
            )
        )
    }
}
#endif
#endif
