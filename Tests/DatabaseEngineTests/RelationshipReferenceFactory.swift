#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue

enum RelationshipReferenceFactory {
    static func make<Target: Persistable>(
        _ type: Target.Type,
        id: String,
        partitions: [DatabaseObjectField] = []
    ) throws -> DatabaseReference<Target> {
        try DatabaseReference(
            identity: PersistableIdentity(
                entity: Target.persistableType,
                id: .string(id),
                partitions: partitions
            )
        )
    }
}
#endif
#endif
