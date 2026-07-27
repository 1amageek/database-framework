#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes
import DatabaseKit

@Persistable
struct RelationshipArrayOwner {
    #Directory<RelationshipArrayOwner>("test", "relationship_v1", "array_owners")

    var id: String = ""

    @Relationship(deleteRule: .nullify)
    var targets: [PersistableReference<RelationshipTarget>] = []
}
#endif
#endif
