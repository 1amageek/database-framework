#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue
import Relationship

@Persistable
struct RelationshipOptionalOwner {
    #Directory<RelationshipOptionalOwner>("test", "relationship_v1", "optional_owners")

    var id: String = ""
    var name: String

    @Relationship(deleteRule: .nullify)
    var target: DatabaseReference<RelationshipTarget>? = nil
}
#endif
#endif
