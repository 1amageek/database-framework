#if !os(WASI)
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipOptionalOwner {
    #Directory<RelationshipOptionalOwner>("test", "relationship_v1", "optional_owners")

    var id: String = ""
    var name: String

    @Relationship(deleteRule: .nullify)
    var target: PersistableReference<RelationshipTarget>? = nil
}
#endif
