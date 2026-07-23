#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue

@Persistable
struct RelationshipTarget {
    #Directory<RelationshipTarget>("test", "relationship_v1", "targets")

    var id: String = ""
    var name: String
}
#endif
#endif
