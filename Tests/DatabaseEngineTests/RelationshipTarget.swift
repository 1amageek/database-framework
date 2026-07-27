#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipTarget {
    #Directory<RelationshipTarget>("test", "relationship_v1", "targets")

    var id: String = ""
    var name: String
}
#endif
#endif
