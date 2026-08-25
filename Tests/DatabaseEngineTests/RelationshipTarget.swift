#if !os(WASI)
import DatabaseKit
import DatabaseTypes

@Persistable
struct RelationshipTarget {
    #Directory<RelationshipTarget>("test", "relationship_v1", "targets")

    var id: String = ""
    var name: String
}
#endif
