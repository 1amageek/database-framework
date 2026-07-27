#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes

@Persistable
struct IndexProjectionEntity {
    #Directory<IndexProjectionEntity>("test", "canonical-index-projection")

    var id: String = ""
    var email: String
    var name: String
    var age: Int64
    var nickname: String? = nil
    var tags: [String] = []
    var target: PersistableReference<RelationshipTarget>? = nil
}
#endif
#endif
