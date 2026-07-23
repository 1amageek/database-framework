#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue

@Persistable
struct IndexProjectionRecord {
    #Directory<IndexProjectionRecord>("test", "canonical-index-projection")

    var id: String = ""
    var email: String
    var name: String
    var age: Int
    var nickname: String? = nil
    var tags: [String] = []
    var target: DatabaseReference<RelationshipTarget>? = nil
}
#endif
#endif
