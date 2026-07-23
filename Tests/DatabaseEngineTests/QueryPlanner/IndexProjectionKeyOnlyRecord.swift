#if !os(WASI)
#if FOUNDATION_DB
import Core
import DatabaseValue

@Persistable
struct IndexProjectionKeyOnlyRecord {
    #Directory<IndexProjectionKeyOnlyRecord>("test", "canonical-key-only-index")

    var id: String = ""
    var email: String
}
#endif
#endif
