#if !os(WASI)
#if FOUNDATION_DB
import DatabaseKit
import DatabaseTypes

@Persistable
struct IndexProjectionKeyOnlyEntity {
    #Directory<IndexProjectionKeyOnlyEntity>("test", "canonical-key-only-index")

    var id: String = ""
    var email: String
}
#endif
#endif
