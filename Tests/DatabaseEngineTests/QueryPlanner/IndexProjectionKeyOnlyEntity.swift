#if !os(WASI)
import DatabaseKit
import DatabaseTypes

@Persistable
struct IndexProjectionKeyOnlyEntity {
    #Directory<IndexProjectionKeyOnlyEntity>("test", "canonical-key-only-index")

    var id: String = ""
    var email: String
}
#endif
