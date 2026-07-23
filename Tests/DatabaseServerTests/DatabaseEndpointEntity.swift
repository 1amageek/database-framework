import Core
import DatabaseValue

@Persistable
struct DatabaseEndpointEntity {
    #Directory<DatabaseEndpointEntity>("test", "database-server")

    var id: String = ""
    var title: String = ""
    var priority: Int = 0
}
