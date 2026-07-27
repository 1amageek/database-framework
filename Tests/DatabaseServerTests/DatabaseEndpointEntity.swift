import DatabaseKit
import DatabaseTypes

@Persistable
struct DatabaseEndpointEntity {
    #Directory<DatabaseEndpointEntity>("test", "database-server")

    var id: String = ""
    var title: String = ""
    var priority: Int64 = 0
}
