import Core
import DatabaseValue

@Persistable
struct DatabaseEndpointRecord {
    #Directory<DatabaseEndpointRecord>("test", "database-server")

    var id: String = ""
    var title: String = ""
    var priority: Int = 0
}
