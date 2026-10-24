#if SQLITE
import DatabaseKit

@Persistable
struct FluentExecutionPerson {
    var id: String = "person"
    var name: String = "Name"
    var age: Int32 = 20
    var isActive: Bool = true
    var nickname: String? = nil
}
#endif
