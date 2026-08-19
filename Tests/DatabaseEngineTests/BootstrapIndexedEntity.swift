import DatabaseKit
import DatabaseRuntime
import DatabaseTypes

@Persistable
struct BootstrapIndexedEntity {
    #Directory<BootstrapIndexedEntity>("test", "schema-bootstrap")
    #Index(.ordered(name: "bootstrap_value", keys: [.ascending(\BootstrapIndexedEntity.value)], unique: false))

    var id: String = ""
    var value: String = ""
}
