import Core
import DatabaseValue
import DatabaseRuntime

@Persistable
struct BootstrapIndexedEntity {
    #Directory<BootstrapIndexedEntity>("test", "schema-bootstrap")
    #Index(
        ScalarIndexKind<BootstrapIndexedEntity>(fields: [\.value]),
        name: "bootstrap_value"
    )

    var id: String = ""
    var value: String = ""
}
