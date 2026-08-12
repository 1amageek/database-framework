import DatabaseKit
import DatabaseTypes
import DatabaseRuntime

@Persistable
struct BootstrapIndexedEntity {
    #Directory<BootstrapIndexedEntity>("test", "schema-bootstrap")
    #Index(
        .scalar,
        fields: [\BootstrapIndexedEntity.value],
        name: "bootstrap_value"
    )

    var id: String = ""
    var value: String = ""
}
