import Core
import DatabaseValue
import DatabaseRuntime

@Persistable
struct BootstrapIndexedRecord {
    #Directory<BootstrapIndexedRecord>("test", "schema-bootstrap")
    #Index(
        ScalarIndexKind<BootstrapIndexedRecord>(fields: [\.value]),
        name: "bootstrap_value"
    )

    var id: String = ""
    var value: String = ""
}
