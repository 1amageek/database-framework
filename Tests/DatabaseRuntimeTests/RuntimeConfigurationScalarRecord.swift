import Core
import DatabaseValue
import ScalarIndex

@Persistable
struct RuntimeConfigurationScalarRecord {
    #Index(
        ScalarIndexKind<RuntimeConfigurationScalarRecord>(
            fields: [\.name]
        ),
        name: "RuntimeConfigurationScalarRecord_name"
    )

    var name: String
}
