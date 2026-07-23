import Core
import DatabaseValue
import ScalarIndex

@Persistable
struct RuntimeConfigurationScalarEntity {
    #Index(
        ScalarIndexKind<RuntimeConfigurationScalarEntity>(
            fields: [\.name]
        ),
        name: "RuntimeConfigurationScalarEntity_name"
    )

    var name: String
}
