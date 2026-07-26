import DatabaseKit
import DatabaseTypes

@Persistable
struct RuntimeConfigurationScalarEntity {
    #Index(
        .scalar,
        fields: [\RuntimeConfigurationScalarEntity.name],
        name: "RuntimeConfigurationScalarEntity_name"
    )

    var id: String = ""
    var name: String
}
