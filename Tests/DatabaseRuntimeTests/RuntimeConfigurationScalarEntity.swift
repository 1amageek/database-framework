import DatabaseKit
import DatabaseTypes

@Persistable
struct RuntimeConfigurationScalarEntity {
    #Index(
        .ordered(
            name: "RuntimeConfigurationScalarEntity_name", keys: [.ascending(\RuntimeConfigurationScalarEntity.name)],
            unique: false))

    var id: String = ""
    var name: String
}
