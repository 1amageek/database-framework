import DatabaseKit
import DatabaseTypes
import DatabaseRuntime

@Persistable
struct CatalogPartitionedEntity {
    #Directory<CatalogPartitionedEntity>(
        "test",
        "partition-catalog",
        \CatalogPartitionedEntity.tenantID
    )
    #Index(
        .scalar,
        fields: [\CatalogPartitionedEntity.value],
        name: "catalog_value"
    )

    var id: String = ""
    var tenantID: String = ""
    var value: String = ""
}
