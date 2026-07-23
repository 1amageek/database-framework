import Core
import DatabaseValue
import DatabaseRuntime

@Persistable
struct CatalogPartitionedEntity {
    #Directory<CatalogPartitionedEntity>(
        "test",
        "partition-catalog",
        Field(\CatalogPartitionedEntity.tenantID)
    )
    #Index(
        ScalarIndexKind<CatalogPartitionedEntity>(fields: [\.value]),
        name: "catalog_value"
    )

    var id: String = ""
    var tenantID: String = ""
    var value: String = ""
}
