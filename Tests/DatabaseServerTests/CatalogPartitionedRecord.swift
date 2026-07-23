import Core
import DatabaseValue
import DatabaseRuntime

@Persistable
struct CatalogPartitionedRecord {
    #Directory<CatalogPartitionedRecord>(
        "test",
        "partition-catalog",
        Field(\CatalogPartitionedRecord.tenantID)
    )
    #Index(
        ScalarIndexKind<CatalogPartitionedRecord>(fields: [\.value]),
        name: "catalog_value"
    )

    var id: String = ""
    var tenantID: String = ""
    var value: String = ""
}
