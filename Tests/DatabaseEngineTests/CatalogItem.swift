import DatabaseKit

@Persistable
struct CatalogItem {
    #Directory<CatalogItem>("catalog", "items")

    var id: String
    var category: String
    var title: String
}
