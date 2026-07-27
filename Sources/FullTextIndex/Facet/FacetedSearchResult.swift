import DatabaseKit

public struct FacetedSearchResult<Item: Persistable>: Sendable {
    public let items: [Item]
    public let facets: [String: [(value: String, count: Int64)]]
    public let totalCount: Int

    public init(
        items: [Item],
        facets: [String: [(value: String, count: Int64)]],
        totalCount: Int
    ) {
        self.items = items
        self.facets = facets
        self.totalCount = totalCount
    }
}
