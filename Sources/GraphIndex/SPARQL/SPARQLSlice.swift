public struct SPARQLSlice: Sendable, Hashable {
    public let offset: Int
    public let limit: Int?

    package init(offset: Int = 0, limit: Int? = nil) {
        self.offset = offset
        self.limit = limit
    }
}
