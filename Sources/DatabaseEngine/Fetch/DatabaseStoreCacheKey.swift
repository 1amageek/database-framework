struct DatabaseStoreCacheKey: Sendable, Hashable {
    let entity: String
    let components: [String]
}
