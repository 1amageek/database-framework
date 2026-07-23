/// Identifies a resolved persistent store within an `DatabaseContext`.
struct ContextDataStoreIdentity: Hashable, Sendable {
    let typeName: String
    let resolvedPath: [String]
}
