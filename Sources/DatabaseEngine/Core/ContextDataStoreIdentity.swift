/// Identifies a resolved persistent store within an `FDBContext`.
struct ContextDataStoreIdentity: Hashable, Sendable {
    let typeName: String
    let resolvedPath: [String]
}
