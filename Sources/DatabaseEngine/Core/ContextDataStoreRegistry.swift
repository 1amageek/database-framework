/// Named state stored behind the context's synchronization boundary.
struct ContextDataStoreRegistry: Sendable {
    var stores: [ContextDataStoreIdentity: FDBDataStore] = [:]
}
