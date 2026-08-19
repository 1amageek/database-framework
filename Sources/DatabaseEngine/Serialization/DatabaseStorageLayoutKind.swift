/// Physical root contract persisted with a database format descriptor.
public enum DatabaseStorageLayoutKind: UInt8, Sendable, Hashable {
    /// One ordinary database occupies the host-selected root.
    case singleDatabase = 1

    /// One control root coordinates explicit Base storage topology.
    case multiBase = 2
}
