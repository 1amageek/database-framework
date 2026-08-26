import DatabaseTypes

/// Stable identity of the storage snapshot selected for one database read.
public enum DatabaseReadPosition: Sendable, Hashable {
    case version(UInt64)
    case opaque(ByteString)
}
