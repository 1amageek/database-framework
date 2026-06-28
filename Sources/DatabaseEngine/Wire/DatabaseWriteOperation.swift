/// Write operation emitted by the wire runtime.
public enum DatabaseWriteOperation: Sendable, Hashable {
    case set(key: [UInt8], value: [UInt8])
    case clear(key: [UInt8])
}
