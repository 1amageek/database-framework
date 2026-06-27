/// Write operation emitted by the WASM framework runtime.
public enum DatabaseFrameworkWasmWriteOperation: Sendable, Hashable {
    case set(key: [UInt8], value: [UInt8])
    case clear(key: [UInt8])
}
