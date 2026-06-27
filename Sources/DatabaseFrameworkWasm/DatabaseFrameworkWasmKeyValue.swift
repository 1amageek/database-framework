/// Key-value row used by the WASM framework runtime.
public struct DatabaseFrameworkWasmKeyValue: Sendable, Hashable {
    public let key: [UInt8]
    public let value: [UInt8]

    public init(key: [UInt8], value: [UInt8]) {
        self.key = key
        self.value = value
    }
}
