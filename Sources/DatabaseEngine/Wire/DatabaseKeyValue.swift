/// Key-value row used by the wire runtime.
public struct DatabaseKeyValue: Sendable, Hashable {
    public let key: [UInt8]
    public let value: [UInt8]

    public init(key: [UInt8], value: [UInt8]) {
        self.key = key
        self.value = value
    }
}
