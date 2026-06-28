/// Storage boundary required by the wire database runtime.
public protocol DatabaseStorage: Sendable {
    func read(key: [UInt8]) throws(DatabaseRuntimeError) -> [UInt8]?

    func scan(
        begin: [UInt8],
        end: [UInt8],
        limit: Int,
        reverse: Bool
    ) throws(DatabaseRuntimeError) -> [DatabaseKeyValue]

    func commit(_ writes: [DatabaseWriteOperation]) throws(DatabaseRuntimeError)
}
