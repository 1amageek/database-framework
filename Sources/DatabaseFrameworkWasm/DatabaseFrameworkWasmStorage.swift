/// Storage boundary required by the WASM database runtime.
public protocol DatabaseFrameworkWasmStorage: Sendable {
    func read(key: [UInt8]) throws(DatabaseFrameworkWasmError) -> [UInt8]?

    func scan(
        begin: [UInt8],
        end: [UInt8],
        limit: Int,
        reverse: Bool
    ) throws(DatabaseFrameworkWasmError) -> [DatabaseFrameworkWasmKeyValue]

    func commit(_ writes: [DatabaseFrameworkWasmWriteOperation]) throws(DatabaseFrameworkWasmError)
}
