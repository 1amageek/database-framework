import DatabaseKit
import DatabaseTypes
import StorageKit

/// Persisted allocator cells and atomic request-event recovery records.
struct SQLSequenceStore: Sendable {
    let root: Subspace

    init(root: Subspace) { self.root = root.subspace("sql_sequences") }

    func headKey(_ id: String) -> ByteString { root.subspace("heads").pack(Tuple(id)) }
    func cellKey(_ id: String, head: ByteString) -> ByteString {
        root.subspace("cells").pack(Tuple(id, head))
    }
    func markerKey(request: ByteString, event: Int) -> ByteString {
        root.subspace("recovery").subspace(request).pack(Tuple(Int64(event)))
    }
    func recoveryRange(request: ByteString) -> (begin: ByteString, end: ByteString) {
        root.subspace("recovery").subspace(request).range()
    }

    static func encode(_ value: Int64, called: Bool) -> ByteString {
        var bytes: [UInt8] = [called ? 1 : 0]
        let bits = UInt64(bitPattern: value)
        for shift in stride(from: 56, through: 0, by: -8) {
            bytes.append(UInt8(truncatingIfNeeded: bits >> shift))
        }
        return ByteString(bytes)
    }

    static func decode(_ bytes: ByteString) throws -> (value: Int64, called: Bool) {
        guard bytes.count == 9, bytes[0] <= 1 else {
            throw SQLSequenceExecutionError.invalidPersistedState
        }
        var bits: UInt64 = 0
        for index in 1..<9 { bits = (bits << 8) | UInt64(bytes[index]) }
        return (Int64(bitPattern: bits), bytes[0] == 1)
    }

    static func next(_ declaration: SQLSequence, state: (value: Int64, called: Bool)) throws -> Int64 {
        if !state.called {
            guard state.value >= declaration.minimum, state.value <= declaration.maximum else {
                throw SQLSequenceExecutionError.exhausted(declaration.id)
            }
            return state.value
        }
        let (candidate, overflow) = state.value.addingReportingOverflow(declaration.increment)
        if !overflow, candidate >= declaration.minimum, candidate <= declaration.maximum { return candidate }
        guard declaration.cycle else { throw SQLSequenceExecutionError.exhausted(declaration.id) }
        return declaration.increment > 0 ? declaration.minimum : declaration.maximum
    }
}
