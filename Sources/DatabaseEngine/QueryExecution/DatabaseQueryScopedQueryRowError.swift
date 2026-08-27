package enum DatabaseQueryScopedQueryRowError:
    Error,
    Sendable,
    Equatable {
    case payloadFootprintMismatch(
        expectedRows: UInt64,
        expectedBytes: UInt64,
        observedRows: UInt64,
        observedBytes: UInt64
    )
}
