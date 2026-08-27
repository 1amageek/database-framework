/// A derived scalar exceeded the safe maximum admitted before construction.
package enum DatabaseQueryScopedFieldValueError: Error, Sendable, Equatable {
    case payloadFootprintExceeded(
        maximumRows: UInt64,
        maximumBytes: UInt64,
        actualRows: UInt64,
        actualBytes: UInt64
    )
}
