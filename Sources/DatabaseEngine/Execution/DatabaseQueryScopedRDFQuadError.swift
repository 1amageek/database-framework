/// A produced RDF quad exceeded the maximum admitted before construction.
package enum DatabaseQueryScopedRDFQuadError: Error, Sendable, Equatable {
    case payloadFootprintExceeded(
        maximumRows: UInt64,
        maximumBytes: UInt64,
        actualRows: UInt64,
        actualBytes: UInt64
    )
}
