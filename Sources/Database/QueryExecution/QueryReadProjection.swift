import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseEngine
@_spi(DatabaseExecution) import DatabaseWire

/// Projects admitted reads without materializing a second row or quad array.
package struct QueryReadProjection: Sendable {
    let requestID: UInt64
    let limits: DatabaseWireLimits

    func measure(_ output: borrowing QueryReadOutput) throws -> UInt64 {
        // The synchronous projection retains only the immutable owner header.
        // Rows, quads and their reservations remain shared without materialization.
        let retained = copy output
        let count = try DatabaseWireEncoder(limits: limits).measureQueryResponse(
            requestID: requestID, resultCount: 1) { writer in try write(retained, into: &writer) }
        guard let count = UInt64(exactly: count) else { throw DatabaseWireError.byteCountOverflow }
        return count
    }

    func encode(_ output: borrowing QueryReadOutput,
                into destination: inout MutableSpan<UInt8>) throws {
        let retained = copy output
        try destination.withUnsafeMutableBytes { bytes in
            try DatabaseWireEncoder(limits: limits).encodeQueryResponse(requestID: requestID,
                resultCount: 1, into: bytes) { writer in try write(retained, into: &writer) }
        }
    }

    private func write(_ output: borrowing QueryReadOutput,
                       into writer: inout QueryResponseWriter) throws {
        switch copy output {
        case .rows(let rows):
            let view = SQLBlockRowsView(rows: rows)
            try QueryResponseEncoder(requestID: requestID, limits: limits).writeRows(view, into: &writer)
        case .boolean(let value, let position, let domain):
            #if DATABASE_MULTI_BASE
            try writer.writeBoolean(QueryBooleanResult(value: value, provenance: nil,
                consistency: consistency(position, domain: domain)))
            #else
            _ = position; _ = domain
            try writer.writeBoolean(value)
            #endif
        case .graph(let graph, let position, let domain):
            #if DATABASE_MULTI_BASE
            try writer.writeRDFGraph(quadCount: graph.count,
                consistency: consistency(position, domain: domain)) { quads in
                for index in 0..<graph.count {
                    try graph.withElement(at: index) { try quads.write($0) }
                }
            }
            #else
            _ = domain
            let version: Int64?
            if case .version(let value) = position { version = Int64(exactly: value) }
            else { version = nil }
            try writer.writeRDFGraph(quadCount: graph.count, snapshotVersion: version) { quads in
                for index in 0..<graph.count {
                    try graph.withElement(at: index) { try quads.write($0) }
                }
            }
            #endif
        }
    }

    #if DATABASE_MULTI_BASE
    private func consistency(_ position: DatabaseReadPosition, domain: String) throws -> DatabaseReadConsistency {
        let selected: DomainReadPoint.Position = switch position {
        case .version(let value): .version(value)
        case .opaque(let identifier): .opaque(identifier)
        }
        return .transactional(try DomainReadPoint(domainID: domain, position: selected))
    }
    #endif
}
