#if os(macOS)
import DatabaseClient
import DatabaseKit
import DatabaseTypes
import DatabaseWire

/// Bounded Wire fixture; Client still owns correlation and checked continuation.
struct PerformanceTransport: DatabaseTransport {
    let rows: [QueryRow]
    let columns: [QueryColumn]
    let input: QueryInput
    let budget: ExecutionBudget
    init(count: Int, input: QueryInput, budget: ExecutionBudget) throws {
        self.input = input; self.budget = budget
        columns = [try QueryColumn(number: 1, name: "n", type: .sql(.integer), isNullable: false)]
        rows = (1...count).map { QueryRow(values: [.primitive(.int32(Int32($0)))]) }
    }
    func send(_ bytes: ByteString) async throws(DatabaseTransportError) -> ByteString {
        do {
            let decoded = try DatabaseWireDecoder().decodeRequest(DatabaseOperationCatalog.queryExecute, from: bytes)
            guard decoded.request.input == input, decoded.request.parameters.isEmpty,
                decoded.request.graphPartitions.isEmpty, decoded.request.budget == budget,
                decoded.request.page?.limit == 256, decoded.metadata.traceID == nil,
                decoded.metadata.idempotencyKey == nil else {
                throw DatabaseTransportError.invalidResponse("Changed benchmark invocation")
            }
            let start: Int
            if let token = decoded.request.page?.continuation {
                guard token.count == 4 else { throw DatabaseTransportError.invalidResponse("Invalid fixture continuation") }
                start = token.reduce(0) { ($0 << 8) | Int($1) }
            } else { start = 0 }
            let end = min(rows.count, start + Int(decoded.request.page?.limit ?? UInt32(rows.count)))
            guard start >= 0, start < rows.count, end > start else { throw DatabaseTransportError.invalidResponse("Invalid fixture range") }
            let token: ByteString? = end == rows.count ? nil : ByteString([
                UInt8((end >> 24) & 255), UInt8((end >> 16) & 255), UInt8((end >> 8) & 255), UInt8(end & 255)])
            // The fixture constructs the server page at this ownership boundary.
            let page = try QueryRowPage(columns: columns, rows: Array(rows[start..<end]), continuation: token, snapshotVersion: 1)
            return try DatabaseWireEncoder().encodeResponse(DatabaseOperationCatalog.queryExecute,
                requestID: decoded.requestID, response: .rows(page))
        } catch let error as DatabaseTransportError { throw error }
        catch { throw .invalidResponse("Performance fixture rejected a request") }
    }
}
#endif
