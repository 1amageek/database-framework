/// Ordered caller-owned outcomes from one authoritatively closed SQL block.
public struct SQLBlockResult: Sendable {
    public let results: [SQLStatementResult]

    init(results: consuming [SQLStatementResult]) {
        self.results = consume results
    }
}
