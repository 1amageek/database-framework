import QueryAST
import Testing

@Suite("SQL parser benchmarks", .serialized)
struct SQLParserBenchmarks {
    private static let wideSelect = makeWideSelect(projectionCount: 256)

    @Test("Wide SELECT parsing", .timeLimit(.minutes(1)))
    func wideSelectParsing() async throws {
        let measurement = try await measureBenchmark(
            name: "256 projections",
            warmupIterations: 10,
            measurementIterations: 100
        ) {
            _ = try SQLParser().parseSelect(Self.wideSelect)
        }
        printBenchmarkReport(
            title: "SQL parser wide SELECT",
            measurement: measurement
        )
    }

    private static func makeWideSelect(projectionCount: Int) -> String {
        let projections = (0..<projectionCount).map {
            "source.field_\($0) AS alias_\($0)"
        }.joined(separator: ", ")
        return """
        SELECT \(projections)
        FROM metrics AS source
        WHERE source.kind = 'timeseries'
          AND source.value BETWEEN 0 AND 1000
        ORDER BY source.timestamp DESC
        LIMIT 100
        """
    }
}
