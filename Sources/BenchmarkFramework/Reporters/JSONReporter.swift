import Foundation

public struct JSONReporter: Sendable {
    public init() {}

    /// Write benchmark result to JSON file
    public static func write(_ result: BenchmarkResult, to path: String) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601

        let data = try encoder.encode(result)
        try data.write(to: URL(fileURLWithPath: path))
    }

    /// Write comparison result to JSON file
    public static func write(_ result: ComparisonResult, to path: String) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        encoder.dateEncodingStrategy = .iso8601

        let data = try encoder.encode(result)
        try data.write(to: URL(fileURLWithPath: path))
    }

    /// Write strategy comparison result to JSON file
    public static func write(_ result: StrategyComparisonResult, to path: String) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]

        let data = try encoder.encode(result)
        try data.write(to: URL(fileURLWithPath: path))
    }

    /// Write scalability result to JSON file
    public static func write(_ result: ScalabilityResult, to path: String) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]

        let data = try encoder.encode(result)
        try data.write(to: URL(fileURLWithPath: path))
    }

    /// Read benchmark result from JSON file
    public static func read(from path: String) throws -> BenchmarkResult {
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(BenchmarkResult.self, from: data)
    }

    /// Read comparison result from JSON file
    public static func readComparison(from path: String) throws -> ComparisonResult {
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        let decoder = JSONDecoder()
        decoder.dateDecodingStrategy = .iso8601
        return try decoder.decode(ComparisonResult.self, from: data)
    }
}
