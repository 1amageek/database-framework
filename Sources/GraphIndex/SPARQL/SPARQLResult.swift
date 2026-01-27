// SPARQLResult.swift
// GraphIndex - SPARQL-like query results
//
// Represents the result set of a SPARQL query execution.

import Foundation
import Core

/// Result of a SPARQL-like query execution
///
/// Contains all solution bindings plus metadata about the query execution.
///
/// **Usage**:
/// ```swift
/// let result = try await context.sparql(Statement.self)
///     .defaultIndex()
///     .where("Alice", "knows", "?friend")
///     .execute()
///
/// print("Found \(result.count) friends")
/// for binding in result.bindings {
///     print("Friend: \(binding["?friend"]!)")
/// }
/// ```
public struct SPARQLResult: Sendable {

    /// All solution bindings (rows)
    public let bindings: [VariableBinding]

    /// Variables that were projected in the query
    public let projectedVariables: [String]

    /// Whether the result set is complete (not truncated by limit)
    public let isComplete: Bool

    /// Reason for incompleteness (if any)
    public let limitReason: SPARQLLimitReason?

    /// Execution statistics
    public let statistics: ExecutionStatistics

    // MARK: - Initialization

    public init(
        bindings: [VariableBinding],
        projectedVariables: [String],
        isComplete: Bool = true,
        limitReason: SPARQLLimitReason? = nil,
        statistics: ExecutionStatistics = ExecutionStatistics()
    ) {
        self.bindings = bindings
        self.projectedVariables = projectedVariables
        self.isComplete = isComplete
        self.limitReason = limitReason
        self.statistics = statistics
    }

    // MARK: - Convenience Properties

    /// Number of solutions
    public var count: Int {
        bindings.count
    }

    /// Whether the result is empty
    public var isEmpty: Bool {
        bindings.isEmpty
    }

    /// Get the first binding (if any)
    public var first: VariableBinding? {
        bindings.first
    }

    // MARK: - Variable Access

    /// Get typed values for a specific variable across all bindings
    ///
    /// Returns `nil` for bindings where the variable is not bound.
    public func values(for variable: String) -> [FieldValue?] {
        bindings.map { $0[variable] }
    }

    /// Get non-nil typed values for a specific variable
    public func nonNilValues(for variable: String) -> [FieldValue] {
        bindings.compactMap { $0[variable] }
    }

    /// Get distinct typed values for a variable
    public func distinctValues(for variable: String) -> Set<FieldValue> {
        Set(bindings.compactMap { $0[variable] })
    }

    /// Get string values for a specific variable across all bindings
    public func stringValues(for variable: String) -> [String?] {
        bindings.map { $0.string(variable) }
    }

    /// Check if a variable has any bound values
    public func hasValues(for variable: String) -> Bool {
        bindings.contains { $0[variable] != nil }
    }

    // MARK: - Transformation

    /// Map bindings to a different type
    public func map<T>(_ transform: (VariableBinding) -> T) -> [T] {
        bindings.map(transform)
    }

    /// Filter bindings
    public func filter(_ predicate: (VariableBinding) -> Bool) -> SPARQLResult {
        SPARQLResult(
            bindings: bindings.filter(predicate),
            projectedVariables: projectedVariables,
            isComplete: isComplete,
            limitReason: limitReason,
            statistics: statistics
        )
    }

    /// Get a subset of bindings
    public func prefix(_ maxLength: Int) -> SPARQLResult {
        SPARQLResult(
            bindings: Array(bindings.prefix(maxLength)),
            projectedVariables: projectedVariables,
            isComplete: bindings.count <= maxLength && isComplete,
            limitReason: bindings.count > maxLength ? .explicitLimit : limitReason,
            statistics: statistics
        )
    }
}

// MARK: - Sequence Conformance

extension SPARQLResult: Sequence {
    public func makeIterator() -> Array<VariableBinding>.Iterator {
        bindings.makeIterator()
    }
}

// MARK: - CustomStringConvertible

extension SPARQLResult: CustomStringConvertible {
    public var description: String {
        let vars = projectedVariables.joined(separator: ", ")
        return "SPARQLResult(variables: [\(vars)], count: \(count), complete: \(isComplete))"
    }
}

// MARK: - Supporting Types

/// Reason why a SPARQL result set might be incomplete
public enum SPARQLLimitReason: Sendable, Equatable {
    /// User-specified LIMIT was reached
    case explicitLimit

    /// Internal result limit was reached
    case internalLimit(Int)

    /// Timeout occurred
    case timeout

    /// Memory limit was reached
    case memoryLimit
}

/// Execution statistics for query analysis
public struct ExecutionStatistics: Sendable {
    /// Number of index scans performed
    public var indexScans: Int

    /// Number of join operations
    public var joinOperations: Int

    /// Number of intermediate results processed
    public var intermediateResults: Int

    /// Patterns evaluated
    public var patternsEvaluated: Int

    /// Total execution time in nanoseconds
    public var durationNs: UInt64

    /// Number of optional patterns that didn't match
    public var optionalMisses: Int

    public init(
        indexScans: Int = 0,
        joinOperations: Int = 0,
        intermediateResults: Int = 0,
        patternsEvaluated: Int = 0,
        durationNs: UInt64 = 0,
        optionalMisses: Int = 0
    ) {
        self.indexScans = indexScans
        self.joinOperations = joinOperations
        self.intermediateResults = intermediateResults
        self.patternsEvaluated = patternsEvaluated
        self.durationNs = durationNs
        self.optionalMisses = optionalMisses
    }

    /// Execution time in milliseconds
    public var durationMs: Double {
        Double(durationNs) / 1_000_000
    }

    /// Execution time in seconds
    public var durationSeconds: Double {
        Double(durationNs) / 1_000_000_000
    }
}

extension ExecutionStatistics: CustomStringConvertible {
    public var description: String {
        "ExecutionStatistics(scans: \(indexScans), joins: \(joinOperations), duration: \(String(format: "%.2f", durationMs))ms)"
    }
}
