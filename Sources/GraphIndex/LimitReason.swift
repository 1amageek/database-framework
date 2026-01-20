// LimitReason.swift
// GraphIndex - Limit reason enumeration for incomplete results
//
// Provides detailed information about why a graph operation was incomplete.

import Foundation

// MARK: - LimitReason

/// Reason why a graph operation did not complete fully.
///
/// Graph algorithms may stop early due to various limits (node count, depth, etc.).
/// This enum provides detailed information about which limit was reached.
///
/// **Design Reference**: Inspired by FDB Record Layer's `NoNextReason` pattern,
/// which always indicates whether results are complete or truncated.
///
/// **Usage**:
/// ```swift
/// let result = try await sorter.sort()
/// if let reason = result.limitReason {
///     switch reason {
///     case .maxNodesReached(let explored, let limit):
///         print("Only explored \(explored) of potentially more nodes (limit: \(limit))")
///     default:
///         print("Result incomplete: \(reason)")
///     }
/// }
/// ```
public enum LimitReason: Sendable, Equatable {

    /// Maximum node exploration limit was reached.
    ///
    /// - Parameters:
    ///   - explored: Number of nodes actually explored
    ///   - limit: The configured maximum
    case maxNodesReached(explored: Int, limit: Int)

    /// Maximum traversal depth was reached.
    ///
    /// - Parameters:
    ///   - depth: The depth at which traversal stopped
    ///   - limit: The configured maximum depth
    case maxDepthReached(depth: Int, limit: Int)

    /// Maximum cycle detection limit was reached.
    ///
    /// - Parameters:
    ///   - found: Number of cycles found before stopping
    ///   - limit: The configured maximum cycles to detect
    case maxCyclesReached(found: Int, limit: Int)

    /// Maximum result count limit was reached.
    ///
    /// - Parameters:
    ///   - returned: Number of results returned
    ///   - limit: The configured maximum results
    case maxResultsReached(returned: Int, limit: Int)
}

// MARK: - CustomStringConvertible

extension LimitReason: CustomStringConvertible {

    public var description: String {
        switch self {
        case .maxNodesReached(let explored, let limit):
            return "maxNodesReached(explored: \(explored), limit: \(limit))"
        case .maxDepthReached(let depth, let limit):
            return "maxDepthReached(depth: \(depth), limit: \(limit))"
        case .maxCyclesReached(let found, let limit):
            return "maxCyclesReached(found: \(found), limit: \(limit))"
        case .maxResultsReached(let returned, let limit):
            return "maxResultsReached(returned: \(returned), limit: \(limit))"
        }
    }
}
