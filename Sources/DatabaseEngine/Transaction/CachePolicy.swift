// CachePolicy.swift
// DatabaseEngine - Cache policy for read operations

import Foundation

/// Cache policy for read operations
///
/// Controls whether read operations use cached read versions
/// for performance optimization.
///
/// **Usage**:
/// ```swift
/// // Default: strict consistency (no cache)
/// let users = try await context.fetch(User.self)
///     .execute()
///
/// // Use cache if available (no time limit)
/// let users = try await context.fetch(User.self)
///     .cachePolicy(.cached)
///     .execute()
///
/// // Use cache only if younger than 30 seconds
/// let users = try await context.fetch(User.self)
///     .cachePolicy(.stale(30))
///     .execute()
/// ```
public enum CachePolicy: Sendable, Hashable {
    /// Fetch latest data from server (no cache)
    ///
    /// Use for:
    /// - Read-after-write consistency
    /// - Critical business logic
    /// - When staleness is not acceptable
    case server

    /// Use cached read version if available (no time limit)
    ///
    /// Returns cached version regardless of how old it is.
    /// Use when you want to minimize server round-trips and
    /// can tolerate potentially stale data.
    case cached

    /// Use cached read version only if younger than specified duration
    ///
    /// - Parameter seconds: Maximum staleness in seconds
    case stale(TimeInterval)
}

extension CachePolicy: CustomStringConvertible {
    public var description: String {
        switch self {
        case .server:
            return "CachePolicy.server"
        case .cached:
            return "CachePolicy.cached"
        case .stale(let seconds):
            return "CachePolicy.stale(\(Int(seconds))s)"
        }
    }
}
