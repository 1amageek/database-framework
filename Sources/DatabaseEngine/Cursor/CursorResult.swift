// CursorResult.swift
// DatabaseEngine - Result container for cursor iteration
//
// Reference: FDB Record Layer RecordCursorResult

import Foundation
import Core

// MARK: - CursorResult

/// Result from a cursor iteration
///
/// Contains the fetched items and a continuation token for resuming.
/// Following FDB Record Layer's RecordCursorResult pattern.
///
/// **Usage**:
/// ```swift
/// let result = try await cursor.next()
///
/// // Process items
/// for item in result.items {
///     process(item)
/// }
///
/// // Check for more data
/// if result.hasMore {
///     let nextResult = try await context.cursor(
///         User.self,
///         continuation: result.continuation
///     ).next()
/// }
/// ```
public struct CursorResult<T: Persistable>: Sendable {
    /// Items fetched in this batch
    public let items: [T]

    /// Continuation token for next batch (nil if no more results)
    public let continuation: ContinuationToken?

    /// Reason why iteration stopped (if continuation is nil)
    public let noNextReason: NoNextReason?

    // MARK: - Computed Properties

    /// Whether there are more results to fetch
    ///
    /// True if a continuation token is available and it's not the end marker.
    public var hasMore: Bool {
        guard let token = continuation else { return false }
        return !token.isEndOfResults
    }

    /// Number of items in this result
    public var count: Int {
        items.count
    }

    /// Whether this result is empty
    public var isEmpty: Bool {
        items.isEmpty
    }

    // MARK: - Initialization

    /// Create a result with items and continuation
    public init(
        items: [T],
        continuation: ContinuationToken?,
        noNextReason: NoNextReason? = nil
    ) {
        self.items = items
        self.continuation = continuation
        self.noNextReason = noNextReason
    }

    // MARK: - Factory Methods

    /// Create a result with more data available
    ///
    /// - Parameters:
    ///   - items: The fetched items
    ///   - continuation: Token for fetching next batch
    /// - Returns: A CursorResult indicating more data is available
    public static func more(items: [T], continuation: ContinuationToken) -> CursorResult {
        CursorResult(items: items, continuation: continuation, noNextReason: nil)
    }

    /// Create a result with no more data
    ///
    /// - Parameters:
    ///   - items: The final batch of items
    ///   - reason: Why iteration ended
    /// - Returns: A CursorResult indicating iteration is complete
    public static func done(
        items: [T],
        reason: NoNextReason = .sourceExhausted
    ) -> CursorResult {
        CursorResult(items: items, continuation: nil, noNextReason: reason)
    }

    /// Create an empty result
    ///
    /// - Parameter reason: Why no results were returned
    /// - Returns: An empty CursorResult
    public static func empty(reason: NoNextReason = .sourceExhausted) -> CursorResult {
        CursorResult(items: [], continuation: nil, noNextReason: reason)
    }
}

// MARK: - CursorResult Transformations

extension CursorResult {
    /// Map items to a different type
    ///
    /// - Parameter transform: Transformation function
    /// - Returns: New CursorResult with transformed items
    public func map<U: Persistable>(_ transform: (T) throws -> U) rethrows -> CursorResult<U> {
        CursorResult<U>(
            items: try items.map(transform),
            continuation: continuation,
            noNextReason: noNextReason
        )
    }

    /// Filter items
    ///
    /// - Parameter isIncluded: Predicate for inclusion
    /// - Returns: New CursorResult with filtered items
    public func filter(_ isIncluded: (T) throws -> Bool) rethrows -> CursorResult {
        CursorResult(
            items: try items.filter(isIncluded),
            continuation: continuation,
            noNextReason: noNextReason
        )
    }

    /// Compact map items
    ///
    /// - Parameter transform: Transformation function that may return nil
    /// - Returns: New CursorResult with non-nil transformed items
    public func compactMap<U: Persistable>(_ transform: (T) throws -> U?) rethrows -> CursorResult<U> {
        CursorResult<U>(
            items: try items.compactMap(transform),
            continuation: continuation,
            noNextReason: noNextReason
        )
    }
}

// MARK: - CustomStringConvertible

extension CursorResult: CustomStringConvertible {
    public var description: String {
        var parts: [String] = ["CursorResult(\(count) items"]
        if hasMore {
            parts.append("hasMore: true")
        } else if let reason = noNextReason {
            parts.append("reason: \(reason)")
        }
        parts.append(")")
        return parts.joined(separator: ", ")
    }
}

// MARK: - CursorResultPage

/// Extended cursor result with pagination metadata
///
/// Includes additional information useful for building pagination UIs.
public struct CursorResultPage<T: Persistable>: Sendable {
    /// The cursor result
    public let result: CursorResult<T>

    /// Page number (0-based)
    public let pageIndex: Int

    /// Number of pages fetched so far
    public let pagesFetched: Int

    /// Total items fetched across all pages
    public let totalItemsFetched: Int

    /// Convenience access to items
    public var items: [T] { result.items }

    /// Convenience access to continuation
    public var continuation: ContinuationToken? { result.continuation }

    /// Convenience access to hasMore
    public var hasMore: Bool { result.hasMore }

    public init(
        result: CursorResult<T>,
        pageIndex: Int,
        pagesFetched: Int,
        totalItemsFetched: Int
    ) {
        self.result = result
        self.pageIndex = pageIndex
        self.pagesFetched = pagesFetched
        self.totalItemsFetched = totalItemsFetched
    }
}
