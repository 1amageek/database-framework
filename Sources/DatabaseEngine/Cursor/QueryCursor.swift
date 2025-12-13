// QueryCursor.swift
// DatabaseEngine - Cursor for paginated query execution
//
// Reference: FDB Record Layer RecordCursor

import Foundation
import FoundationDB
import Core
import Synchronization

// MARK: - QueryCursor

/// A cursor for paginated query execution
///
/// Unlike `execute()` which returns all results, `QueryCursor` yields results
/// in batches with continuation tokens for resuming.
///
/// **Key Benefits**:
/// - Efficient pagination without re-scanning from start
/// - Stateless resumption across transactions/requests
/// - Memory-efficient streaming for large result sets
///
/// **Usage**:
/// ```swift
/// // First page
/// let cursor = try context.cursor(User.self)
///     .where(\.isActive == true)
///     .orderBy(\.createdAt, .descending)
///     .limit(20)
///     .build()
///
/// let result = try await cursor.next()
/// displayUsers(result.items)
///
/// // Next page (can be in a different request/session)
/// if let continuation = result.continuation {
///     let nextCursor = try context.cursor(User.self, continuation: continuation).build()
///     let nextResult = try await nextCursor.next()
/// }
/// ```
///
/// **Reference**: FDB Record Layer RecordCursor
public final class QueryCursor<T: Persistable & Codable>: @unchecked Sendable {

    // MARK: - Properties

    private let context: FDBContext
    private let query: Query<T>
    private let batchSize: Int
    private let state: Mutex<CursorState>

    /// Plan fingerprint for token validation
    private let planFingerprint: [UInt8]

    // MARK: - State

    private struct CursorState: Sendable {
        var currentContinuation: ContinuationState?
        var exhausted: Bool = false
        var itemsReturned: Int = 0
        var pagesReturned: Int = 0
    }

    // MARK: - Initialization

    /// Create a new cursor from a query
    ///
    /// - Parameters:
    ///   - context: The FDBContext for database access
    ///   - query: The query to execute
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - continuation: Optional continuation token to resume from
    /// - Throws: `ContinuationError` if token is invalid
    internal init(
        context: FDBContext,
        query: Query<T>,
        batchSize: Int = 100,
        continuation: ContinuationToken? = nil
    ) throws {
        self.context = context
        self.query = query
        self.batchSize = batchSize

        // Compute plan fingerprint
        let sortFields = query.sortDescriptors.map { desc -> String in
            // Extract field name from KeyPath
            String(describing: desc.keyPath)
        }
        self.planFingerprint = PlanFingerprint.compute(
            operatorDescription: String(describing: T.self),
            indexNames: T.indexDescriptors.map { $0.name },
            sortFields: sortFields
        )

        // Initialize state
        var initialState = CursorState()
        if let token = continuation, !token.isEndOfResults {
            let contState = try ContinuationState.fromToken(token)

            // Validate plan fingerprint matches
            if contState.planFingerprint != planFingerprint {
                throw ContinuationError.planMismatch(
                    "Continuation was created for a different query"
                )
            }
            initialState.currentContinuation = contState
        }

        self.state = Mutex(initialState)
    }

    // MARK: - Public API

    /// Fetch the next batch of results
    ///
    /// - Returns: CursorResult containing items and optional continuation
    /// - Throws: Database or continuation errors
    public func next() async throws -> CursorResult<T> {
        let (isExhausted, currentCont) = state.withLock { state in
            (state.exhausted, state.currentContinuation)
        }

        if isExhausted {
            return .empty(reason: .sourceExhausted)
        }

        // Execute with continuation
        let (items, nextContinuation, stopReason) = try await executeWithContinuation(
            continuation: currentCont
        )

        // Update state
        state.withLock { state in
            state.itemsReturned += items.count
            state.pagesReturned += 1
            if let nextCont = nextContinuation {
                state.currentContinuation = nextCont
            } else {
                state.exhausted = true
            }
        }

        if let nextCont = nextContinuation {
            return .more(items: items, continuation: nextCont.toToken())
        } else {
            return .done(items: items, reason: stopReason ?? .sourceExhausted)
        }
    }

    /// Stream all remaining results as an async sequence
    ///
    /// **Usage**:
    /// ```swift
    /// let cursor = try context.cursor(User.self).build()
    /// for try await user in cursor.stream() {
    ///     process(user)
    /// }
    /// ```
    public func stream() -> AsyncThrowingStream<T, Error> {
        AsyncThrowingStream { continuation in
            Task {
                do {
                    while true {
                        let result = try await self.next()
                        for item in result.items {
                            continuation.yield(item)
                        }
                        if !result.hasMore {
                            break
                        }
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
        }
    }

    /// Collect all remaining results into an array
    ///
    /// **Warning**: This loads all results into memory. Use `stream()` or
    /// paginated `next()` calls for large result sets.
    public func collect() async throws -> [T] {
        var all: [T] = []
        while true {
            let result = try await next()
            all.append(contentsOf: result.items)
            if !result.hasMore {
                break
            }
        }
        return all
    }

    /// Get cursor statistics
    public var statistics: CursorStatistics {
        state.withLock { state in
            CursorStatistics(
                itemsReturned: state.itemsReturned,
                pagesReturned: state.pagesReturned,
                isExhausted: state.exhausted
            )
        }
    }

    // MARK: - Private Implementation

    /// Execute query with continuation support
    private func executeWithContinuation(
        continuation: ContinuationState?
    ) async throws -> (items: [T], nextContinuation: ContinuationState?, stopReason: NoNextReason?) {
        // Determine effective limit
        let effectiveLimit: Int
        if let remaining = continuation?.remainingLimit {
            effectiveLimit = min(batchSize, remaining)
        } else if let queryLimit = query.fetchLimit {
            let returned = state.withLock { $0.itemsReturned }
            effectiveLimit = min(batchSize, queryLimit - returned)
        } else {
            effectiveLimit = batchSize
        }

        if effectiveLimit <= 0 {
            return ([], nil, .returnLimitReached)
        }

        // Build modified query with continuation
        var modifiedQuery = query

        // If we have a continuation, we need to start after the last key
        // For now, we use offset-based continuation (will be optimized to key-based)
        if continuation != nil {
            // Calculate offset based on items already returned
            let itemsReturned = state.withLock { $0.itemsReturned }
            modifiedQuery = modifiedQuery.offset(itemsReturned)
        }

        // Set limit to fetch one extra to detect if more exist
        modifiedQuery = modifiedQuery.limit(effectiveLimit + 1)

        // Execute query
        let results = try await context.fetch(modifiedQuery)

        // Check if there are more results
        let hasMore = results.count > effectiveLimit
        let returnedItems = hasMore ? Array(results.prefix(effectiveLimit)) : results

        // Build next continuation if there are more results
        let nextContinuation: ContinuationState?
        if hasMore {
            // Calculate remaining limit
            let newRemaining: Int?
            if let queryLimit = query.fetchLimit {
                let totalReturned = state.withLock { $0.itemsReturned } + returnedItems.count
                newRemaining = queryLimit - totalReturned
                if newRemaining! <= 0 {
                    return (returnedItems, nil, .returnLimitReached)
                }
            } else {
                newRemaining = nil
            }

            nextContinuation = ContinuationState(
                scanType: .tableScan,  // Will be determined by actual scan type
                lastKey: [],  // Will be populated with actual last key
                reverse: query.sortDescriptors.first?.order == .descending,
                remainingLimit: newRemaining,
                originalLimit: query.fetchLimit,
                planFingerprint: planFingerprint
            )
        } else {
            nextContinuation = nil
        }

        return (returnedItems, nextContinuation, hasMore ? nil : .sourceExhausted)
    }
}

// MARK: - CursorStatistics

/// Statistics about cursor execution
public struct CursorStatistics: Sendable {
    /// Total items returned across all pages
    public let itemsReturned: Int

    /// Number of pages (next() calls) completed
    public let pagesReturned: Int

    /// Whether the cursor has reached the end
    public let isExhausted: Bool
}

// MARK: - CursorQueryBuilder

/// Builder for cursor-based queries
///
/// Provides a fluent API similar to QueryExecutor but builds a QueryCursor
/// instead of executing immediately.
///
/// **Usage**:
/// ```swift
/// let cursor = try context.cursor(User.self)
///     .where(\.isActive == true)
///     .orderBy(\.name)
///     .limit(100)  // Total limit
///     .batchSize(20)  // Per-page limit
///     .build()
///
/// let firstPage = try await cursor.next()
/// ```
public struct CursorQueryBuilder<T: Persistable & Codable>: Sendable {
    private let context: FDBContext
    private let continuation: ContinuationToken?
    private var query: Query<T>
    private var _batchSize: Int = 100

    // MARK: - Initialization

    internal init(context: FDBContext, continuation: ContinuationToken? = nil) {
        self.context = context
        self.continuation = continuation
        self.query = Query<T>()
    }

    // MARK: - Fluent API

    /// Add a filter predicate
    public func `where`(_ predicate: Predicate<T>) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.where(predicate)
        return copy
    }

    /// Add sort order (ascending)
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.orderBy(keyPath)
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(
        _ keyPath: KeyPath<T, V>,
        _ order: SortOrder
    ) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.orderBy(keyPath, order)
        return copy
    }

    /// Set total maximum number of results (across all pages)
    public func limit(_ count: Int) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.limit(count)
        return copy
    }

    // MARK: - Partition

    /// Bind a partition field value for dynamic directory resolution
    ///
    /// Required for types with `Field(\.keyPath)` in their `#Directory` declaration.
    /// The partition value is used to resolve the correct directory subspace.
    ///
    /// **Usage**:
    /// ```swift
    /// let cursor = try await context.cursor(Order.self)
    ///     .partition(\.tenantID, equals: "tenant_123")
    ///     .where(\.status == "open")
    ///     .batchSize(50)
    ///     .build()
    /// ```
    ///
    /// - Parameters:
    ///   - keyPath: The partition field's keyPath
    ///   - value: The value for directory resolution
    /// - Returns: A new CursorQueryBuilder with the partition binding added
    public func partition<V: Sendable & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.partition(keyPath, equals: value)
        return copy
    }

    /// Set the batch size (items per page)
    ///
    /// - Parameter size: Number of items per cursor.next() call
    /// - Returns: Updated builder
    public func batchSize(_ size: Int) -> CursorQueryBuilder<T> {
        var copy = self
        copy._batchSize = size
        return copy
    }

    /// Build and return the cursor
    ///
    /// - Returns: A QueryCursor ready for iteration
    /// - Throws: `ContinuationError` if continuation token is invalid
    public func build() throws -> QueryCursor<T> {
        try QueryCursor(
            context: context,
            query: query,
            batchSize: _batchSize,
            continuation: continuation
        )
    }

    /// Convenience: fetch first page directly
    ///
    /// - Returns: First page of results with continuation
    /// - Throws: Database or continuation errors
    public func next() async throws -> CursorResult<T> {
        let cursor = try build()
        return try await cursor.next()
    }
}
