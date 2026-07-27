// QueryCursor.swift
// DatabaseEngine - Cursor for paginated query execution
//
// Reference: FDB Record Layer RecordCursor

import DatabaseKit
import DatabaseTypes

// MARK: - QueryCursor

/// Failures raised by typed cursor lifecycle and bounds validation.
public enum QueryCursorError: Error, Sendable, Equatable {
    case invalidBatchSize(actual: Int, allowed: ClosedRange<Int>)
    case closed
    case positionOutOfRange(UInt64)
    case positionOverflow
}

/// A serialized cursor for bounded typed-query pagination.
///
/// Unlike `execute()` which returns all results, `QueryCursor` yields results
/// in batches with continuation tokens for resuming.
///
/// Continuations are stateless and bind their logical position to the complete
/// canonical query. The current typed fetch pipeline applies that position as
/// an offset. This gives correct cross-request resumption, but a backend may
/// still scan preceding rows when its selected access path cannot push down
/// the offset.
///
/// **Usage**:
/// ```swift
/// // First page
/// let cursor = try context.cursor(User.self)
///     .where(User.fields.isActive == true)
///     .orderBy(User.fields.createdAt, .descending)
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
public actor QueryCursor<T: Persistable> {

    // MARK: - Properties

    public static var allowedBatchSizes: ClosedRange<Int> {
        1...10_000
    }

    private let context: DatabaseContext
    private let query: Query<T>
    private let batchSize: Int
    private let baseOffset: UInt64
    private let queryFingerprint: ByteString
    private var state: CursorState
    private var executionIsHeld = false
    private var executionWaiters: [CheckedContinuation<Void, Never>] = []

    // MARK: - State

    private struct CursorState: Sendable {
        var nextOffset: UInt64
        var remainingLimit: UInt64?
        var exhausted: Bool = false
        var closed: Bool = false
        var itemsReturned: Int = 0
        var pagesReturned: Int = 0
    }

    private struct PageExecution {
        let items: [T]
        let nextOffset: UInt64
        let remainingLimit: UInt64?
        let continuation: ContinuationToken?
        let stopReason: NoNextReason?
    }

    // MARK: - Initialization

    /// Create a new cursor from a query
    ///
    /// - Parameters:
    ///   - context: The DatabaseContext for database access
    ///   - query: The query to execute
    ///   - batchSize: Number of items per batch (default: 100)
    ///   - continuation: Optional continuation token to resume from
    /// - Throws: `ContinuationError` if token is invalid
    internal init(
        context: DatabaseContext,
        query: Query<T>,
        batchSize: Int = 100,
        continuation: ContinuationToken? = nil
    ) throws {
        guard Self.allowedBatchSizes.contains(batchSize) else {
            throw QueryCursorError.invalidBatchSize(
                actual: batchSize,
                allowed: Self.allowedBatchSizes
            )
        }
        self.context = context
        self.query = query
        self.batchSize = batchSize
        self.queryFingerprint = try QueryFingerprint.compute(for: query)
        self.baseOffset = try Self.validatedBaseOffset(query.fetchOffset)
        let queryLimit = try Self.validatedLimit(query.fetchLimit)

        if let continuation {
            let continuationState = try ContinuationState.decode(continuation)
            guard continuationState.queryFingerprint == queryFingerprint else {
                throw ContinuationError.planMismatch(
                    "Continuation was created for a different query"
                )
            }
            let returned = try Self.validate(
                continuationState,
                baseOffset: baseOffset,
                queryLimit: queryLimit
            )
            self.state = CursorState(
                nextOffset: continuationState.nextOffset,
                remainingLimit: continuationState.remainingLimit,
                itemsReturned: returned
            )
        } else {
            self.state = CursorState(
                nextOffset: baseOffset,
                remainingLimit: queryLimit
            )
        }
    }

    // MARK: - Public API

    /// Fetch the next batch of results
    ///
    /// - Returns: CursorResult containing items and optional continuation
    /// - Throws: Database or continuation errors
    public func next() async throws -> CursorResult<T> {
        await acquireExecution()
        defer { releaseExecution() }
        try Task.checkCancellation()
        guard !state.closed else {
            throw QueryCursorError.closed
        }
        guard !state.exhausted else {
            return .empty(reason: .sourceExhausted)
        }

        let execution = try await executePage(
            offset: state.nextOffset,
            remainingLimit: state.remainingLimit
        )
        try Task.checkCancellation()
        guard !state.closed else {
            throw QueryCursorError.closed
        }
        state.nextOffset = execution.nextOffset
        state.remainingLimit = execution.remainingLimit
        state.itemsReturned += execution.items.count
        state.pagesReturned += 1
        state.exhausted = execution.continuation == nil

        if let continuation = execution.continuation {
            return .more(
                items: execution.items,
                continuation: continuation
            )
        }
        return .done(
            items: execution.items,
            reason: execution.stopReason ?? .sourceExhausted
        )
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
    public nonisolated func stream() -> AsyncThrowingStream<T, any Error> {
        AsyncThrowingStream { continuation in
            let producer = Task {
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
            continuation.onTermination = { _ in
                producer.cancel()
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

    /// Close the cursor and reject subsequent reads.
    ///
    /// An in-flight backend request is discarded when it returns. The task
    /// performing that request remains responsible for cancellation of the
    /// backend operation itself.
    public func shutdown() {
        state.closed = true
        state.exhausted = true
    }

    /// Get cursor statistics
    public var statistics: CursorStatistics {
        CursorStatistics(
            itemsReturned: state.itemsReturned,
            pagesReturned: state.pagesReturned,
            isExhausted: state.exhausted
        )
    }

    // MARK: - Private Implementation

    private func acquireExecution() async {
        guard executionIsHeld else {
            executionIsHeld = true
            return
        }
        await withCheckedContinuation { continuation in
            executionWaiters.append(continuation)
        }
    }

    private func releaseExecution() {
        guard !executionWaiters.isEmpty else {
            executionIsHeld = false
            return
        }
        executionWaiters.removeFirst().resume()
    }

    private func executePage(
        offset: UInt64,
        remainingLimit: UInt64?
    ) async throws -> PageExecution {
        let effectiveLimit: Int
        if let remainingLimit {
            effectiveLimit = min(
                batchSize,
                Int(clamping: remainingLimit)
            )
        } else {
            effectiveLimit = batchSize
        }
        guard effectiveLimit > 0 else {
            return PageExecution(
                items: [],
                nextOffset: offset,
                remainingLimit: 0,
                continuation: nil,
                stopReason: .returnLimitReached
            )
        }
        guard let integerOffset = Int(exactly: offset) else {
            throw QueryCursorError.positionOutOfRange(offset)
        }
        var modifiedQuery = query.offset(integerOffset)
        modifiedQuery = modifiedQuery.limit(effectiveLimit + 1)
        let results = try await context.fetch(modifiedQuery)
        let hasMore = results.count > effectiveLimit
        // Detaching the bounded prefix is intentional: retaining the probe row
        // would keep an oversized result allocation alive across API boundaries.
        let returnedItems = hasMore ? Array(results.prefix(effectiveLimit)) : results
        let returnedCount = UInt64(returnedItems.count)
        let (nextOffset, offsetOverflow) =
            offset.addingReportingOverflow(returnedCount)
        guard !offsetOverflow else {
            throw QueryCursorError.positionOverflow
        }
        let nextRemaining = remainingLimit.map {
            $0 >= returnedCount ? $0 - returnedCount : 0
        }
        if nextRemaining == 0 {
            return PageExecution(
                items: returnedItems,
                nextOffset: nextOffset,
                remainingLimit: nextRemaining,
                continuation: nil,
                stopReason: .returnLimitReached
            )
        }
        guard hasMore else {
            return PageExecution(
                items: returnedItems,
                nextOffset: nextOffset,
                remainingLimit: nextRemaining,
                continuation: nil,
                stopReason: .sourceExhausted
            )
        }
        let continuation = try ContinuationState(
            nextOffset: nextOffset,
            remainingLimit: nextRemaining,
            queryFingerprint: queryFingerprint
        ).token()
        return PageExecution(
            items: returnedItems,
            nextOffset: nextOffset,
            remainingLimit: nextRemaining,
            continuation: continuation,
            stopReason: nil
        )
    }

    private static func validatedBaseOffset(
        _ offset: Int?
    ) throws -> UInt64 {
        guard let offset else {
            return 0
        }
        guard let value = UInt64(exactly: offset) else {
            throw QueryConversionError.negativeOffset(offset)
        }
        return value
    }

    private static func validatedLimit(
        _ limit: Int?
    ) throws -> UInt64? {
        guard let limit else {
            return nil
        }
        guard let value = UInt64(exactly: limit) else {
            throw QueryConversionError.negativeLimit(limit)
        }
        return value
    }

    private static func validate(
        _ continuation: ContinuationState,
        baseOffset: UInt64,
        queryLimit: UInt64?
    ) throws -> Int {
        guard continuation.nextOffset >= baseOffset else {
            throw ContinuationError.corruptedToken
        }
        let returned = continuation.nextOffset - baseOffset
        switch (queryLimit, continuation.remainingLimit) {
        case let (.some(limit), .some(remaining)):
            guard remaining <= limit, limit - remaining == returned else {
                throw ContinuationError.corruptedToken
            }
        case (.none, .none):
            break
        case (.some, .none), (.none, .some):
            throw ContinuationError.corruptedToken
        }
        guard let result = Int(exactly: returned) else {
            throw ContinuationError.corruptedToken
        }
        return result
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
///     .where(#field(\User.isActive) == true)
///     .orderBy(#field(\User.name))
///     .limit(100)  // Total limit
///     .batchSize(20)  // Per-page limit
///     .build()
///
/// let firstPage = try await cursor.next()
/// ```
public struct CursorQueryBuilder<T: Persistable>: Sendable {
    private let context: DatabaseContext
    private let continuation: ContinuationToken?
    private var query: Query<T>
    private var _batchSize: Int = 100

    // MARK: - Initialization

    internal init(context: DatabaseContext, continuation: ContinuationToken? = nil) {
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
    public func orderBy<V: Comparable & Sendable>(
        _ field: Field<T, V>
    ) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.orderBy(field)
        return copy
    }

    /// Add sort order with direction
    public func orderBy<V: Comparable & Sendable>(
        _ field: Field<T, V>,
        _ order: SortOrder
    ) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.orderBy(field, order)
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
    ///     .partition(#field(\Order.tenantID), equals: "tenant_123")
    ///     .where(#field(\Order.status) == "open")
    ///     .batchSize(50)
    ///     .build()
    /// ```
    ///
    /// - Parameters:
    ///   - field: The compiled partition field
    ///   - value: The value for directory resolution
    /// - Returns: A new CursorQueryBuilder with the partition binding added
    public func partition<V: Sendable & Equatable & FieldValueRepresentable>(
        _ field: Field<T, V>,
        equals value: V
    ) -> CursorQueryBuilder<T> {
        var copy = self
        copy.query = query.partition(field, equals: value)
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
