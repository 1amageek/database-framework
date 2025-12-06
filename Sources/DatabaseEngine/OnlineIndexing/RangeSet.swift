import FoundationDB

// MARK: - Byte Array Comparison Helpers

/// Compare two byte arrays lexicographically
/// - Returns: true if lhs < rhs
private func bytesLessThan(_ lhs: FDB.Bytes, _ rhs: FDB.Bytes) -> Bool {
    let minLength = min(lhs.count, rhs.count)
    for i in 0..<minLength {
        if lhs[i] < rhs[i] { return true }
        if lhs[i] > rhs[i] { return false }
    }
    return lhs.count < rhs.count
}

/// Compare two byte arrays lexicographically
/// - Returns: true if lhs <= rhs
private func bytesLessThanOrEqual(_ lhs: FDB.Bytes, _ rhs: FDB.Bytes) -> Bool {
    return lhs == rhs || bytesLessThan(lhs, rhs)
}

/// Compare two byte arrays lexicographically
/// - Returns: true if lhs > rhs
private func bytesGreaterThan(_ lhs: FDB.Bytes, _ rhs: FDB.Bytes) -> Bool {
    return bytesLessThan(rhs, lhs)
}

/// Compare two byte arrays lexicographically
/// - Returns: true if lhs >= rhs
private func bytesGreaterThanOrEqual(_ lhs: FDB.Bytes, _ rhs: FDB.Bytes) -> Bool {
    return lhs == rhs || bytesGreaterThan(lhs, rhs)
}

/// Return the maximum of two byte arrays
private func bytesMax(_ lhs: FDB.Bytes, _ rhs: FDB.Bytes) -> FDB.Bytes {
    return bytesGreaterThanOrEqual(lhs, rhs) ? lhs : rhs
}

// MARK: - RangeContinuation

/// Represents a range with continuation support for resumable processing
///
/// This enables the "continuation pattern" used by FDB Record Layer:
/// - Process N records from the range
/// - Record the last processed key as continuation
/// - Next batch starts AFTER the continuation key
///
/// **Reference**: FDB Record Layer's `ScanProperties` and continuation handling
public struct RangeContinuation: Sendable, Codable, Equatable {
    /// Beginning of the range (inclusive)
    public let rangeBegin: FDB.Bytes

    /// End of the range (exclusive)
    public let rangeEnd: FDB.Bytes

    /// Last successfully processed key (nil if not started)
    /// Next batch starts AFTER this key (firstGreaterThan)
    public var lastProcessedKey: FDB.Bytes?

    /// Whether this range has been fully processed
    public var isComplete: Bool

    /// Initialize a new range continuation
    public init(begin: FDB.Bytes, end: FDB.Bytes) {
        self.rangeBegin = begin
        self.rangeEnd = end
        self.lastProcessedKey = nil
        self.isComplete = false
    }

    /// Get the effective start key for the next batch
    ///
    /// If we have a continuation, start AFTER it (exclusive).
    /// Otherwise start at the range beginning (inclusive).
    public var nextBatchBegin: FDB.Bytes {
        if let lastKey = lastProcessedKey {
            // Add a zero byte to make it "first greater than lastKey"
            return lastKey + [0x00]
        }
        return rangeBegin
    }

    /// Check if there's more to process in this range
    public var hasMoreToProcess: Bool {
        guard !isComplete else { return false }

        if let lastKey = lastProcessedKey {
            // Check if we've reached or passed the end
            return bytesLessThan(lastKey, rangeEnd)
        }
        // Not started yet, check if range is valid
        return bytesLessThan(rangeBegin, rangeEnd)
    }
}

// MARK: - RangeSet

/// A set of ranges for tracking progress in batch operations
///
/// RangeSet maintains a collection of key ranges with continuation support,
/// enabling resumable batch processing across transaction boundaries.
///
/// **Key Design Principles**:
/// 1. **Continuation-based**: Uses last processed key, not range splitting
/// 2. **Transaction-safe**: Progress is recorded per batch, not per range
/// 3. **FDB-native batching**: Actual batching via `getRange(limit:)`
///
/// **Usage Pattern**:
/// ```swift
/// var rangeSet = RangeSet(initialRange: totalRange)
///
/// while let bounds = rangeSet.nextBatchBounds() {
///     try await database.withTransaction { tx in
///         var lastKey: FDB.Bytes? = nil
///         var count = 0
///
///         // FDB limits the batch, not RangeSet
///         let sequence = tx.getRange(
///             begin: bounds.begin,
///             end: bounds.end,
///             limit: batchSize
///         )
///
///         for try await (key, value) in sequence {
///             // Process item
///             lastKey = Array(key)
///             count += 1
///         }
///
///         // Record progress
///         if let lastKey = lastKey {
///             let isComplete = count < batchSize
///             rangeSet.recordProgress(lastProcessedKey: lastKey, isComplete: isComplete)
///         }
///
///         // Save to FDB
///         saveProgress(rangeSet, tx)
///     }
/// }
/// ```
public struct RangeSet: Sendable, Codable {
    /// A single range of keys (for backwards compatibility)
    public struct Range: Sendable, Codable, Equatable {
        /// Beginning of range (inclusive)
        public let begin: FDB.Bytes

        /// End of range (exclusive)
        public let end: FDB.Bytes

        public init(begin: FDB.Bytes, end: FDB.Bytes) {
            self.begin = begin
            self.end = end
        }

        public func contains(_ key: FDB.Bytes) -> Bool {
            return bytesGreaterThanOrEqual(key, begin) && bytesLessThan(key, end)
        }

        public var estimatedSize: Int {
            return max(0, end.count - begin.count)
        }
    }

    // MARK: - Properties

    /// Range continuations (sorted by rangeBegin)
    private var continuations: [RangeContinuation]

    /// Index of the currently processing range
    private var currentIndex: Int

    // MARK: - Initialization

    /// Initialize with a single range
    public init(initialRange: (begin: FDB.Bytes, end: FDB.Bytes)) {
        self.continuations = [RangeContinuation(begin: initialRange.begin, end: initialRange.end)]
        self.currentIndex = 0
    }

    /// Initialize with multiple ranges
    public init(ranges: [Range]) {
        self.continuations = ranges
            .sorted { bytesLessThan($0.begin, $1.begin) }
            .map { RangeContinuation(begin: $0.begin, end: $0.end) }
        self.currentIndex = 0
    }

    /// Initialize with continuations (for deserialization)
    public init(continuations: [RangeContinuation], currentIndex: Int = 0) {
        self.continuations = continuations
        self.currentIndex = currentIndex
    }

    // MARK: - Query

    /// Check if there are no more ranges to process
    public var isEmpty: Bool {
        // Find first incomplete range
        for continuation in continuations {
            if continuation.hasMoreToProcess {
                return false
            }
        }
        return true
    }

    /// Number of ranges remaining (including partially processed)
    public var count: Int {
        return continuations.filter { $0.hasMoreToProcess }.count
    }

    /// Total estimated size of all ranges
    public var estimatedTotalSize: Int {
        return continuations.reduce(0) { total, cont in
            if cont.isComplete { return total }
            let begin = cont.lastProcessedKey ?? cont.rangeBegin
            return total + max(0, cont.rangeEnd.count - begin.count)
        }
    }

    /// Get all remaining ranges (for compatibility)
    public var ranges: [Range] {
        return continuations
            .filter { $0.hasMoreToProcess }
            .map { Range(begin: $0.nextBatchBegin, end: $0.rangeEnd) }
    }

    // MARK: - Batch Processing (New API)

    /// Result type for next batch bounds
    public struct BatchBounds: Sendable {
        /// Index of the range this batch belongs to
        public let rangeIndex: Int

        /// Beginning of batch (inclusive)
        public let begin: FDB.Bytes

        /// End of batch (exclusive) - this is the range end, not batch end
        /// Actual batch end is controlled by getRange(limit:)
        public let end: FDB.Bytes

        public init(rangeIndex: Int, begin: FDB.Bytes, end: FDB.Bytes) {
            self.rangeIndex = rangeIndex
            self.begin = begin
            self.end = end
        }
    }

    /// Get bounds for the next batch to process
    ///
    /// Returns the start/end keys for the next batch. The caller should use
    /// `getRange(begin:end:limit:)` to actually limit the batch size.
    ///
    /// - Returns: Batch bounds with range index, or nil if all complete
    public func nextBatchBounds() -> BatchBounds? {
        // Find first range with remaining work
        for (index, continuation) in continuations.enumerated() {
            if continuation.hasMoreToProcess {
                return BatchBounds(
                    rangeIndex: index,
                    begin: continuation.nextBatchBegin,
                    end: continuation.rangeEnd
                )
            }
        }
        return nil
    }

    /// Record progress after processing a batch
    ///
    /// - Parameters:
    ///   - rangeIndex: Index of the range being processed
    ///   - lastProcessedKey: Last key that was successfully processed
    ///   - isComplete: True if the range is fully processed (count < limit)
    public mutating func recordProgress(
        rangeIndex: Int,
        lastProcessedKey: FDB.Bytes,
        isComplete: Bool
    ) {
        guard rangeIndex < continuations.count else { return }

        continuations[rangeIndex].lastProcessedKey = lastProcessedKey
        continuations[rangeIndex].isComplete = isComplete
    }

    /// Mark a range as complete without a specific key
    ///
    /// Used when a range is found to be empty or should be skipped.
    public mutating func markRangeComplete(rangeIndex: Int) {
        guard rangeIndex < continuations.count else { return }
        continuations[rangeIndex].isComplete = true
    }

    /// Clear all ranges (mark everything as completed)
    public mutating func clear() {
        for index in continuations.indices {
            continuations[index].isComplete = true
        }
    }

    // MARK: - Merge and Normalize

    /// Merge overlapping or adjacent ranges
    public mutating func normalize() {
        // Remove completed ranges
        continuations = continuations.filter { $0.hasMoreToProcess }

        guard continuations.count > 1 else { return }

        // Sort by current position
        continuations.sort { bytesLessThan($0.nextBatchBegin, $1.nextBatchBegin) }

        var normalized: [RangeContinuation] = []
        var current = continuations[0]

        for i in 1..<continuations.count {
            let next = continuations[i]

            // Check if ranges can be merged (overlapping or adjacent)
            if bytesGreaterThanOrEqual(current.rangeEnd, next.nextBatchBegin) {
                // Merge: extend current to include next
                var merged = RangeContinuation(
                    begin: current.nextBatchBegin,
                    end: bytesMax(current.rangeEnd, next.rangeEnd)
                )
                merged.lastProcessedKey = current.lastProcessedKey
                current = merged
            } else {
                normalized.append(current)
                current = next
            }
        }

        normalized.append(current)
        continuations = normalized
    }

    // MARK: - Progress Tracking

    /// Get progress as a percentage (0.0 to 1.0)
    ///
    /// **Note**: This is an estimate based on key bytes, not record count.
    public var progressEstimate: Double {
        guard !continuations.isEmpty else { return 1.0 }

        var totalBytes: Int = 0
        var completedBytes: Int = 0

        for continuation in continuations {
            let rangeSize = max(1, continuation.rangeEnd.count - continuation.rangeBegin.count)
            totalBytes += rangeSize

            if continuation.isComplete {
                completedBytes += rangeSize
            } else if let lastKey = continuation.lastProcessedKey {
                let processedSize = max(0, lastKey.count - continuation.rangeBegin.count)
                completedBytes += processedSize
            }
        }

        guard totalBytes > 0 else { return 1.0 }
        return Double(completedBytes) / Double(totalBytes)
    }
}

// MARK: - CustomStringConvertible

extension RangeSet: CustomStringConvertible {
    public var description: String {
        if isEmpty {
            return "RangeSet(empty)"
        }
        let remaining = count
        let progress = Int(progressEstimate * 100)
        return "RangeSet(\(remaining) ranges remaining, ~\(progress)% complete)"
    }
}

extension RangeSet.Range: CustomStringConvertible {
    public var description: String {
        let beginHex = begin.prefix(8).map { String(format: "%02x", $0) }.joined()
        let endHex = end.prefix(8).map { String(format: "%02x", $0) }.joined()
        return "Range[\(beginHex)...\(endHex)]"
    }
}

extension RangeContinuation: CustomStringConvertible {
    public var description: String {
        let beginHex = rangeBegin.prefix(4).map { String(format: "%02x", $0) }.joined()
        let endHex = rangeEnd.prefix(4).map { String(format: "%02x", $0) }.joined()
        let status = isComplete ? "complete" : (lastProcessedKey != nil ? "in-progress" : "pending")
        return "RangeContinuation[\(beginHex)...\(endHex), \(status)]"
    }
}
