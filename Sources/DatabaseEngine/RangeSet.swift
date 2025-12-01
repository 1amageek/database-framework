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

/// A set of ranges for tracking progress in batch operations
///
/// RangeSet maintains a collection of non-overlapping key ranges that represent
/// work remaining to be done. It's used by OnlineIndexer to track which portions
/// of the keyspace have been processed, enabling resumable builds.
///
/// **Features**:
/// - Codable (can be persisted to FDB)
/// - Tracks completed ranges
/// - Provides next batch for processing
/// - Handles transaction boundaries
///
/// **Usage Example**:
/// ```swift
/// // Initialize with full range
/// let totalRange = itemSubspace.range()
/// var rangeSet = RangeSet(initialRange: totalRange)
///
/// // Process in batches
/// while !rangeSet.isEmpty {
///     let batch = rangeSet.nextBatch(size: 100)
///
///     // Process batch...
///
///     rangeSet.markCompleted(batch)
/// }
/// ```
///
/// **Persistence**:
/// ```swift
/// // Save progress
/// let data = try JSONEncoder().encode(rangeSet)
/// transaction.setValue(data, for: progressKey)
///
/// // Load progress
/// let data = try await transaction.getValue(for: progressKey)
/// let rangeSet = try JSONDecoder().decode(RangeSet.self, from: data)
/// ```
public struct RangeSet: Sendable, Codable {
    /// A single range of keys
    public struct Range: Sendable, Codable, Equatable {
        /// Beginning of range (inclusive)
        public let begin: FDB.Bytes

        /// End of range (exclusive)
        public let end: FDB.Bytes

        /// Initialize a range
        ///
        /// - Parameters:
        ///   - begin: Beginning of range (inclusive)
        ///   - end: End of range (exclusive)
        public init(begin: FDB.Bytes, end: FDB.Bytes) {
            self.begin = begin
            self.end = end
        }

        /// Check if this range contains a key
        ///
        /// - Parameter key: Key to check
        /// - Returns: true if key is in [begin, end)
        public func contains(_ key: FDB.Bytes) -> Bool {
            return bytesGreaterThanOrEqual(key, begin) && bytesLessThan(key, end)
        }

        /// Size estimate (for display purposes)
        ///
        /// - Returns: Approximate size in bytes
        public var estimatedSize: Int {
            // This is a rough estimate for display purposes only
            // Actual key count cannot be determined without scanning
            return max(0, end.count - begin.count)
        }
    }

    // MARK: - Properties

    /// Remaining ranges to process (sorted by begin key)
    private var ranges: [Range]

    // MARK: - Initialization

    /// Initialize with a single range
    ///
    /// - Parameter initialRange: Initial range tuple (begin, end)
    public init(initialRange: (begin: FDB.Bytes, end: FDB.Bytes)) {
        self.ranges = [Range(begin: initialRange.begin, end: initialRange.end)]
    }

    /// Initialize with multiple ranges
    ///
    /// - Parameter ranges: Array of ranges
    public init(ranges: [Range]) {
        self.ranges = ranges.sorted { bytesLessThan($0.begin, $1.begin) }
    }

    // MARK: - Query

    /// Check if there are no more ranges to process
    public var isEmpty: Bool {
        return ranges.isEmpty
    }

    /// Number of ranges remaining
    public var count: Int {
        return ranges.count
    }

    /// Total estimated size of all ranges
    ///
    /// **Note**: This is a rough estimate, not actual key count
    public var estimatedTotalSize: Int {
        return ranges.reduce(0) { $0 + $1.estimatedSize }
    }

    // MARK: - Batch Processing

    /// Get the next batch range to process
    ///
    /// This returns a sub-range of the first remaining range, sized to
    /// approximately contain `size` keys (based on key space estimation).
    ///
    /// **Note**: The actual number of keys in the batch may differ from `size`
    /// due to key distribution. This is an estimate based on key space.
    ///
    /// - Parameter size: Desired batch size (number of keys estimate)
    /// - Returns: Range for next batch, or nil if empty
    public func nextBatch(size: Int) -> Range? {
        guard let firstRange = ranges.first else {
            return nil
        }

        // For simplicity, return the entire first range
        // A more sophisticated implementation could split ranges
        // based on actual key scanning or size estimates
        return firstRange
    }

    /// Mark a range as completed
    ///
    /// This removes the completed range from the set. If the completed range
    /// is a sub-range of a larger range, the remaining portions are preserved.
    ///
    /// - Parameter completedRange: Range that has been processed
    public mutating func markCompleted(_ completedRange: Range) {
        var newRanges: [Range] = []

        for range in ranges {
            // If completed range fully contains this range, skip it
            if bytesLessThanOrEqual(completedRange.begin, range.begin) && bytesGreaterThanOrEqual(completedRange.end, range.end) {
                continue
            }

            // If this range fully contains completed range, split it
            if bytesLessThan(range.begin, completedRange.begin) && bytesGreaterThan(range.end, completedRange.end) {
                // Add portion before completed range
                newRanges.append(Range(begin: range.begin, end: completedRange.begin))
                // Add portion after completed range
                newRanges.append(Range(begin: completedRange.end, end: range.end))
                continue
            }

            // If completed range overlaps beginning of this range
            if bytesLessThanOrEqual(completedRange.begin, range.begin) && bytesGreaterThan(completedRange.end, range.begin) && bytesLessThan(completedRange.end, range.end) {
                newRanges.append(Range(begin: completedRange.end, end: range.end))
                continue
            }

            // If completed range overlaps end of this range
            if bytesGreaterThan(completedRange.begin, range.begin) && bytesLessThan(completedRange.begin, range.end) && bytesGreaterThanOrEqual(completedRange.end, range.end) {
                newRanges.append(Range(begin: range.begin, end: completedRange.begin))
                continue
            }

            // No overlap, keep original range
            newRanges.append(range)
        }

        self.ranges = newRanges.sorted { bytesLessThan($0.begin, $1.begin) }
    }

    /// Clear all ranges (mark everything as completed)
    public mutating func clear() {
        self.ranges = []
    }

    // MARK: - Merge and Normalize

    /// Merge overlapping or adjacent ranges
    ///
    /// This optimizes the internal representation by combining ranges
    /// that can be represented as a single range.
    public mutating func normalize() {
        guard ranges.count > 1 else { return }

        var normalized: [Range] = []
        var current = ranges[0]

        for i in 1..<ranges.count {
            let next = ranges[i]

            // Check if ranges can be merged (overlapping or adjacent)
            if bytesGreaterThanOrEqual(current.end, next.begin) {
                // Merge ranges
                current = Range(begin: current.begin, end: bytesMax(current.end, next.end))
            } else {
                // Add current and start new range
                normalized.append(current)
                current = next
            }
        }

        normalized.append(current)
        self.ranges = normalized
    }
}

// MARK: - CustomStringConvertible

extension RangeSet: CustomStringConvertible {
    public var description: String {
        if isEmpty {
            return "RangeSet(empty)"
        }
        return "RangeSet(\(ranges.count) ranges, ~\(estimatedTotalSize) bytes)"
    }
}

extension RangeSet.Range: CustomStringConvertible {
    public var description: String {
        return "Range[\(begin.prefix(8).map { String(format: "%02x", $0) }.joined())...\(end.prefix(8).map { String(format: "%02x", $0) }.joined())]"
    }
}
