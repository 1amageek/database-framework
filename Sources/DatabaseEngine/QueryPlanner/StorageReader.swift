// StorageReader.swift
// QueryPlanner - Low-level storage access abstraction

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import StorageKit
import Core

/// Protocol for low-level storage access during query execution
///
/// StorageReader provides basic key-value operations for raw storage access.
/// It is storage-agnostic and knows nothing about index structures.
///
/// **Design Principle**:
/// - StorageReader provides raw KV operations only
/// - Subspace resolution is done via Persistable type + DirectoryLayer
/// - IndexSearcher receives pre-resolved Subspace, not raw StorageReader
///
/// **Note**: Index subspace is NOT exposed here. Use `IndexQueryContext.indexSubspace(for:)`
/// which resolves subspace via DirectoryLayer based on Persistable type.
public protocol StorageReader: Sendable {
    // MARK: - Raw Key-Value Access

    /// Scan a range within a subspace
    ///
    /// - Parameters:
    ///   - subspace: The subspace to scan
    ///   - start: Start key (relative to subspace), nil for beginning
    ///   - end: End key (relative to subspace), nil for end
    ///   - startInclusive: Whether start is inclusive (default: true)
    ///   - endInclusive: Whether end is inclusive (default: false)
    ///   - reverse: Whether to scan in reverse order
    /// - Returns: Stream of (key, value) pairs
    func scanRange(
        subspace: Subspace,
        start: Tuple?,
        end: Tuple?,
        startInclusive: Bool,
        endInclusive: Bool,
        reverse: Bool
    ) -> AsyncThrowingStream<(key: Bytes, value: Bytes), Error>

    /// Get a single value by key
    ///
    /// - Parameter key: The full key
    /// - Returns: The value if found, nil otherwise
    func getValue(key: Bytes) async throws -> Bytes?
}

// MARK: - Default Implementations

extension StorageReader {
    /// Convenience method to scan entire subspace
    public func scanSubspace(
        _ subspace: Subspace,
        reverse: Bool = false
    ) -> AsyncThrowingStream<(key: Bytes, value: Bytes), Error> {
        scanRange(
            subspace: subspace,
            start: nil,
            end: nil,
            startInclusive: true,
            endInclusive: false,
            reverse: reverse
        )
    }
}

// MARK: - Index Entry

/// Represents an entry returned from an index search
///
/// This design follows the FDB Record Layer approach where:
/// - Index key contains: [indexedValues...][primaryKey...]
/// - Index value contains: canonical DBIX projection bytes
///
/// **Structure**:
/// - `itemID`: Primary key of the referenced item
/// - `keyValues`: Values extracted from the index key (indexed fields)
/// - `coveringValue`: Canonical projection bytes from the index value
/// - `score`: Optional relevance/distance score
///
/// **Usage**:
/// ```swift
/// // For covering indexes, coveringValue contains the complete typed projection.
/// // For non-covering indexes, coveringValue is empty and entity fetch is needed.
/// ```
public struct IndexEntry: Sendable {
    /// The item ID as a Tuple (supports composite keys)
    public let itemID: Tuple

    /// Values extracted from the index key (in indexed field order)
    /// These are the values from the index key portion before the primary key
    public let keyValues: Tuple

    /// Canonical DBIX bytes stored in the index value.
    public let coveringValue: Bytes

    /// Optional score (relevance, distance, etc.)
    public let score: Double?

    public init(
        itemID: Tuple,
        keyValues: Tuple = Tuple(),
        coveringValue: Bytes = [],
        score: Double? = nil
    ) {
        self.itemID = itemID
        self.keyValues = keyValues
        self.coveringValue = coveringValue
        self.score = score
    }

    /// Convenience initializer for single-value ID
    public init(
        itemID: any TupleElement,
        keyValues: Tuple = Tuple(),
        coveringValue: Bytes = [],
        score: Double? = nil
    ) {
        self.itemID = Tuple([itemID])
        self.keyValues = keyValues
        self.coveringValue = coveringValue
        self.score = score
    }
}
