// SpanValue.swift
// Span Counter encoding for Skip List
//
// The Span Counter is the key mechanism that enables O(log n) rank lookup.
// Each link in the skip list stores the count of elements it skips.
//
// References:
// - FoundationDB Record Layer RankedSet
// - Redis Skip List implementation

import Foundation
import FoundationDB
import Core

/// Span value stored at each skip list node
///
/// A span represents the number of elements between the current node
/// and the next node at a given level.
///
/// Example:
/// ```
/// [Node A] ───span=5───→ [Node F]
///        ↓
///   A, B, C, D, E, F = skips 5 elements
/// ```
public struct SpanValue: Codable, Sendable, Equatable {

    // MARK: - Properties

    /// Number of elements this link skips
    ///
    /// For Level 0 (leaf), count is always 1.
    /// For higher levels, count can be any positive integer.
    public let count: Int64

    // MARK: - Initialization

    /// Initialize with element count
    ///
    /// - Parameter count: Number of elements this link skips
    public init(count: Int64) {
        self.count = count
    }

    // MARK: - Encoding

    /// Encode span value to bytes using Tuple encoding
    ///
    /// - Returns: Tuple-encoded bytes
    public func encoded() -> [UInt8] {
        Tuple(count).pack()
    }

    /// Decode span value from bytes
    ///
    /// - Parameter bytes: Tuple-encoded bytes
    /// - Returns: Decoded SpanValue
    /// - Throws: `IndexError.invalidStructure` if decoding fails
    public static func decode(_ bytes: [UInt8]) throws -> SpanValue {
        let tuple = try Tuple.unpack(from: bytes)

        guard tuple.count >= 1 else {
            throw IndexError.invalidStructure(
                "Failed to decode SpanValue: tuple is empty, bytes: \(bytes)"
            )
        }

        // Try Int64 first, then Int (for compatibility)
        if let count = tuple[0] as? Int64 {
            return SpanValue(count: count)
        } else if let count = tuple[0] as? Int {
            return SpanValue(count: Int64(count))
        } else {
            throw IndexError.invalidStructure(
                "Failed to decode SpanValue: expected Int64 or Int, got \(type(of: tuple[0])), tuple: \(tuple), bytes: \(bytes)"
            )
        }
    }
}

// MARK: - Constants

extension SpanValue {
    /// Span value for leaf level (always 1)
    public static let leaf = SpanValue(count: 1)
}
