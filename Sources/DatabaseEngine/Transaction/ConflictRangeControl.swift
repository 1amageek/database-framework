// ConflictRangeControl.swift
// DatabaseEngine - Utilities for fine-grained conflict range control
//
// FDB's optimistic concurrency control can be tuned by manually controlling
// which key ranges cause conflicts. This is useful for high-contention scenarios.

import Foundation
import FoundationDB

// MARK: - Conflict Range Types

/// Strategies for conflict range control
public enum ConflictRangeStrategy: Sendable {
    /// Use FDB's default conflict detection (automatic)
    case automatic

    /// Manually control read conflicts
    /// Reads with snapshot=true don't add read conflicts, allowing manual control
    case manualReadConflicts

    /// Optimistic write pattern: read without conflict, add write conflict only
    case optimisticWrite
}

// MARK: - TransactionProtocol Extension

extension TransactionProtocol {
    /// Read a value without adding a read conflict
    ///
    /// Use this when you want to read data but not conflict with concurrent writes.
    /// The read may return stale data if another transaction writes to this key
    /// between your read and commit.
    ///
    /// **Use Case**: Reading metadata that rarely changes, or when you'll
    /// verify consistency through other means.
    ///
    /// - Parameters:
    ///   - key: The key to read
    /// - Returns: The value if it exists
    public func getValueNoConflict(for key: FDB.Bytes) async throws -> FDB.Bytes? {
        // snapshot=true reads don't add read conflicts
        return try await getValue(for: key, snapshot: true)
    }

    /// Perform an optimistic write: read current value, then write with conflict control
    ///
    /// This pattern reads the current value without adding a read conflict,
    /// then adds only a write conflict. This reduces conflicts in scenarios where:
    /// - Multiple transactions read the same key but write different keys
    /// - You want to detect concurrent writes but not concurrent reads
    ///
    /// **Example**:
    /// ```swift
    /// // Counter increment without read conflict
    /// let current = try await transaction.optimisticRead(key: counterKey)
    /// let newValue = (current ?? 0) + 1
    /// try transaction.optimisticWrite(key: counterKey, value: newValue)
    /// ```
    ///
    /// - Parameters:
    ///   - key: The key to read
    /// - Returns: The current value
    public func optimisticRead(key: FDB.Bytes) async throws -> FDB.Bytes? {
        return try await getValue(for: key, snapshot: true)
    }

    /// Write a value with explicit write conflict
    ///
    /// Adds a write conflict range for the key and sets the value.
    /// Use after `optimisticRead` for the optimistic write pattern.
    ///
    /// - Parameters:
    ///   - key: The key to write
    ///   - value: The value to write
    public func optimisticWrite(key: FDB.Bytes, value: FDB.Bytes) throws {
        // Add write conflict for this key
        try addConflictRange(beginKey: key, endKey: key + [0x00], type: .write)
        setValue(value, for: key)
    }

    /// Add a read conflict range without reading the data
    ///
    /// Use this when you want to conflict with writes to a range even
    /// if you don't read all the data in that range.
    ///
    /// - Parameters:
    ///   - begin: Begin key (inclusive)
    ///   - end: End key (exclusive)
    public func addReadConflict(begin: FDB.Bytes, end: FDB.Bytes) throws {
        try addConflictRange(beginKey: begin, endKey: end, type: .read)
    }

    /// Add a write conflict range without writing data
    ///
    /// Use this when you want to conflict with concurrent transactions
    /// that read from this range, even if you don't write to all keys.
    ///
    /// - Parameters:
    ///   - begin: Begin key (inclusive)
    ///   - end: End key (exclusive)
    public func addWriteConflict(begin: FDB.Bytes, end: FDB.Bytes) throws {
        try addConflictRange(beginKey: begin, endKey: end, type: .write)
    }

    /// Perform a compare-and-set operation with optimistic concurrency
    ///
    /// Reads the current value without read conflict, checks if it matches
    /// expected value, and if so, writes the new value with write conflict.
    ///
    /// - Parameters:
    ///   - key: The key to update
    ///   - expected: Expected current value (nil for key not existing)
    ///   - newValue: New value to write
    /// - Returns: true if update succeeded, false if current value didn't match
    public func compareAndSet(
        key: FDB.Bytes,
        expected: FDB.Bytes?,
        newValue: FDB.Bytes
    ) async throws -> Bool {
        let current = try await getValue(for: key, snapshot: true)

        // Check if current matches expected
        let matches: Bool
        switch (current, expected) {
        case (nil, nil):
            matches = true
        case (let c?, let e?):
            matches = c == e
        default:
            matches = false
        }

        if matches {
            // Add write conflict and set value
            try addConflictRange(beginKey: key, endKey: key + [0x00], type: .write)
            setValue(newValue, for: key)
            return true
        }

        return false
    }
}

// MARK: - Subspace Conflict Helpers

extension Subspace {
    /// Get conflict range for entire subspace
    ///
    /// - Returns: Tuple of (begin, end) keys for conflict range
    public func conflictRange() -> (begin: FDB.Bytes, end: FDB.Bytes) {
        return range()
    }
}

// MARK: - High-Contention Atomic Operations

extension TransactionProtocol {
    /// Increment a counter atomically without read conflict
    ///
    /// Uses FDB's atomic ADD operation which doesn't require reading
    /// the current value and thus avoids read conflicts entirely.
    ///
    /// - Parameters:
    ///   - key: The counter key
    ///   - delta: Amount to add (can be negative for decrement)
    public func atomicIncrement(key: FDB.Bytes, delta: Int64 = 1) {
        let deltaBytes = withUnsafeBytes(of: delta.littleEndian) { Array($0) }
        atomicOp(key: key, param: deltaBytes, mutationType: .add)
    }

    /// Set maximum value atomically
    ///
    /// Compares current value with new value and keeps the larger one.
    /// Useful for tracking high-water marks without conflicts.
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - value: The value to compare (as little-endian Int64)
    public func atomicMax(key: FDB.Bytes, value: Int64) {
        let valueBytes = withUnsafeBytes(of: value.littleEndian) { Array($0) }
        atomicOp(key: key, param: valueBytes, mutationType: .max)
    }

    /// Set minimum value atomically
    ///
    /// Compares current value with new value and keeps the smaller one.
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - value: The value to compare (as little-endian Int64)
    public func atomicMin(key: FDB.Bytes, value: Int64) {
        let valueBytes = withUnsafeBytes(of: value.littleEndian) { Array($0) }
        atomicOp(key: key, param: valueBytes, mutationType: .min)
    }

    /// Perform bitwise OR atomically
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - mask: Bit mask to OR with current value
    public func atomicBitOr(key: FDB.Bytes, mask: FDB.Bytes) {
        atomicOp(key: key, param: mask, mutationType: .bitOr)
    }

    /// Perform bitwise AND atomically
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - mask: Bit mask to AND with current value
    public func atomicBitAnd(key: FDB.Bytes, mask: FDB.Bytes) {
        atomicOp(key: key, param: mask, mutationType: .bitAnd)
    }

    /// Perform bitwise XOR atomically
    ///
    /// - Parameters:
    ///   - key: The key
    ///   - mask: Bit mask to XOR with current value
    public func atomicBitXor(key: FDB.Bytes, mask: FDB.Bytes) {
        atomicOp(key: key, param: mask, mutationType: .bitXor)
    }
}
