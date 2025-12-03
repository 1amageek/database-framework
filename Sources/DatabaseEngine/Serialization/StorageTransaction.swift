// StorageTransaction.swift
// DatabaseEngine - Internal abstraction layer for record data storage
//
// Transparently handles compression and large value splitting.
// Always enabled - no configuration needed.

import Foundation
import FoundationDB
import Core

// MARK: - StorageTransaction

/// Internal abstraction layer that wraps TransactionProtocol with compression and splitting
///
/// **Purpose**: Intercept read/write operations for record data to apply:
/// - Compression (zlib, auto-skipped if not beneficial)
/// - Large value splitting (for values > 90KB)
///
/// **Design**: Always enabled, no configuration. Compression is skipped automatically
/// for small or incompressible data. Splitting is only applied when needed.
///
/// **Usage**:
/// ```swift
/// let storage = StorageTransaction(transaction)
///
/// // Write (automatically compresses and splits if needed)
/// try storage.write(value, for: key)
///
/// // Read (automatically joins and decompresses)
/// let value = try await storage.read(for: key)
/// ```
internal struct StorageTransaction: Sendable {
    // MARK: - Properties

    /// Underlying FDB transaction
    private let transaction: any TransactionProtocol

    /// Transformer for compression (always enabled with default settings)
    private let transformer: TransformingSerializer

    /// Splitter for large values (always enabled with default settings)
    private let splitter: LargeValueSplitter

    // MARK: - Initialization

    init(_ transaction: any TransactionProtocol) {
        self.transaction = transaction
        self.transformer = TransformingSerializer(configuration: .default)
        self.splitter = LargeValueSplitter(configuration: .default)
    }

    // MARK: - Write Operations

    /// Write a value with automatic compression and splitting
    ///
    /// Pipeline: raw data → compress → split (if needed) → FDB
    ///
    /// - Parameters:
    ///   - value: The value to write
    ///   - key: The key to write to
    func write(_ value: FDB.Bytes, for key: FDB.Bytes) throws {
        let compressed = try compress(value)
        try splitter.save(compressed, for: key, transaction: transaction)
    }

    // MARK: - Read Operations

    /// Read a value with automatic joining and decompression
    ///
    /// Pipeline: FDB → join (if split) → decompress → raw data
    ///
    /// - Parameter key: The key to read from
    /// - Returns: The decompressed value, or nil if not found
    func read(for key: FDB.Bytes) async throws -> FDB.Bytes? {
        guard let raw = try await splitter.load(for: key, transaction: transaction) else {
            return nil
        }
        return try decompress(raw)
    }

    // MARK: - Delete Operations

    /// Delete a value (handles split values automatically)
    ///
    /// - Parameter key: The key to delete
    func delete(for key: FDB.Bytes) async throws {
        try await splitter.delete(for: key, transaction: transaction)
    }

    // MARK: - Utility Operations

    /// Check if a value exists (without loading full data)
    ///
    /// - Parameter key: The key to check
    /// - Returns: True if value exists
    func exists(for key: FDB.Bytes) async throws -> Bool {
        // Check for direct value
        if try await transaction.getValue(for: key) != nil {
            return true
        }
        // Check for split value header
        return try await splitter.isSplit(for: key, transaction: transaction)
    }

    /// Get the size of a stored value (without loading full data)
    ///
    /// Returns the compressed size, not the original size.
    ///
    /// - Parameter key: The key to check
    /// - Returns: Size in bytes, or nil if not found
    func size(for key: FDB.Bytes) async throws -> Int? {
        try await splitter.getSize(for: key, transaction: transaction)
    }

    // MARK: - Direct Transaction Access

    /// Access the underlying transaction for operations that don't need transformation
    ///
    /// Use this for:
    /// - Index operations (empty values)
    /// - Metadata operations
    /// - Range scans (to get keys, not values)
    var underlying: any TransactionProtocol {
        transaction
    }

    // MARK: - Private Helpers

    private func compress(_ value: FDB.Bytes) throws -> FDB.Bytes {
        let data = Data(value)
        let compressed = try transformer.serializeSync(data)
        return Array(compressed)
    }

    private func decompress(_ value: FDB.Bytes) throws -> FDB.Bytes {
        guard !value.isEmpty else { return value }

        let data = Data(value)

        // Check header byte to determine if transformation was applied
        let flags = TransformationType(rawValue: value[0])

        // If no transformation flags set and first byte isn't a valid header,
        // treat as legacy uncompressed data
        if flags == .none && value[0] != 0x00 {
            return value
        }

        let decompressed = try transformer.deserializeSync(data)
        return Array(decompressed)
    }
}
