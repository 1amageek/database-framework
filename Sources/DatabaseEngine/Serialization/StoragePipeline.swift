// StoragePipeline.swift
// DatabaseEngine - Pipeline for data transformation between application and storage
//
// Combines serialization, compression, and value splitting into a unified pipeline.
// Each stage is independent and can be enabled/disabled via configuration.

import Foundation
import FoundationDB
import Core

// MARK: - StorageConfiguration

/// Configuration for the storage pipeline
public struct StorageConfiguration: Sendable, Equatable {
    /// Whether compression is enabled
    public let compressionEnabled: Bool

    /// Compression configuration (used when compressionEnabled is true)
    public let compressionConfig: TransformConfiguration

    /// Whether large value splitting is enabled
    public let splitEnabled: Bool

    /// Split configuration (used when splitEnabled is true)
    public let splitConfig: SplitConfiguration

    /// Default configuration (compression enabled, splitting enabled)
    public static let `default` = StorageConfiguration(
        compressionEnabled: true,
        compressionConfig: .default,
        splitEnabled: true,
        splitConfig: .default
    )

    /// No transformation (raw Protobuf only)
    public static let none = StorageConfiguration(
        compressionEnabled: false,
        compressionConfig: .none,
        splitEnabled: false,
        splitConfig: .disabled
    )

    /// Compression only (no splitting)
    public static let compressionOnly = StorageConfiguration(
        compressionEnabled: true,
        compressionConfig: .default,
        splitEnabled: false,
        splitConfig: .disabled
    )

    /// Splitting only (no compression)
    public static let splitOnly = StorageConfiguration(
        compressionEnabled: false,
        compressionConfig: .none,
        splitEnabled: true,
        splitConfig: .default
    )

    public init(
        compressionEnabled: Bool = true,
        compressionConfig: TransformConfiguration = .default,
        splitEnabled: Bool = true,
        splitConfig: SplitConfiguration = .default
    ) {
        self.compressionEnabled = compressionEnabled
        self.compressionConfig = compressionConfig
        self.splitEnabled = splitEnabled
        self.splitConfig = splitConfig
    }
}

// MARK: - StoragePipeline

/// Pipeline for transforming data between application and storage layers
///
/// **Pipeline Stages**:
/// ```
/// Save: Item → Protobuf → Compress → Split → FDB
/// Load: FDB → Join → Decompress → Protobuf → Item
/// ```
///
/// **Usage**:
/// ```swift
/// let pipeline = StoragePipeline(configuration: .default)
///
/// // Save
/// try pipeline.save(item, for: baseKey, transaction: transaction)
///
/// // Load
/// let item: User = try await pipeline.load(for: baseKey, transaction: transaction)
/// ```
public struct StoragePipeline: Sendable {
    // MARK: - Properties

    /// Pipeline configuration
    public let configuration: StorageConfiguration

    /// Transformer for compression/encryption
    private let transformer: TransformingSerializer

    /// Splitter for large values
    private let splitter: LargeValueSplitter

    // MARK: - Initialization

    public init(configuration: StorageConfiguration = .default) {
        self.configuration = configuration
        self.transformer = TransformingSerializer(configuration: configuration.compressionConfig)
        self.splitter = LargeValueSplitter(configuration: configuration.splitConfig)
    }

    // MARK: - Save Pipeline

    /// Save an item through the pipeline
    ///
    /// Pipeline: Item → Protobuf → Compress → Split → FDB
    ///
    /// - Parameters:
    ///   - item: The item to save
    ///   - baseKey: The base key for storage
    ///   - transaction: The transaction to use
    /// - Throws: Error if any pipeline stage fails
    public func save<Item: Persistable>(
        _ item: Item,
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) throws {
        // Stage 1: Protobuf serialization
        let serialized = try DataAccess.serialize(item)

        // Stage 2: Compression (if enabled)
        let compressed: FDB.Bytes
        if configuration.compressionEnabled {
            let data = Data(serialized)
            let transformedData = try transformer.serializeSync(data)
            compressed = Array(transformedData)
        } else {
            compressed = serialized
        }

        // Stage 3: Split (if enabled and needed)
        if configuration.splitEnabled {
            try splitter.save(compressed, for: baseKey, transaction: transaction)
        } else {
            transaction.setValue(compressed, for: baseKey)
        }
    }

    /// Save raw bytes through the pipeline (skip Protobuf stage)
    ///
    /// - Parameters:
    ///   - bytes: The bytes to save
    ///   - baseKey: The base key for storage
    ///   - transaction: The transaction to use
    public func saveBytes(
        _ bytes: FDB.Bytes,
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) throws {
        // Stage 1: Compression (if enabled)
        let compressed: FDB.Bytes
        if configuration.compressionEnabled {
            let data = Data(bytes)
            let transformedData = try transformer.serializeSync(data)
            compressed = Array(transformedData)
        } else {
            compressed = bytes
        }

        // Stage 2: Split (if enabled and needed)
        if configuration.splitEnabled {
            try splitter.save(compressed, for: baseKey, transaction: transaction)
        } else {
            transaction.setValue(compressed, for: baseKey)
        }
    }

    // MARK: - Load Pipeline

    /// Load an item through the pipeline
    ///
    /// Pipeline: FDB → Join → Decompress → Protobuf → Item
    ///
    /// - Parameters:
    ///   - baseKey: The base key to load from
    ///   - transaction: The transaction to use
    /// - Returns: The loaded item, or nil if not found
    /// - Throws: Error if any pipeline stage fails
    public func load<Item: Persistable>(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws -> Item? {
        // Stage 1: Load (with join if split)
        let rawBytes: FDB.Bytes?
        if configuration.splitEnabled {
            rawBytes = try await splitter.load(for: baseKey, transaction: transaction)
        } else {
            rawBytes = try await transaction.getValue(for: baseKey)
        }

        guard let bytes = rawBytes else {
            return nil
        }

        // Stage 2: Decompress (if needed)
        let decompressed: FDB.Bytes
        if configuration.compressionEnabled && !bytes.isEmpty {
            let data = Data(bytes)
            // Check if data has transformation header
            if TransformationType(rawValue: bytes[0]) != .none {
                let decompressedData = try transformer.deserializeSync(data)
                decompressed = Array(decompressedData)
            } else {
                // No transformation applied, use as-is
                decompressed = bytes
            }
        } else {
            decompressed = bytes
        }

        // Stage 3: Protobuf deserialization
        return try DataAccess.deserialize(decompressed)
    }

    /// Load raw bytes through the pipeline (skip Protobuf stage)
    ///
    /// - Parameters:
    ///   - baseKey: The base key to load from
    ///   - transaction: The transaction to use
    /// - Returns: The loaded bytes, or nil if not found
    public func loadBytes(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws -> FDB.Bytes? {
        // Stage 1: Load (with join if split)
        let rawBytes: FDB.Bytes?
        if configuration.splitEnabled {
            rawBytes = try await splitter.load(for: baseKey, transaction: transaction)
        } else {
            rawBytes = try await transaction.getValue(for: baseKey)
        }

        guard let bytes = rawBytes else {
            return nil
        }

        // Stage 2: Decompress (if needed)
        if configuration.compressionEnabled && !bytes.isEmpty {
            let data = Data(bytes)
            if TransformationType(rawValue: bytes[0]) != .none {
                let decompressedData = try transformer.deserializeSync(data)
                return Array(decompressedData)
            }
        }

        return bytes
    }

    // MARK: - Delete Pipeline

    /// Delete a value (handles split values)
    ///
    /// - Parameters:
    ///   - baseKey: The base key to delete
    ///   - transaction: The transaction to use
    public func delete(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws {
        if configuration.splitEnabled {
            try await splitter.delete(for: baseKey, transaction: transaction)
        } else {
            transaction.clear(key: baseKey)
        }
    }

    // MARK: - Utilities

    /// Check if a value is split
    public func isSplit(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws -> Bool {
        guard configuration.splitEnabled else { return false }
        return try await splitter.isSplit(for: baseKey, transaction: transaction)
    }

    /// Get the stored size of a value (without loading all data)
    public func getStoredSize(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws -> Int? {
        if configuration.splitEnabled {
            return try await splitter.getSize(for: baseKey, transaction: transaction)
        } else {
            let bytes = try await transaction.getValue(for: baseKey)
            return bytes?.count
        }
    }
}

// MARK: - StoragePipelineStatistics

/// Statistics about pipeline operations
public struct StoragePipelineStatistics: Sendable {
    /// Number of items saved
    public var itemsSaved: Int = 0

    /// Number of items loaded
    public var itemsLoaded: Int = 0

    /// Number of items that were compressed
    public var itemsCompressed: Int = 0

    /// Number of items that were split
    public var itemsSplit: Int = 0

    /// Total bytes before compression
    public var totalBytesBeforeCompression: Int = 0

    /// Total bytes after compression
    public var totalBytesAfterCompression: Int = 0

    /// Compression ratio (0.0 - 1.0, lower is better)
    public var compressionRatio: Double {
        guard totalBytesBeforeCompression > 0 else { return 1.0 }
        return Double(totalBytesAfterCompression) / Double(totalBytesBeforeCompression)
    }

    /// Space saved by compression
    public var spaceSaved: Int {
        max(0, totalBytesBeforeCompression - totalBytesAfterCompression)
    }
}
