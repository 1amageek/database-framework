// LargeValueSplitter.swift
// DatabaseEngine - Handle large values that exceed FoundationDB's value size limit
//
// Reference: FDB Record Layer SplitHelper
// FoundationDB has a 100KB value size limit. This module transparently splits
// larger values into multiple key-value pairs and reassembles them on read.

import Foundation
import FoundationDB
import Core

// MARK: - Configuration

/// Configuration for large value splitting
public struct SplitConfiguration: Sendable, Equatable {
    /// Maximum size of a single value in bytes (default: 90KB to leave room for overhead)
    public let maxValueSize: Int

    /// Whether splitting is enabled
    public let enabled: Bool

    /// Default configuration
    public static let `default` = SplitConfiguration(
        maxValueSize: 90_000,
        enabled: true
    )

    /// Configuration with splitting disabled
    public static let disabled = SplitConfiguration(
        maxValueSize: 100_000,
        enabled: false
    )

    public init(maxValueSize: Int = 90_000, enabled: Bool = true) {
        precondition(maxValueSize > 0 && maxValueSize <= 100_000,
                     "maxValueSize must be between 1 and 100,000")
        self.maxValueSize = maxValueSize
        self.enabled = enabled
    }
}

// MARK: - LargeValueSplitter

/// Handles splitting and reassembling large values
///
/// **Key Structure**:
/// ```
/// // For unsplit values (size <= maxValueSize):
/// Key: [baseKey]
/// Value: [data]
///
/// // For split values (size > maxValueSize):
/// Key: [baseKey][0x00]          → Header: [totalSize:Int64][partCount:Int32]
/// Key: [baseKey][0x01]          → Part 1 data
/// Key: [baseKey][0x02]          → Part 2 data
/// ...
/// Key: [baseKey][0xNN]          → Part N data
/// ```
///
/// **Usage**:
/// ```swift
/// let splitter = LargeValueSplitter(configuration: .default)
///
/// // Save a large value
/// try splitter.save(data, for: baseKey, transaction: transaction)
///
/// // Load a value (handles split transparently)
/// let data = try await splitter.load(for: baseKey, transaction: transaction)
/// ```
public struct LargeValueSplitter: Sendable {
    // MARK: - Constants

    /// Suffix byte for header
    private static let headerSuffix: UInt8 = 0x00

    /// Starting suffix for data parts
    private static let firstPartSuffix: UInt8 = 0x01

    /// Maximum number of parts (255 - 1 for header)
    private static let maxParts: Int = 254

    /// Header size: totalSize (8 bytes) + partCount (4 bytes)
    private static let headerSize: Int = 12

    // MARK: - Properties

    /// Configuration
    public let configuration: SplitConfiguration

    // MARK: - Initialization

    public init(configuration: SplitConfiguration = .default) {
        self.configuration = configuration
    }

    // MARK: - Public API

    /// Save a value, splitting if necessary
    ///
    /// - Parameters:
    ///   - data: The data to save
    ///   - baseKey: The base key for storage
    ///   - transaction: The transaction to use
    /// - Throws: If the value is too large to split
    public func save(
        _ data: FDB.Bytes,
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) throws {
        // Check if splitting is needed
        if !configuration.enabled || data.count <= configuration.maxValueSize {
            // Store directly without splitting
            transaction.setValue(data, for: baseKey)
            return
        }

        // Calculate number of parts needed
        let partCount = (data.count + configuration.maxValueSize - 1) / configuration.maxValueSize

        guard partCount <= Self.maxParts else {
            throw SplitError.valueTooLarge(
                size: data.count,
                maxSize: configuration.maxValueSize * Self.maxParts
            )
        }

        // Write header
        let headerKey = baseKey + [Self.headerSuffix]
        var header = [UInt8]()
        header.reserveCapacity(Self.headerSize)
        header.append(contentsOf: ByteConversion.int64ToBytes(Int64(data.count)))
        header.append(contentsOf: Self.int32ToBytes(Int32(partCount)))
        transaction.setValue(header, for: headerKey)

        // Write data parts
        var offset = 0
        for partIndex in 0..<partCount {
            let partSuffix = Self.firstPartSuffix + UInt8(partIndex)
            let partKey = baseKey + [partSuffix]

            let partStart = offset
            let partEnd = min(offset + configuration.maxValueSize, data.count)
            let partData = Array(data[partStart..<partEnd])

            transaction.setValue(partData, for: partKey)
            offset = partEnd
        }
    }

    /// Load a value, reassembling if it was split
    ///
    /// - Parameters:
    ///   - baseKey: The base key to load from
    ///   - transaction: The transaction to use
    /// - Returns: The complete data, or nil if not found
    public func load(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws -> FDB.Bytes? {
        // First, try to load as unsplit value
        if let directValue = try await transaction.getValue(for: baseKey) {
            return directValue
        }

        // Check for split value header
        let headerKey = baseKey + [Self.headerSuffix]
        guard let headerData = try await transaction.getValue(for: headerKey) else {
            return nil
        }

        // Parse header
        guard headerData.count >= Self.headerSize else {
            throw SplitError.invalidHeader
        }

        let totalSize = Int(ByteConversion.bytesToInt64(Array(headerData[0..<8])))
        let partCount = Int(Self.bytesToInt32(Array(headerData[8..<12])))

        guard partCount > 0 && partCount <= Self.maxParts else {
            throw SplitError.invalidPartCount(partCount)
        }

        // Read all parts
        var result = [UInt8]()
        result.reserveCapacity(totalSize)

        for partIndex in 0..<partCount {
            let partSuffix = Self.firstPartSuffix + UInt8(partIndex)
            let partKey = baseKey + [partSuffix]

            guard let partData = try await transaction.getValue(for: partKey) else {
                throw SplitError.missingPart(index: partIndex)
            }

            result.append(contentsOf: partData)
        }

        guard result.count == totalSize else {
            throw SplitError.sizeMismatch(expected: totalSize, actual: result.count)
        }

        return result
    }

    /// Delete a value (handles split values)
    ///
    /// - Parameters:
    ///   - baseKey: The base key to delete
    ///   - transaction: The transaction to use
    public func delete(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws {
        // Clear the base key (for unsplit values)
        transaction.clear(key: baseKey)

        // Check for and clear split value parts
        let headerKey = baseKey + [Self.headerSuffix]
        if let headerData = try await transaction.getValue(for: headerKey),
           headerData.count >= Self.headerSize {
            let partCount = Int(Self.bytesToInt32(Array(headerData[8..<12])))

            // Clear header
            transaction.clear(key: headerKey)

            // Clear all parts
            for partIndex in 0..<min(partCount, Self.maxParts) {
                let partSuffix = Self.firstPartSuffix + UInt8(partIndex)
                let partKey = baseKey + [partSuffix]
                transaction.clear(key: partKey)
            }
        }
    }

    /// Check if a value is split
    ///
    /// - Parameters:
    ///   - baseKey: The base key to check
    ///   - transaction: The transaction to use
    /// - Returns: True if the value is split across multiple keys
    public func isSplit(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws -> Bool {
        let headerKey = baseKey + [Self.headerSuffix]
        return try await transaction.getValue(for: headerKey) != nil
    }

    /// Get the total size of a value without loading all data
    ///
    /// - Parameters:
    ///   - baseKey: The base key to check
    ///   - transaction: The transaction to use
    /// - Returns: The total size in bytes, or nil if not found
    public func getSize(
        for baseKey: FDB.Bytes,
        transaction: any TransactionProtocol
    ) async throws -> Int? {
        // Check for unsplit value
        if let directValue = try await transaction.getValue(for: baseKey) {
            return directValue.count
        }

        // Check for split value header
        let headerKey = baseKey + [Self.headerSuffix]
        guard let headerData = try await transaction.getValue(for: headerKey),
              headerData.count >= 8 else {
            return nil
        }

        return Int(ByteConversion.bytesToInt64(Array(headerData[0..<8])))
    }

    // MARK: - Private Helpers

    /// Convert Int32 to bytes (big-endian)
    private static func int32ToBytes(_ value: Int32) -> [UInt8] {
        var v = value.bigEndian
        return withUnsafeBytes(of: &v) { Array($0) }
    }

    /// Convert bytes to Int32 (big-endian)
    private static func bytesToInt32(_ bytes: [UInt8]) -> Int32 {
        guard bytes.count >= 4 else { return 0 }
        var value: Int32 = 0
        withUnsafeMutableBytes(of: &value) { dest in
            bytes.prefix(4).withUnsafeBytes { src in
                dest.copyMemory(from: src)
            }
        }
        return Int32(bigEndian: value)
    }
}

// MARK: - SplitError

/// Errors from large value splitting
public enum SplitError: Error, CustomStringConvertible, Sendable {
    case valueTooLarge(size: Int, maxSize: Int)
    case invalidHeader
    case invalidPartCount(Int)
    case missingPart(index: Int)
    case sizeMismatch(expected: Int, actual: Int)

    public var description: String {
        switch self {
        case .valueTooLarge(let size, let maxSize):
            return "Value too large: \(size) bytes exceeds maximum \(maxSize) bytes"
        case .invalidHeader:
            return "Invalid split value header"
        case .invalidPartCount(let count):
            return "Invalid part count: \(count)"
        case .missingPart(let index):
            return "Missing split value part at index \(index)"
        case .sizeMismatch(let expected, let actual):
            return "Size mismatch: expected \(expected) bytes, got \(actual) bytes"
        }
    }
}

// MARK: - SplitInfo

/// Information about a stored value's split status
public struct SplitInfo: Sendable {
    /// Total size of the value
    public let totalSize: Int

    /// Whether the value is split
    public let isSplit: Bool

    /// Number of parts (1 if not split)
    public let partCount: Int

    /// Size of each part (approximate for split values)
    public var averagePartSize: Int {
        isSplit ? (totalSize + partCount - 1) / partCount : totalSize
    }
}
