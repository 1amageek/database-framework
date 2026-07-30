// ByteConversion.swift
// DatabaseEngine - Byte conversion utilities for FDB atomic operations
//
// Provides type-safe conversion between numeric types and byte arrays
// for use with FoundationDB atomic operations.

import DatabaseTypes
import StorageKit

/// Byte conversion utilities for FDB atomic operations
///
/// **Purpose**:
/// FoundationDB atomic operations (like `.add`) require values as little-endian byte arrays.
/// This utility provides consistent, optimized conversion functions.
///
/// **Usage**:
/// ```swift
/// // For COUNT indexes (Int64)
/// let incrementBytes = ByteConversion.int64ToBytes(1)
/// try transaction.atomicOp(key: countKey, param: incrementBytes, mutationType: .add)
///
/// // For SUM indexes (fixed-point Double)
/// let sumBytes = ByteConversion.doubleToScaledBytes(123.456)
/// try transaction.atomicOp(key: sumKey, param: sumBytes, mutationType: .add)
/// ```
public enum ByteConversion {
    /// Scale factor for fixed-point Double representation
    ///
    /// 6 decimal places provides precision for most financial calculations.
    /// Range: ±9,223,372,036,854.775807 (Int64.max / scaleFactor)
    public static let scaleFactor: Double = 1_000_000.0

    // MARK: - Int64 Conversion

    /// Convert Int64 to little-endian bytes for FDB atomic operations
    ///
    /// - Parameter value: The Int64 value to convert
    /// - Returns: 8-byte array in little-endian format
    @inlinable
    public static func int64ToBytes(_ value: Int64) -> ByteString {
        var littleEndian = value.littleEndian
        return ByteString.copying(count: MemoryLayout<Int64>.size) { destination in
            withUnsafeBytes(of: &littleEndian) { source in
                destination.copyMemory(from: source)
            }
        }
    }

    /// Convert little-endian bytes to Int64
    ///
    /// - Parameter bytes: 8-byte array in little-endian format
    /// - Returns: Decoded Int64 value.
    /// - Throws: `ByteConversionError.invalidByteCount` for malformed storage.
    @inlinable
    public static func bytesToInt64(
        _ bytes: ByteString
    ) throws(ByteConversionError) -> Int64 {
        guard bytes.count == MemoryLayout<Int64>.size else {
            throw .invalidByteCount(
                expected: MemoryLayout<Int64>.size,
                actual: bytes.count
            )
        }
        return bytes.withUnsafeBytes {
            Int64(littleEndian: $0.loadUnaligned(as: Int64.self))
        }
    }

    // MARK: - Scaled Double Conversion (Fixed-Point)

    /// Convert Double to scaled Int64 bytes for atomic SUM operations
    ///
    /// Uses fixed-point representation with 6 decimal places.
    /// This allows atomic add operations on Double values.
    ///
    /// - Parameter value: The Double value to convert
    /// - Returns: 8-byte array representing scaled Int64
    @inlinable
    public static func doubleToScaledBytes(
        _ value: Double
    ) throws(ByteConversionError) -> ByteString {
        guard value.isFinite else { throw .nonFiniteDouble }
        let scaled = value * scaleFactor
        guard scaled >= Double(Int64.min), scaled < -Double(Int64.min) else {
            throw .scaledDoubleOutOfRange
        }
        return int64ToBytes(Int64(scaled))
    }

    /// Convert scaled Int64 bytes back to Double
    ///
    /// - Parameter bytes: 8-byte array from FDB
    /// - Returns: Decoded Double value.
    @inlinable
    public static func scaledBytesToDouble(
        _ bytes: ByteString
    ) throws(ByteConversionError) -> Double {
        Double(try bytesToInt64(bytes)) / scaleFactor
    }

    // MARK: - UInt64 Conversion

    /// Convert UInt64 to little-endian bytes
    ///
    /// - Parameter value: The UInt64 value to convert
    /// - Returns: 8-byte array in little-endian format
    @inlinable
    public static func uint64ToBytes(_ value: UInt64) -> ByteString {
        var littleEndian = value.littleEndian
        return ByteString.copying(count: MemoryLayout<UInt64>.size) { destination in
            withUnsafeBytes(of: &littleEndian) { source in
                destination.copyMemory(from: source)
            }
        }
    }

    /// Convert little-endian bytes to UInt64
    ///
    /// - Parameter bytes: 8-byte array in little-endian format
    /// - Returns: Decoded UInt64 value.
    /// - Throws: `ByteConversionError.invalidByteCount` for malformed storage.
    @inlinable
    public static func bytesToUInt64(
        _ bytes: ByteString
    ) throws(ByteConversionError) -> UInt64 {
        guard bytes.count == MemoryLayout<UInt64>.size else {
            throw .invalidByteCount(
                expected: MemoryLayout<UInt64>.size,
                actual: bytes.count
            )
        }
        return bytes.withUnsafeBytes {
            UInt64(littleEndian: $0.loadUnaligned(as: UInt64.self))
        }
    }
}
