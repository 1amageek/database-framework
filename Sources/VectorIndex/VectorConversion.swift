// VectorConversion.swift
// VectorIndex - Unified vector type conversion utilities
//
// This file provides a single entry point for all vector-related
// type conversions between Swift types and TupleElement.
//
// Reference: Consolidates duplicate conversion logic from
// FlatVectorIndexMaintainer, HNSWIndexMaintainer, IVFIndexMaintainer, PQIndexMaintainer

import Foundation
import FoundationDB
import DatabaseEngine

/// Unified vector conversion utilities for VectorIndex module
///
/// **MANDATORY**: All VectorIndex maintainers MUST use this utility.
/// Custom conversion implementations are PROHIBITED.
///
/// ## Type Mapping
/// | Input Type | Output (to Float) |
/// |------------|------------------|
/// | Float | Direct |
/// | Double | Float(d) |
/// | Int64 | Float(i64) |
/// | Int | Float(i) |
/// | [Float] | Append all |
/// | [Double] | Map to Float |
public struct VectorConversion: Sendable {

    private init() {}

    // MARK: - TupleElement to Vector

    /// Convert TupleElement array to Float vector
    ///
    /// - Parameter elements: Array of TupleElements
    /// - Returns: Float vector
    public static func tupleToVector(_ elements: [any TupleElement]) -> [Float] {
        var vector: [Float] = []
        vector.reserveCapacity(elements.count)
        for element in elements {
            if let f = TypeConversion.asFloat(element) {
                vector.append(f)
            }
            // Skip unsupported types silently
        }
        return vector
    }

    /// Convert Tuple to Float vector
    ///
    /// - Parameter tuple: Tuple containing vector elements
    /// - Returns: Float vector
    public static func tupleToVector(_ tuple: Tuple) -> [Float] {
        var elements: [any TupleElement] = []
        for i in 0..<tuple.count {
            if let element = tuple[i] {
                elements.append(element)
            }
        }
        return tupleToVector(elements)
    }

    // MARK: - Vector to Tuple

    /// Convert Float vector to Tuple
    ///
    /// - Parameter vector: Float vector
    /// - Returns: Tuple containing vector elements
    public static func vectorToTuple(_ vector: [Float]) -> Tuple {
        let elements: [any TupleElement] = vector.map { $0 as any TupleElement }
        return Tuple(elements)
    }

    // MARK: - Field Value Extraction

    /// Extract Float vector from field values
    ///
    /// Handles arrays and individual numeric values.
    ///
    /// - Parameter fieldValues: Array of field values from DataAccess
    /// - Returns: Float vector
    /// - Throws: VectorIndexError if values are not numeric
    public static func extractFloatArray(from fieldValues: [any TupleElement]) throws -> [Float] {
        var floatArray: [Float] = []
        for element in fieldValues {
            if let array = element as? [Float] {
                floatArray.append(contentsOf: array)
            } else if let array = element as? [Float32] {
                floatArray.append(contentsOf: array.map { Float($0) })
            } else if let array = element as? [Double] {
                floatArray.append(contentsOf: array.map { Float($0) })
            } else if let f = element as? Float {
                floatArray.append(f)
            } else if let d = element as? Double {
                floatArray.append(Float(d))
            } else {
                throw VectorIndexError.invalidArgument(
                    "Vector field must contain numeric values, got: \(type(of: element))"
                )
            }
        }
        return floatArray
    }

    // MARK: - Byte Conversion

    /// Convert Float array to bytes (little-endian)
    ///
    /// - Parameter floats: Float array
    /// - Returns: Byte array
    public static func floatArrayToBytes(_ floats: [Float]) -> [UInt8] {
        var bytes: [UInt8] = []
        bytes.reserveCapacity(floats.count * 4)
        for f in floats {
            var value = f.bitPattern.littleEndian
            withUnsafeBytes(of: &value) { bytes.append(contentsOf: $0) }
        }
        return bytes
    }

    /// Convert bytes to Float array (little-endian)
    ///
    /// - Parameter bytes: Byte array
    /// - Returns: Float array
    public static func bytesToFloatArray(_ bytes: [UInt8]) -> [Float] {
        var floats: [Float] = []
        floats.reserveCapacity(bytes.count / 4)
        for i in stride(from: 0, to: bytes.count - 3, by: 4) {
            let bits = bytes[i..<i+4].withUnsafeBytes {
                UInt32(littleEndian: $0.load(as: UInt32.self))
            }
            floats.append(Float(bitPattern: bits))
        }
        return floats
    }

    /// Convert UInt64 to bytes (little-endian)
    public static func uint64ToBytes(_ value: UInt64) -> [UInt8] {
        withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }

    /// Convert bytes to UInt64 (little-endian)
    public static func bytesToUInt64(_ bytes: [UInt8]) -> UInt64 {
        guard bytes.count == 8 else { return 0 }
        return bytes.withUnsafeBytes {
            UInt64(littleEndian: $0.load(as: UInt64.self))
        }
    }

    /// Convert Int64 to bytes (little-endian)
    public static func int64ToBytes(_ value: Int64) -> [UInt8] {
        withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }

    /// Convert bytes to Int64 (little-endian)
    public static func bytesToInt64(_ bytes: [UInt8]) -> Int64 {
        guard bytes.count >= 8 else { return 0 }
        return bytes.withUnsafeBytes {
            Int64(littleEndian: $0.load(as: Int64.self))
        }
    }
}

// MARK: - Distance Calculations

extension VectorConversion {

    /// Calculate cosine distance between two vectors
    ///
    /// - Returns: Distance in range [0, 2] (0 = identical, 2 = opposite)
    public static func cosineDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        let norm1 = sqrt(v1.map { Double($0) * Double($0) }.reduce(0, +))
        let norm2 = sqrt(v2.map { Double($0) * Double($0) }.reduce(0, +))

        guard norm1 > 0 && norm2 > 0 else { return 2.0 }
        let cosineSimilarity = dotProduct / (norm1 * norm2)
        return 1.0 - cosineSimilarity
    }

    /// Calculate Euclidean distance between two vectors
    public static func euclideanDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let sumSquares = zip(v1, v2).map { pow(Double($0) - Double($1), 2) }.reduce(0, +)
        return sqrt(sumSquares)
    }

    /// Calculate Euclidean distance squared (faster than sqrt for comparisons)
    ///
    /// Use this when you only need to compare distances, not compute exact values.
    public static func euclideanDistanceSquared(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        var sum: Double = 0
        for i in 0..<v1.count {
            let diff = Double(v1[i]) - Double(v2[i])
            sum += diff * diff
        }
        return sum
    }

    /// Calculate Euclidean distance squared (Float version for performance)
    public static func euclideanDistanceSquaredFloat(_ v1: [Float], _ v2: [Float]) -> Float {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        var sum: Float = 0
        for i in 0..<v1.count {
            let diff = v1[i] - v2[i]
            sum += diff * diff
        }
        return sum
    }

    /// Calculate dot product distance (negative dot product for min-heap)
    public static func dotProductDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let dotProduct = zip(v1, v2).map { Double($0) * Double($1) }.reduce(0, +)
        return -dotProduct  // Negate for min-heap (higher similarity = lower distance)
    }
}
