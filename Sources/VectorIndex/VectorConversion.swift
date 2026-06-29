// VectorConversion.swift
// VectorIndex - Unified vector type conversion utilities
//
// This file provides a single entry point for all vector-related
// type conversions between Swift types and TupleElement.
//
// Reference: Consolidates duplicate conversion logic from
// FlatVectorIndexMaintainer, HNSWIndexMaintainer, IVFIndexMaintainer, PQIndexMaintainer

import Foundation
import StorageKit
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
        var bytes = [UInt8](repeating: 0, count: floats.count * MemoryLayout<Float>.stride)
        guard !floats.isEmpty else {
            return bytes
        }

#if _endian(little)
        bytes.withUnsafeMutableBytes { output in
            floats.withUnsafeBufferPointer { input in
                output.copyMemory(from: UnsafeRawBufferPointer(input))
            }
        }
#else
        for index in floats.indices {
            var value = floats[index].bitPattern.littleEndian
            withUnsafeBytes(of: &value) { source in
                let offset = index * MemoryLayout<Float>.stride
                bytes.replaceSubrange(offset..<(offset + MemoryLayout<Float>.stride), with: source)
            }
        }
#endif
        return bytes
    }

    /// Convert bytes to Float array (little-endian)
    ///
    /// - Parameter bytes: Byte array
    /// - Returns: Float array
    public static func bytesToFloatArray(_ bytes: [UInt8]) -> [Float] {
        let count = bytes.count / MemoryLayout<Float>.stride
        var floats = [Float](repeating: 0, count: count)
        guard count > 0 else {
            return floats
        }

#if _endian(little)
        floats.withUnsafeMutableBytes { output in
            bytes.withUnsafeBytes { input in
                let source = UnsafeRawBufferPointer(
                    start: input.baseAddress,
                    count: count * MemoryLayout<Float>.stride
                )
                output.copyMemory(from: source)
            }
        }
#else
        for index in 0..<count {
            let offset = index * MemoryLayout<Float>.stride
            let bits = UInt32(bytes[offset])
                | (UInt32(bytes[offset + 1]) << 8)
                | (UInt32(bytes[offset + 2]) << 16)
                | (UInt32(bytes[offset + 3]) << 24)
            floats[index] = Float(bitPattern: bits)
        }
#endif
        return floats
    }

    /// Decode a Float array from a validated little-endian binary payload.
    ///
    /// Maintainers should use this method when reading persisted vector payloads.
    public static func decodeFloatArray(_ bytes: [UInt8], expectedCount: Int) throws -> [Float] {
        guard bytes.count == expectedCount * 4 else {
            throw VectorIndexError.invalidStructure(
                "Vector payload length \(bytes.count) does not match expected dimension \(expectedCount)"
            )
        }
        return bytesToFloatArray(bytes)
    }

    /// Convert UInt64 to bytes (little-endian)
    public static func uint64ToBytes(_ value: UInt64) -> [UInt8] {
        withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }

    /// Convert bytes to UInt64 (little-endian)
    public static func bytesToUInt64(_ bytes: [UInt8]) -> UInt64 {
        guard bytes.count == 8 else { return 0 }
        return UInt64(bytes[0])
            | (UInt64(bytes[1]) << 8)
            | (UInt64(bytes[2]) << 16)
            | (UInt64(bytes[3]) << 24)
            | (UInt64(bytes[4]) << 32)
            | (UInt64(bytes[5]) << 40)
            | (UInt64(bytes[6]) << 48)
            | (UInt64(bytes[7]) << 56)
    }

    /// Convert Int64 to bytes (little-endian)
    public static func int64ToBytes(_ value: Int64) -> [UInt8] {
        withUnsafeBytes(of: value.littleEndian) { Array($0) }
    }

    /// Convert bytes to Int64 (little-endian)
    public static func bytesToInt64(_ bytes: [UInt8]) -> Int64 {
        guard bytes.count >= 8 else { return 0 }
        let value = UInt64(bytes[0])
            | (UInt64(bytes[1]) << 8)
            | (UInt64(bytes[2]) << 16)
            | (UInt64(bytes[3]) << 24)
            | (UInt64(bytes[4]) << 32)
            | (UInt64(bytes[5]) << 40)
            | (UInt64(bytes[6]) << 48)
            | (UInt64(bytes[7]) << 56)
        return Int64(bitPattern: value)
    }
}

// MARK: - Distance Calculations

extension VectorConversion {

    /// Calculate cosine distance between two vectors
    ///
    /// - Returns: Distance in range [0, 2] (0 = identical, 2 = opposite)
    public static func cosineDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let values = v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                dotAndNorms(lhs, rhs)
            }
        }

        let norm1 = sqrt(Double(values.lhsNormSquared))
        let norm2 = sqrt(Double(values.rhsNormSquared))
        guard norm1 > 0 && norm2 > 0 else { return 2.0 }
        let cosineSimilarity = Double(values.dotProduct) / (norm1 * norm2)
        return 1.0 - cosineSimilarity
    }

    /// Calculate Euclidean distance between two vectors
    public static func euclideanDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let sumSquares = v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                squaredDistance(lhs, rhs)
            }
        }
        return sqrt(Double(sumSquares))
    }

    /// Calculate Euclidean distance squared (faster than sqrt for comparisons)
    ///
    /// Use this when you only need to compare distances, not compute exact values.
    public static func euclideanDistanceSquared(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let sum = v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                squaredDistance(lhs, rhs)
            }
        }
        return Double(sum)
    }

    /// Calculate Euclidean distance squared (Float version for performance)
    public static func euclideanDistanceSquaredFloat(_ v1: [Float], _ v2: [Float]) -> Float {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        return v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                squaredDistance(lhs, rhs)
            }
        }
    }

    /// Calculate dot product distance (negative dot product for min-heap)
    public static func dotProductDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let dotProduct = v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                dot(lhs, rhs)
            }
        }
        return -Double(dotProduct)  // Negate for min-heap (higher similarity = lower distance)
    }

    @inline(__always)
    private static func dot(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: UnsafeBufferPointer<Float>
    ) -> Float {
        var index = 0
        var accumulator = SIMD8<Float>.zero
        let simdEnd = lhs.count - (lhs.count % SIMD8<Float>.scalarCount)

        while index < simdEnd {
            let left = loadSIMD8(lhs, at: index)
            let right = loadSIMD8(rhs, at: index)
            accumulator += left * right
            index += SIMD8<Float>.scalarCount
        }

        var result = accumulator.sum()
        while index < lhs.count {
            result += lhs[index] * rhs[index]
            index += 1
        }
        return result
    }

    @inline(__always)
    private static func squaredDistance(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: UnsafeBufferPointer<Float>
    ) -> Float {
        var index = 0
        var accumulator = SIMD8<Float>.zero
        let simdEnd = lhs.count - (lhs.count % SIMD8<Float>.scalarCount)

        while index < simdEnd {
            let left = loadSIMD8(lhs, at: index)
            let right = loadSIMD8(rhs, at: index)
            let diff = left - right
            accumulator += diff * diff
            index += SIMD8<Float>.scalarCount
        }

        var result = accumulator.sum()
        while index < lhs.count {
            let diff = lhs[index] - rhs[index]
            result += diff * diff
            index += 1
        }
        return result
    }

    @inline(__always)
    private static func dotAndNorms(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: UnsafeBufferPointer<Float>
    ) -> (dotProduct: Float, lhsNormSquared: Float, rhsNormSquared: Float) {
        var index = 0
        var dotAccumulator = SIMD8<Float>.zero
        var lhsAccumulator = SIMD8<Float>.zero
        var rhsAccumulator = SIMD8<Float>.zero
        let simdEnd = lhs.count - (lhs.count % SIMD8<Float>.scalarCount)

        while index < simdEnd {
            let left = loadSIMD8(lhs, at: index)
            let right = loadSIMD8(rhs, at: index)
            dotAccumulator += left * right
            lhsAccumulator += left * left
            rhsAccumulator += right * right
            index += SIMD8<Float>.scalarCount
        }

        var dotProduct = dotAccumulator.sum()
        var lhsNormSquared = lhsAccumulator.sum()
        var rhsNormSquared = rhsAccumulator.sum()
        while index < lhs.count {
            let left = lhs[index]
            let right = rhs[index]
            dotProduct += left * right
            lhsNormSquared += left * left
            rhsNormSquared += right * right
            index += 1
        }
        return (dotProduct, lhsNormSquared, rhsNormSquared)
    }

    @inline(__always)
    private static func loadSIMD8(
        _ pointer: UnsafeBufferPointer<Float>,
        at index: Int
    ) -> SIMD8<Float> {
        UnsafeRawPointer(pointer.baseAddress!.advanced(by: index))
            .loadUnaligned(as: SIMD8<Float>.self)
    }
}
