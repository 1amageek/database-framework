// VectorConversion.swift
// VectorIndex - Unified vector type conversion utilities
//
// This file provides a single entry point for all vector-related
// type conversions between Swift types and TupleElement.
//
// Reference: Consolidates duplicate conversion logic from
// FlatVectorIndexMaintainer, HNSWIndexMaintainer, IVFIndexMaintainer, PQIndexMaintainer

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseMath
import DatabaseTypes
import StorageKit
import DatabaseEngine

/// Converts canonical vector field values to the Float32 representation used
/// by the vector index algorithms and their persisted payloads.
///
/// `DatabaseTypes.Vector` preserves its declared scalar width. Vector indexes
/// intentionally operate on Float32 values, so integer and Float64 vectors are
/// converted at this boundary. The source vector's contiguous storage is
/// borrowed and only the required Float32 result buffer is allocated.
public struct VectorConversion: Sendable {

    private init() {}

    // MARK: - Field Value Extraction

    /// Extracts the one canonical vector value produced by the index key
    /// expression.
    public static func extractFloatArray(from fieldValues: [any TupleElement]) throws -> [Float] {
        guard fieldValues.count == 1, let element = fieldValues.first else {
            throw VectorIndexError.invalidStructure(
                "A vector index expression must produce exactly one field value"
            )
        }

        let fieldValue: FieldValue
        do {
            fieldValue = try FieldValue(tupleElement: element)
        } catch {
            throw VectorIndexError.invalidStructure(
                "The vector index expression did not produce a canonical field value: \(error)"
            )
        }

        guard case .vector(let vector) = fieldValue else {
            throw VectorIndexError.invalidArgument(
                "A vector index expression must resolve to FieldValue.vector"
            )
        }
        return try floatArray(from: vector)
    }

    private static func floatArray(from vector: Vector) throws -> [Float] {
        var values: [Float] = []
        values.reserveCapacity(vector.count)

        switch vector.elementType {
        case .int8:
            guard vector.withInt8Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .int16:
            guard vector.withInt16Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .int32:
            guard vector.withInt32Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .int64:
            guard vector.withInt64Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .uint8:
            guard vector.withUInt8Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .uint16:
            guard vector.withUInt16Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .uint32:
            guard vector.withUInt32Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .uint64:
            guard vector.withUInt64Elements({
                appendIntegers($0, to: &values)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .float32:
            guard vector.withFloat32Elements({
                values.append(contentsOf: $0)
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        case .float64:
            guard try vector.withFloat64Elements({
                for element in $0 {
                    let converted = Float(element)
                    guard converted.isFinite else {
                        throw VectorIndexError.invalidArgument(
                            "A Float64 vector element exceeds the finite Float32 range"
                        )
                    }
                    values.append(converted)
                }
                return ()
            }) != nil else {
                throw inconsistentStorage(for: vector)
            }
        }
        return values
    }

    private static func appendIntegers<Element>(
        _ elements: UnsafeBufferPointer<Element>,
        to values: inout [Float]
    ) where Element: BinaryInteger {
        for element in elements {
            values.append(Float(element))
        }
    }

    private static func inconsistentStorage(
        for vector: Vector
    ) -> VectorIndexError {
        .invalidStructure(
            "Vector storage does not match its declared element type \(vector.elementType)"
        )
    }

    // MARK: - Byte Conversion

    /// Convert Float array to bytes (little-endian)
    ///
    /// - Parameter floats: Float array
    /// - Returns: Byte array
    public static func floatArrayToBytes(_ floats: [Float]) -> ByteString {
        let byteCount = floats.count * MemoryLayout<Float>.stride
        guard !floats.isEmpty else {
            return ByteString()
        }
        return ByteString.copying(count: byteCount) { output in
#if _endian(little)
            floats.withUnsafeBufferPointer { input in
                output.copyMemory(from: UnsafeRawBufferPointer(input))
            }
#else
            for index in floats.indices {
                let value = floats[index].bitPattern
                let offset = index * MemoryLayout<Float>.stride
                output[offset] = UInt8(truncatingIfNeeded: value)
                output[offset + 1] = UInt8(truncatingIfNeeded: value >> 8)
                output[offset + 2] = UInt8(truncatingIfNeeded: value >> 16)
                output[offset + 3] = UInt8(truncatingIfNeeded: value >> 24)
            }
#endif
        }
    }

    /// Convert bytes to Float array (little-endian)
    ///
    /// - Parameter bytes: Byte array
    /// - Returns: Float array
    public static func bytesToFloatArray(_ bytes: ByteString) -> [Float] {
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
    public static func decodeFloatArray(_ bytes: ByteString, expectedCount: Int) throws -> [Float] {
        guard bytes.count == expectedCount * 4 else {
            throw VectorIndexError.invalidStructure(
                "Vector payload length \(bytes.count) does not match expected dimension \(expectedCount)"
            )
        }
        return bytesToFloatArray(bytes)
    }

    /// Convert UInt64 to bytes (little-endian)
    public static func uint64ToBytes(_ value: UInt64) -> ByteString {
        ByteConversion.uint64ToBytes(value)
    }

    /// Convert bytes to UInt64 (little-endian)
    public static func bytesToUInt64(_ bytes: ByteString) throws -> UInt64 {
        try ByteConversion.bytesToUInt64(bytes)
    }

    /// Convert Int64 to bytes (little-endian)
    public static func int64ToBytes(_ value: Int64) -> ByteString {
        ByteConversion.int64ToBytes(value)
    }

    /// Convert bytes to Int64 (little-endian)
    public static func bytesToInt64(_ bytes: ByteString) throws -> Int64 {
        try ByteConversion.bytesToInt64(bytes)
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

        let norm1 = DatabaseMath.squareRoot(Double(values.lhsNormSquared))
        let norm2 = DatabaseMath.squareRoot(Double(values.rhsNormSquared))
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
        return DatabaseMath.squareRoot(Double(sumSquares))
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
