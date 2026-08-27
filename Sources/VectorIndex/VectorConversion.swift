// VectorConversion.swift
// VectorIndex - Unified vector type conversion utilities
//
// This file provides a single entry point for all vector-related
// type conversions between Swift types and TupleElement.
//
// Reference: Consolidates duplicate conversion logic from
// FlatVectorIndexMaintainer, HNSWIndexMaintainer, IVFIndexMaintainer, PQIndexMaintainer

import DatabaseMath
import DatabaseKit
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
    public static func extractFloat32Vector(
        from fieldValues: [any TupleElement]
    ) throws -> Vector {
        let fieldValue = try canonicalFieldValue(from: fieldValues)

        guard case .vector(let vector) = fieldValue else {
            throw VectorIndexError.invalidArgument(
                "A vector index expression must resolve to FieldValue.vector"
            )
        }
        return try float32Vector(from: vector)
    }

    /// Compares a canonical model vector with a retained persisted Float32
    /// payload without materializing either vector into an intermediate array.
    static func matchesPersistedVector(
        _ persisted: PersistedVectorView,
        fieldValues: [any TupleElement]
    ) throws(VectorIndexError) -> Bool {
        try matchesPersistedVector(
            persisted,
            fieldValue: canonicalFieldValue(from: fieldValues)
        )
    }

    static func matchesPersistedVector(
        _ persisted: PersistedVectorView,
        fieldValue: FieldValue
    ) throws(VectorIndexError) -> Bool {
        guard case .vector(let vector) = fieldValue else {
            throw .invalidArgument(
                "A vector index expression must resolve to FieldValue.vector"
            )
        }
        guard vector.count == persisted.count else {
            return false
        }

        return try persisted.withElements {
            (persistedElements) throws(VectorIndexError) -> Bool in
            switch vector.elementType {
            case .int8:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withInt8Elements(body)
                    }
                )
            case .int16:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withInt16Elements(body)
                    }
                )
            case .int32:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withInt32Elements(body)
                    }
                )
            case .int64:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withInt64Elements(body)
                    }
                )
            case .uint8:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withUInt8Elements(body)
                    }
                )
            case .uint16:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withUInt16Elements(body)
                    }
                )
            case .uint32:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withUInt32Elements(body)
                    }
                )
            case .uint64:
                return try compareIntegerElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withUInt64Elements(body)
                    }
                )
            case .float32:
                return try compareFloatingElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withFloat32Elements(body)
                    }
                )
            case .float64:
                return try compareFloatingElements(
                    of: vector,
                    persistedElements: persistedElements,
                    borrowing: { body in
                        vector.withFloat64Elements(body)
                    }
                )
            }
        }
    }

    /// Compares the VectorIndex Float32 projection while the retained model
    /// field remains inside DatabaseEngine's noncopyable owner boundary.
    static func matchesPersistedVector(
        _ persisted: PersistedVectorView,
        field: borrowing DatabaseRetainedVectorFieldView
    ) throws(VectorIndexError) -> Bool {
        guard field.count == persisted.count else { return false }
        return try persisted.withElements {
            (persistedElements) throws(VectorIndexError) -> Bool in
            switch field.elementType {
            case .int8:
                return try retainedFieldComparison(
                    field.withInt8Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .int16:
                return try retainedFieldComparison(
                    field.withInt16Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .int32:
                return try retainedFieldComparison(
                    field.withInt32Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .int64:
                return try retainedFieldComparison(
                    field.withInt64Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .uint8:
                return try retainedFieldComparison(
                    field.withUInt8Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .uint16:
                return try retainedFieldComparison(
                    field.withUInt16Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .uint32:
                return try retainedFieldComparison(
                    field.withUInt32Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .uint64:
                return try retainedFieldComparison(
                    field.withUInt64Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try integerElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .float32:
                return try retainedFieldComparison(
                    field.withFloat32Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try floatingElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            case .float64:
                return try retainedFieldComparison(
                    field.withFloat64Elements {
                        elements in
                        Result {
                            () throws(VectorIndexError) -> Bool in
                            try floatingElementsMatch(
                                elements,
                                persistedElements: persistedElements
                            )
                        }
                    },
                    elementType: field.elementType
                )
            }
        }
    }

    private static func retainedFieldComparison(
        _ result: Result<Bool, VectorIndexError>?,
        elementType: VectorElementType
    ) throws(VectorIndexError) -> Bool {
        guard let result else {
            throw .invalidStructure(
                "Retained vector storage does not match its declared element type \(elementType)"
            )
        }
        return try result.get()
    }

    private static func canonicalFieldValue(
        from fieldValues: [any TupleElement]
    ) throws(VectorIndexError) -> FieldValue {
        guard fieldValues.count == 1, let element = fieldValues.first else {
            throw .invalidStructure(
                "A vector index expression must produce exactly one field value"
            )
        }

        if let canonical = element as? CanonicalFieldValueTupleElement {
            // DataAccess creates this retained element for model fields. Read
            // its prepared value directly so indexing does not encode and then
            // decode a large vector merely to cross the TupleElement API.
            return canonical.prepared.value
        }
        do {
            return try FieldValue(tupleElement: element)
        } catch {
            throw .invalidStructure(
                "The vector index expression did not produce a canonical field value: \(error)"
            )
        }
    }

    private static func compareIntegerElements<Element>(
        of vector: Vector,
        persistedElements: borrowing PersistedVectorElements,
        borrowing: (((UnsafeBufferPointer<Element>) -> Void) -> Void?)
    ) throws(VectorIndexError) -> Bool where Element: BinaryInteger {
        var comparison: Result<Bool, VectorIndexError>?
        guard borrowing({ elements in
            comparison = Result {
                () throws(VectorIndexError) -> Bool in
                try integerElementsMatch(
                    elements,
                    persistedElements: persistedElements
                )
            }
        }) != nil, let comparison else {
            throw inconsistentStorage(for: vector)
        }
        return try comparison.get()
    }

    private static func compareFloatingElements<Element>(
        of vector: Vector,
        persistedElements: borrowing PersistedVectorElements,
        borrowing: (((UnsafeBufferPointer<Element>) -> Void) -> Void?)
    ) throws(VectorIndexError) -> Bool where Element: BinaryFloatingPoint {
        var comparison: Result<Bool, VectorIndexError>?
        guard borrowing({ elements in
            comparison = Result {
                () throws(VectorIndexError) -> Bool in
                try floatingElementsMatch(
                    elements,
                    persistedElements: persistedElements
                )
            }
        }) != nil, let comparison else {
            throw inconsistentStorage(for: vector)
        }
        return try comparison.get()
    }

    private static func integerElementsMatch<Element>(
        _ elements: UnsafeBufferPointer<Element>,
        persistedElements: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> Bool where Element: BinaryInteger {
        for index in elements.indices {
            let expected = try persistedElements.element(at: index)
            guard Float(elements[index]).bitPattern == expected.bitPattern else {
                return false
            }
        }
        return true
    }

    private static func floatingElementsMatch<Element>(
        _ elements: UnsafeBufferPointer<Element>,
        persistedElements: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> Bool where Element: BinaryFloatingPoint {
        for index in elements.indices {
            let converted = Float(elements[index])
            guard converted.isFinite else {
                throw .invalidArgument(
                    "A vector element exceeds the finite Float32 range"
                )
            }
            let expected = try persistedElements.element(at: index)
            guard converted.bitPattern == expected.bitPattern else {
                return false
            }
        }
        return true
    }

    private static func integerElementsMatch<Element>(
        _ elements: borrowing DatabaseRetainedVectorElements<Element>,
        persistedElements: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> Bool where Element: BinaryInteger {
        for index in 0..<elements.count {
            let expected = try persistedElements.element(at: index)
            guard Float(elements[index]).bitPattern == expected.bitPattern else {
                return false
            }
        }
        return true
    }

    private static func floatingElementsMatch<Element>(
        _ elements: borrowing DatabaseRetainedVectorElements<Element>,
        persistedElements: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> Bool where Element: BinaryFloatingPoint {
        for index in 0..<elements.count {
            let converted = Float(elements[index])
            guard converted.isFinite else {
                throw .invalidArgument(
                    "A vector element exceeds the finite Float32 range"
                )
            }
            let expected = try persistedElements.element(at: index)
            guard converted.bitPattern == expected.bitPattern else {
                return false
            }
        }
        return true
    }

    static func float32Vector(from vector: Vector) throws -> Vector {
        if vector.elementType == .float32 {
            return vector
        }

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
            preconditionFailure(
                "The Float32 zero-copy path must return before conversion"
            )
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
        do {
            return try Vector(float32: values)
        } catch {
            throw VectorIndexError.invalidStructure(
                "A validated vector became non-finite during Float32 conversion: \(error)"
            )
        }
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
        let (byteCount, overflow) = floats.count.multipliedReportingOverflow(
            by: MemoryLayout<Float>.stride
        )
        precondition(!overflow, "Float32 payload size overflow")
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

    /// Encodes a retained Float32 vector directly into its final persisted
    /// payload. This allocation is the required storage ownership boundary;
    /// the source elements are borrowed and no intermediate array is created.
    public static func float32VectorToBytes(
        _ vector: Vector
    ) throws -> ByteString {
        guard vector.elementType == .float32 else {
            throw VectorIndexError.invalidArgument(
                "Persisted vector encoding requires Float32 elements"
            )
        }
        guard let payload = vector.withFloat32Elements({ elements in
            floatArrayToBytes(elements)
        }) else {
            throw inconsistentStorage(for: vector)
        }
        return payload
    }

    private static func floatArrayToBytes(
        _ floats: UnsafeBufferPointer<Float>
    ) -> ByteString {
        let (byteCount, overflow) = floats.count.multipliedReportingOverflow(
            by: MemoryLayout<Float>.stride
        )
        precondition(!overflow, "Float32 payload size overflow")
        guard !floats.isEmpty else {
            return ByteString()
        }
        return ByteString.copying(count: byteCount) { output in
#if _endian(little)
            output.copyMemory(from: UnsafeRawBufferPointer(floats))
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

    /// Encodes a rectangular Float32 matrix into its final persisted owner.
    /// Rows are borrowed directly so large trained codebooks do not require a
    /// flattened intermediate array before the storage-boundary allocation.
    static func floatMatrixToBytesForPersistence(
        _ rows: [[Float]],
        columnCount: Int
    ) throws(VectorIndexError) -> ByteString {
        guard columnCount >= 0,
              rows.allSatisfy({ $0.count == columnCount }) else {
            throw .invalidStructure(
                "Float32 persistence matrix is not rectangular"
            )
        }
        guard rows.allSatisfy({ row in
            row.allSatisfy { $0.isFinite }
        }) else {
            throw .invalidStructure(
                "Float32 persistence matrix contains a non-finite element"
            )
        }
        let (valueCount, valueCountOverflow) = rows.count
            .multipliedReportingOverflow(by: columnCount)
        let (byteCount, byteCountOverflow) = valueCount
            .multipliedReportingOverflow(by: MemoryLayout<Float>.stride)
        guard !valueCountOverflow, !byteCountOverflow else {
            throw .invalidArgument(
                "Float32 persistence matrix exceeds the current platform limit"
            )
        }
        guard byteCount > 0 else {
            return ByteString()
        }

        return ByteString.copying(count: byteCount) { output in
            var outputOffset = 0
            for row in rows {
#if _endian(little)
                row.withUnsafeBytes { source in
                    let outputRange = outputOffset..<(outputOffset + source.count)
                    UnsafeMutableRawBufferPointer(
                        rebasing: output[outputRange]
                    ).copyMemory(from: source)
                    outputOffset += source.count
                }
#else
                for value in row {
                    let bits = value.bitPattern
                    output[outputOffset] = UInt8(truncatingIfNeeded: bits)
                    output[outputOffset + 1] = UInt8(truncatingIfNeeded: bits >> 8)
                    output[outputOffset + 2] = UInt8(truncatingIfNeeded: bits >> 16)
                    output[outputOffset + 3] = UInt8(truncatingIfNeeded: bits >> 24)
                    outputOffset += MemoryLayout<Float>.stride
                }
#endif
            }
        }
    }

    /// Creates a retained view over a validated persisted vector payload.
    /// Search paths use this API so candidate bytes are never materialized.
    static func persistedVector(
        _ bytes: ByteString,
        expectedCount: Int
    ) throws(VectorIndexError) -> PersistedVectorView {
        try PersistedVectorView(
            payload: bytes,
            expectedCount: expectedCount
        )
    }

    /// Materializes persisted values for algorithms that require mutable,
    /// repeatedly indexed training storage. Search execution must use
    /// `persistedVector(_:expectedCount:)` instead.
    static func materializeFloatArrayForTraining(
        _ bytes: ByteString,
        expectedCount: Int
    ) throws(VectorIndexError) -> [Float] {
        let view = try persistedVector(bytes, expectedCount: expectedCount)
        return try view.withElements {
            (source) throws(VectorIndexError) -> [Float] in
            var values: [Float] = []
            values.reserveCapacity(view.count)
            for index in 0..<view.count {
                values.append(
                    try source.element(at: index)
                )
            }
            return values
        }
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

    /// Computes a distance while borrowing both the retained query vector and
    /// the persisted candidate payload. No element buffer is allocated.
    static func distance(
        metric: VectorMetric,
        from query: Vector,
        to candidate: PersistedVectorView
    ) throws -> Double {
        guard query.elementType == .float32 else {
            throw VectorIndexError.invalidArgument(
                "Vector index queries require Float32 elements"
            )
        }
        guard query.count == candidate.count else {
            throw VectorIndexError.dimensionMismatch(
                expected: query.count,
                actual: candidate.count
            )
        }
        var outcome: Result<Double, VectorIndexError>?
        guard query.withFloat32Elements({ queryElements in
            outcome = Result {
                () throws(VectorIndexError) -> Double in
                try candidate.withElements {
                    (candidateElements) throws(VectorIndexError) -> Double in
                    try distance(
                        metric: metric,
                        query: queryElements,
                        candidateElements: candidateElements
                    )
                }
            }
        }) != nil else {
            throw inconsistentStorage(for: query)
        }
        guard let outcome else {
            preconditionFailure("Vector query borrow produced no result")
        }
        return try outcome.get()
    }

    /// Computes a distance while borrowing two retained Float32 vectors.
    static func distance(
        metric: VectorMetric,
        from query: Vector,
        to candidate: Vector
    ) throws -> Double {
        guard query.elementType == .float32,
              candidate.elementType == .float32 else {
            throw VectorIndexError.invalidArgument(
                "Vector index distance requires Float32 elements"
            )
        }
        guard query.count == candidate.count else {
            throw VectorIndexError.dimensionMismatch(
                expected: query.count,
                actual: candidate.count
            )
        }
        guard let result = query.withFloat32Elements({ queryElements in
            candidate.withFloat32Elements { candidateElements in
                distance(
                    metric: metric,
                    query: queryElements,
                    candidate: candidateElements
                )
            }
        }), let distance = result else {
            throw VectorIndexError.invalidStructure(
                "Vector storage does not match its declared Float32 element type"
            )
        }
        return distance
    }

    /// Calculate cosine distance between two vectors
    ///
    /// - Returns: Distance in range [0, 2] (0 = identical, 2 = opposite)
    static func cosineDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let values = v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                dotAndNorms(lhs, rhs)
            }
        }

        return cosineDistance(
            dotProduct: values.dotProduct,
            lhsNormSquared: values.lhsNormSquared,
            rhsNormSquared: values.rhsNormSquared
        )
    }

    /// Calculate Euclidean distance between two vectors
    static func euclideanDistance(_ v1: [Float], _ v2: [Float]) -> Double {
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
    static func euclideanDistanceSquared(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let sum = v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                squaredDistance(lhs, rhs)
            }
        }
        return sum
    }

    /// Calculate dot product distance (negative dot product for min-heap)
    static func dotProductDistance(_ v1: [Float], _ v2: [Float]) -> Double {
        precondition(v1.count == v2.count, "Vector dimensions must match")

        let dotProduct = v1.withUnsafeBufferPointer { lhs in
            v2.withUnsafeBufferPointer { rhs in
                dot(lhs, rhs)
            }
        }
        return -dotProduct  // Negate for min-heap (higher similarity = lower distance)
    }

    @inline(__always)
    private static func distance(
        metric: VectorMetric,
        query: UnsafeBufferPointer<Float>,
        candidate: UnsafeBufferPointer<Float>
    ) -> Double {
        switch metric {
        case .cosine:
            let values = dotAndNorms(query, candidate)
            return cosineDistance(
                dotProduct: values.dotProduct,
                lhsNormSquared: values.lhsNormSquared,
                rhsNormSquared: values.rhsNormSquared
            )
        case .euclidean:
            return DatabaseMath.squareRoot(squaredDistance(query, candidate))
        case .dotProduct:
            return -dot(query, candidate)
        }
    }

    @inline(__always)
    private static func distance(
        metric: VectorMetric,
        query: UnsafeBufferPointer<Float>,
        candidateElements: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> Double {
        switch metric {
        case .cosine:
            let values = try dotAndNorms(query, candidateElements)
            return cosineDistance(
                dotProduct: values.dotProduct,
                lhsNormSquared: values.lhsNormSquared,
                rhsNormSquared: values.rhsNormSquared
            )
        case .euclidean:
            return DatabaseMath.squareRoot(
                try squaredDistance(query, candidateElements)
            )
        case .dotProduct:
            return -(try dot(query, candidateElements))
        }
    }

    @inline(__always)
    private static func dot(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: UnsafeBufferPointer<Float>
    ) -> Double {
        var result = 0.0
        for index in lhs.indices {
            result += Double(lhs[index]) * Double(rhs[index])
        }
        return result
    }

    @inline(__always)
    private static func squaredDistance(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: UnsafeBufferPointer<Float>
    ) -> Double {
        var result = 0.0
        for index in lhs.indices {
            let diff = Double(lhs[index]) - Double(rhs[index])
            result += diff * diff
        }
        return result
    }

    @inline(__always)
    private static func dotAndNorms(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: UnsafeBufferPointer<Float>
    ) -> (dotProduct: Double, lhsNormSquared: Double, rhsNormSquared: Double) {
        var dotProduct = 0.0
        var lhsNormSquared = 0.0
        var rhsNormSquared = 0.0
        for index in lhs.indices {
            let left = Double(lhs[index])
            let right = Double(rhs[index])
            dotProduct += left * right
            lhsNormSquared += left * left
            rhsNormSquared += right * right
        }
        return (dotProduct, lhsNormSquared, rhsNormSquared)
    }

    @inline(__always)
    private static func dot(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> Double {
        var result = 0.0
        for index in lhs.indices {
            result += Double(lhs[index]) * Double(
                try rhs.element(at: index)
            )
        }
        return result
    }

    @inline(__always)
    private static func squaredDistance(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> Double {
        var result = 0.0
        for index in lhs.indices {
            let difference = Double(lhs[index]) - Double(
                try rhs.element(at: index)
            )
            result += difference * difference
        }
        return result
    }

    @inline(__always)
    private static func dotAndNorms(
        _ lhs: UnsafeBufferPointer<Float>,
        _ rhs: borrowing PersistedVectorElements
    ) throws(VectorIndexError) -> (
        dotProduct: Double,
        lhsNormSquared: Double,
        rhsNormSquared: Double
    ) {
        var dotProduct = 0.0
        var lhsNormSquared = 0.0
        var rhsNormSquared = 0.0
        for index in lhs.indices {
            let left = Double(lhs[index])
            let right = Double(
                try rhs.element(at: index)
            )
            dotProduct += left * right
            lhsNormSquared += left * left
            rhsNormSquared += right * right
        }
        return (dotProduct, lhsNormSquared, rhsNormSquared)
    }

    @inline(__always)
    private static func cosineDistance(
        dotProduct: Double,
        lhsNormSquared: Double,
        rhsNormSquared: Double
    ) -> Double {
        let lhsNorm = DatabaseMath.squareRoot(lhsNormSquared)
        let rhsNorm = DatabaseMath.squareRoot(rhsNormSquared)
        // A zero vector has no direction. Treat it as orthogonal so every
        // backend uses the same finite distance as SwiftHNSW (1 - 0).
        guard lhsNorm > 0, rhsNorm > 0 else { return 1.0 }
        let similarity = min(
            1.0,
            max(-1.0, dotProduct / (lhsNorm * rhsNorm))
        )
        return 1.0 - similarity
    }
}
