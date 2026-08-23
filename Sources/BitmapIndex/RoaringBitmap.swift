// RoaringBitmap.swift
// BitmapIndex - Roaring Bitmap implementation for efficient set operations
//
// Reference: Lemire et al., "Roaring Bitmaps: Implementation of an Optimized
// Software Library", Software: Practice and Experience, 2016
// https://arxiv.org/abs/1603.06549

import DatabaseTypes
import StorageKit

/// Roaring Bitmap implementation for efficient set operations
///
/// **Overview**:
/// Roaring bitmaps use a two-level structure:
/// - High 16 bits determine the container (up to 65536 containers)
/// - Low 16 bits are stored in containers (array, bitmap, or run)
///
/// **Container Types**:
/// - Array: For sparse containers (<4096 values)
/// - Bitmap: For dense containers (1024 × 64-bit words = 65536 bits)
/// - Run: For consecutive runs of values
///
/// **Performance**:
/// - AND: O(n) where n is smaller bitmap
/// - OR: O(n + m)
/// - Cardinality: O(array values + bitmap words + runs)
/// - Memory: Adaptive based on density
public struct RoaringBitmap: Sendable, Equatable, Sequence {
    /// Container threshold: switch from array to bitmap at 4096 elements
    private static let arrayMaxSize = 4096

    /// Containers indexed by high 16 bits
    fileprivate var containers: [UInt16: Container]

    /// Container storage types
    ///
    /// **NOTE on `run` encoding**: In `(start, length)`, `length` is the
    /// **end offset (inclusive)**, not the element count — this follows the
    /// original Roaring paper (Lemire et al., 2016). Specifically:
    ///
    /// - Run covers `[start, start + length]`, so its cardinality is `length + 1`.
    /// - A single element is `(start: v, length: 0)`.
    /// - The full `UInt16` range is encoded as `(start: 0, length: 65535)` —
    ///   relying on `length: UInt16` wrap to implicitly represent "all 65536 values".
    ///
    /// Any code iterating a run must therefore use `start...start + length`
    /// (inclusive) and any code computing cardinality must add 1.
    enum Container: Sendable, Equatable {
        case array([UInt16])           // Sorted array of low 16 bits
        case bitmap([UInt64])          // 1024 × 64-bit words
        case run([(start: UInt16, length: UInt16)])  // Run-length encoded (length = end offset inclusive)

        // Manual Equatable conformance (tuples don't auto-conform)
        static func == (lhs: Container, rhs: Container) -> Bool {
            switch (lhs, rhs) {
            case (.array(let a), .array(let b)):
                return a == b
            case (.bitmap(let a), .bitmap(let b)):
                return a == b
            case (.run(let a), .run(let b)):
                guard a.count == b.count else { return false }
                for (runA, runB) in zip(a, b) {
                    if runA.start != runB.start || runA.length != runB.length {
                        return false
                    }
                }
                return true
            default:
                return false
            }
        }

        /// Number of elements in container
        var cardinality: Int {
            switch self {
            case .array(let arr):
                return arr.count
            case .bitmap(let bits):
                return bits.reduce(0) { $0 + $1.nonzeroBitCount }
            case .run(let runs):
                return runs.reduce(0) { $0 + Int($1.length) + 1 }
            }
        }

        @inline(__always)
        private static func lowerBound(
            of value: UInt16,
            in values: [UInt16]
        ) -> Int {
            var lower = 0
            var upper = values.count
            while lower < upper {
                let middle = lower + (upper - lower) / 2
                if values[middle] < value {
                    lower = middle + 1
                } else {
                    upper = middle
                }
            }
            return lower
        }

        /// Check if value is in container
        func contains(_ value: UInt16) -> Bool {
            switch self {
            case .array(let arr):
                let index = Container.lowerBound(of: value, in: arr)
                return index < arr.count && arr[index] == value
            case .bitmap(let bits):
                let wordIndex = Int(value) / 64
                let bitIndex = Int(value) % 64
                return (bits[wordIndex] & (1 << bitIndex)) != 0
            case .run(let runs):
                var lower = 0
                var upper = runs.count
                let target = Int(value)
                while lower < upper {
                    let middle = lower + (upper - lower) / 2
                    let run = runs[middle]
                    let start = Int(run.start)
                    let end = start + Int(run.length)
                    if target < start {
                        upper = middle
                    } else if target > end {
                        lower = middle + 1
                    } else {
                        return true
                    }
                }
                return false
            }
        }

        /// Add value to container, return new container (may change type)
        func adding(_ value: UInt16) -> Container {
            switch self {
            case .array(var arr):
                let index = Container.lowerBound(of: value, in: arr)
                if index < arr.count, arr[index] == value {
                    return .array(arr)
                }
                arr.insert(value, at: index)
                if arr.count > RoaringBitmap.arrayMaxSize {
                    return Container.arrayToBitmap(arr)
                }
                return .array(arr)

            case .bitmap(var bits):
                let wordIndex = Int(value) / 64
                let bitIndex = Int(value) % 64
                bits[wordIndex] |= (1 << bitIndex)
                return .bitmap(bits)

            case .run(let runs):
                return Container.adding(value, to: runs)
            }
        }

        /// Remove value from container, return new container (may change type)
        func removing(_ value: UInt16) -> Container? {
            switch self {
            case .array(var arr):
                let index = Container.lowerBound(of: value, in: arr)
                guard index < arr.count, arr[index] == value else {
                    return .array(arr)
                }
                arr.remove(at: index)
                return arr.isEmpty ? nil : .array(arr)

            case .bitmap(var bits):
                let wordIndex = Int(value) / 64
                let bitIndex = Int(value) % 64
                bits[wordIndex] &= ~(1 << bitIndex)
                let count = bits.reduce(0) { $0 + $1.nonzeroBitCount }
                if count == 0 {
                    return nil
                }
                if count <= RoaringBitmap.arrayMaxSize {
                    return Container.bitmapToArray(bits)
                }
                return .bitmap(bits)

            case .run(let runs):
                return Container.removing(value, from: runs)
            }
        }

        private static func adding(
            _ value: UInt16,
            to runs: [(start: UInt16, length: UInt16)]
        ) -> Container {
            var updated = runs
            let target = Int(value)

            for index in updated.indices {
                let run = updated[index]
                let start = Int(run.start)
                let end = start + Int(run.length)
                if target >= start, target <= end {
                    return .run(updated)
                }
                guard target < start else { continue }

                let joinsPrevious: Bool
                if index > updated.startIndex {
                    let previous = updated[index - 1]
                    joinsPrevious = Int(previous.start)
                        + Int(previous.length) + 1 == target
                } else {
                    joinsPrevious = false
                }
                let joinsNext = target + 1 == start

                switch (joinsPrevious, joinsNext) {
                case (true, true):
                    let previousIndex = index - 1
                    let previousStart = Int(updated[previousIndex].start)
                    updated[previousIndex].length = UInt16(end - previousStart)
                    updated.remove(at: index)
                case (true, false):
                    let previousIndex = index - 1
                    updated[previousIndex].length &+= 1
                case (false, true):
                    updated[index] = (
                        start: value,
                        length: UInt16(end - target)
                    )
                case (false, false):
                    updated.insert((start: value, length: 0), at: index)
                }
                return .run(updated)
            }

            if let lastIndex = updated.indices.last {
                let last = updated[lastIndex]
                if Int(last.start) + Int(last.length) + 1 == target {
                    updated[lastIndex].length &+= 1
                    return .run(updated)
                }
            }
            updated.append((start: value, length: 0))
            return .run(updated)
        }

        private static func removing(
            _ value: UInt16,
            from runs: [(start: UInt16, length: UInt16)]
        ) -> Container? {
            var updated = runs
            let target = Int(value)

            for index in updated.indices {
                let run = updated[index]
                let start = Int(run.start)
                let end = start + Int(run.length)
                if target < start {
                    return .run(updated)
                }
                guard target <= end else { continue }

                if start == end {
                    updated.remove(at: index)
                } else if target == start {
                    updated[index] = (
                        start: UInt16(start + 1),
                        length: UInt16(end - start - 1)
                    )
                } else if target == end {
                    updated[index].length = UInt16(end - start - 1)
                } else {
                    updated[index].length = UInt16(target - start - 1)
                    updated.insert(
                        (
                            start: UInt16(target + 1),
                            length: UInt16(end - target - 1)
                        ),
                        at: index + 1
                    )
                }
                return updated.isEmpty ? nil : .run(updated)
            }
            return .run(updated)
        }

        // MARK: - Container Type Conversions

        static func arrayToBitmap(_ arr: [UInt16]) -> Container {
            var bits = [UInt64](repeating: 0, count: 1024)
            for value in arr {
                let wordIndex = Int(value) / 64
                let bitIndex = Int(value) % 64
                bits[wordIndex] |= (1 << bitIndex)
            }
            return .bitmap(bits)
        }

        static func bitmapToArray(_ bits: [UInt64]) -> Container {
            var arr: [UInt16] = []
            for (wordIndex, word) in bits.enumerated() {
                var w = word
                while w != 0 {
                    let bitIndex = w.trailingZeroBitCount
                    arr.append(UInt16(wordIndex * 64 + bitIndex))
                    w &= w - 1  // Clear lowest set bit
                }
            }
            return .array(arr)
        }

        static func runToArray(_ runs: [(start: UInt16, length: UInt16)]) -> [UInt16] {
            var arr: [UInt16] = []
            arr.reserveCapacity(
                runs.reduce(0) { $0 + Int($1.length) + 1 }
            )
            for (start, length) in runs {
                for i in 0...length {
                    arr.append(start &+ i)
                }
            }
            return arr
        }

        private static func intersectSorted(
            _ lhs: [UInt16],
            _ rhs: [UInt16]
        ) -> [UInt16] {
            var result: [UInt16] = []
            result.reserveCapacity(Swift.min(lhs.count, rhs.count))
            var leftIndex = 0
            var rightIndex = 0
            while leftIndex < lhs.count, rightIndex < rhs.count {
                let left = lhs[leftIndex]
                let right = rhs[rightIndex]
                if left < right {
                    leftIndex += 1
                } else if left > right {
                    rightIndex += 1
                } else {
                    result.append(left)
                    leftIndex += 1
                    rightIndex += 1
                }
            }
            return result
        }

        private static func unionSorted(
            _ lhs: [UInt16],
            _ rhs: [UInt16]
        ) -> [UInt16] {
            var result: [UInt16] = []
            result.reserveCapacity(lhs.count + rhs.count)
            var leftIndex = 0
            var rightIndex = 0
            while leftIndex < lhs.count, rightIndex < rhs.count {
                let left = lhs[leftIndex]
                let right = rhs[rightIndex]
                if left < right {
                    result.append(left)
                    leftIndex += 1
                } else if left > right {
                    result.append(right)
                    rightIndex += 1
                } else {
                    result.append(left)
                    leftIndex += 1
                    rightIndex += 1
                }
            }
            result.append(contentsOf: lhs[leftIndex...])
            result.append(contentsOf: rhs[rightIndex...])
            return result
        }

        private static func subtractSorted(
            _ lhs: [UInt16],
            _ rhs: [UInt16]
        ) -> [UInt16] {
            var result: [UInt16] = []
            result.reserveCapacity(lhs.count)
            var leftIndex = 0
            var rightIndex = 0
            while leftIndex < lhs.count, rightIndex < rhs.count {
                let left = lhs[leftIndex]
                let right = rhs[rightIndex]
                if left < right {
                    result.append(left)
                    leftIndex += 1
                } else if left > right {
                    rightIndex += 1
                } else {
                    leftIndex += 1
                    rightIndex += 1
                }
            }
            result.append(contentsOf: lhs[leftIndex...])
            return result
        }

        private static func symmetricDifferenceSorted(
            _ lhs: [UInt16],
            _ rhs: [UInt16]
        ) -> [UInt16] {
            var result: [UInt16] = []
            result.reserveCapacity(lhs.count + rhs.count)
            var leftIndex = 0
            var rightIndex = 0
            while leftIndex < lhs.count, rightIndex < rhs.count {
                let left = lhs[leftIndex]
                let right = rhs[rightIndex]
                if left < right {
                    result.append(left)
                    leftIndex += 1
                } else if left > right {
                    result.append(right)
                    rightIndex += 1
                } else {
                    leftIndex += 1
                    rightIndex += 1
                }
            }
            result.append(contentsOf: lhs[leftIndex...])
            result.append(contentsOf: rhs[rightIndex...])
            return result
        }

        private static func container(
            fromSorted values: [UInt16]
        ) -> Container? {
            guard !values.isEmpty else { return nil }
            if values.count > RoaringBitmap.arrayMaxSize {
                return arrayToBitmap(values)
            }
            return .array(values)
        }

        // MARK: - Set Operations

        /// Intersection (AND)
        static func intersection(_ a: Container, _ b: Container) -> Container? {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                return container(fromSorted: intersectSorted(arrA, arrB))

            case (.array(let arr), .bitmap(let bits)),
                 (.bitmap(let bits), .array(let arr)):
                let result = arr.filter { value in
                    let wordIndex = Int(value) / 64
                    let bitIndex = Int(value) % 64
                    return (bits[wordIndex] & (1 << bitIndex)) != 0
                }
                return result.isEmpty ? nil : .array(result)

            case (.bitmap(let bitsA), .bitmap(let bitsB)):
                var result = [UInt64](repeating: 0, count: 1024)
                var hasAny = false
                for i in 0..<1024 {
                    result[i] = bitsA[i] & bitsB[i]
                    if result[i] != 0 { hasAny = true }
                }
                if !hasAny { return nil }
                let count = result.reduce(0) { $0 + $1.nonzeroBitCount }
                if count <= RoaringBitmap.arrayMaxSize {
                    return bitmapToArray(result)
                }
                return .bitmap(result)

            default:
                return container(
                    fromSorted: intersectSorted(a.toArray(), b.toArray())
                )
            }
        }

        /// Union (OR)
        static func union(_ a: Container, _ b: Container) -> Container {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                let result = unionSorted(arrA, arrB)
                if result.count > RoaringBitmap.arrayMaxSize {
                    return arrayToBitmap(result)
                }
                return .array(result)

            case (.array(let arr), .bitmap(var bits)),
                 (.bitmap(var bits), .array(let arr)):
                for value in arr {
                    let wordIndex = Int(value) / 64
                    let bitIndex = Int(value) % 64
                    bits[wordIndex] |= (1 << bitIndex)
                }
                return .bitmap(bits)

            case (.bitmap(let bitsA), .bitmap(let bitsB)):
                var result = [UInt64](repeating: 0, count: 1024)
                for i in 0..<1024 {
                    result[i] = bitsA[i] | bitsB[i]
                }
                return .bitmap(result)

            default:
                let result = unionSorted(a.toArray(), b.toArray())
                if result.count > RoaringBitmap.arrayMaxSize {
                    return arrayToBitmap(result)
                }
                return .array(result)
            }
        }

        /// Symmetric Difference (XOR)
        ///
        /// Returns elements that are in exactly one of the two containers.
        static func symmetricDifference(_ a: Container, _ b: Container) -> Container? {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                return container(
                    fromSorted: symmetricDifferenceSorted(arrA, arrB)
                )

            case (.array(let arr), .bitmap(let bits)):
                var resultBits = bits
                for value in arr {
                    let wordIndex = Int(value) / 64
                    let bitIndex = Int(value) % 64
                    resultBits[wordIndex] ^= (1 << bitIndex)  // Toggle bit
                }
                let count = resultBits.reduce(0) { $0 + $1.nonzeroBitCount }
                if count == 0 { return nil }
                if count <= RoaringBitmap.arrayMaxSize {
                    return bitmapToArray(resultBits)
                }
                return .bitmap(resultBits)

            case (.bitmap(let bits), .array(let arr)):
                // Same as above, just swap order
                var resultBits = bits
                for value in arr {
                    let wordIndex = Int(value) / 64
                    let bitIndex = Int(value) % 64
                    resultBits[wordIndex] ^= (1 << bitIndex)
                }
                let count = resultBits.reduce(0) { $0 + $1.nonzeroBitCount }
                if count == 0 { return nil }
                if count <= RoaringBitmap.arrayMaxSize {
                    return bitmapToArray(resultBits)
                }
                return .bitmap(resultBits)

            case (.bitmap(let bitsA), .bitmap(let bitsB)):
                var result = [UInt64](repeating: 0, count: 1024)
                var hasAny = false
                for i in 0..<1024 {
                    result[i] = bitsA[i] ^ bitsB[i]
                    if result[i] != 0 { hasAny = true }
                }
                if !hasAny { return nil }
                let count = result.reduce(0) { $0 + $1.nonzeroBitCount }
                if count <= RoaringBitmap.arrayMaxSize {
                    return bitmapToArray(result)
                }
                return .bitmap(result)

            default:
                return container(
                    fromSorted: symmetricDifferenceSorted(
                        a.toArray(),
                        b.toArray()
                    )
                )
            }
        }

        /// Difference (AND NOT)
        ///
        /// Returns elements in `a` that are NOT in `b`.
        ///
        /// **Time Complexity**:
        /// - array-array: O(n + m) using a linear merge
        /// - array-bitmap: O(n) with O(1) bitmap lookup per element
        /// - bitmap-array: O(m) for clearing bits + O(1024) for counting
        /// - bitmap-bitmap: O(1024) bitwise AND NOT
        ///
        /// Reference: Lemire et al., "Roaring Bitmaps", Section 5.3
        static func difference(_ a: Container, _ b: Container) -> Container? {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                return container(fromSorted: subtractSorted(arrA, arrB))

            case (.array(let arr), .bitmap(let bits)):
                let result = arr.filter { value in
                    let wordIndex = Int(value) / 64
                    let bitIndex = Int(value) % 64
                    return (bits[wordIndex] & (1 << bitIndex)) == 0
                }
                return result.isEmpty ? nil : .array(result)

            case (.bitmap(let bits), .array(let arr)):
                var resultBits = bits
                for value in arr {
                    let wordIndex = Int(value) / 64
                    let bitIndex = Int(value) % 64
                    resultBits[wordIndex] &= ~(1 << bitIndex)
                }
                let count = resultBits.reduce(0) { $0 + $1.nonzeroBitCount }
                if count == 0 { return nil }
                if count <= RoaringBitmap.arrayMaxSize {
                    return bitmapToArray(resultBits)
                }
                return .bitmap(resultBits)

            case (.bitmap(let bitsA), .bitmap(let bitsB)):
                var result = [UInt64](repeating: 0, count: 1024)
                var hasAny = false
                for i in 0..<1024 {
                    result[i] = bitsA[i] & ~bitsB[i]
                    if result[i] != 0 { hasAny = true }
                }
                if !hasAny { return nil }
                let count = result.reduce(0) { $0 + $1.nonzeroBitCount }
                if count <= RoaringBitmap.arrayMaxSize {
                    return bitmapToArray(result)
                }
                return .bitmap(result)

            default:
                return container(
                    fromSorted: subtractSorted(a.toArray(), b.toArray())
                )
            }
        }

        /// Convert to array
        func toArray() -> [UInt16] {
            switch self {
            case .array(let arr):
                return arr
            case .bitmap(let bits):
                var arr: [UInt16] = []
                for (wordIndex, word) in bits.enumerated() {
                    var w = word
                    while w != 0 {
                        let bitIndex = w.trailingZeroBitCount
                        arr.append(UInt16(wordIndex * 64 + bitIndex))
                        w &= w - 1
                    }
                }
                return arr
            case .run(let runs):
                return Container.runToArray(runs)
            }
        }

    }

    // MARK: - Initialization

    /// Create an empty bitmap
    public init() {
        self.containers = [:]
    }

    /// Create a bitmap from values
    public init<S: Sequence>(_ values: S) where S.Element == UInt32 {
        var groupedValues: [UInt16: [UInt16]] = [:]
        for value in values {
            let high = UInt16(value >> 16)
            let low = UInt16(value & 0xFFFF)
            groupedValues[high, default: []].append(low)
        }

        self.containers = [:]
        self.containers.reserveCapacity(groupedValues.count)
        for (high, var lowValues) in groupedValues {
            lowValues.sort()
            var uniqueValues: [UInt16] = []
            uniqueValues.reserveCapacity(lowValues.count)
            for value in lowValues where uniqueValues.last != value {
                uniqueValues.append(value)
            }
            if uniqueValues.count > Self.arrayMaxSize {
                containers[high] = Container.arrayToBitmap(uniqueValues)
            } else {
                containers[high] = .array(uniqueValues)
            }
        }
    }

    // MARK: - Basic Operations

    /// Add a value to the bitmap
    public mutating func add(_ value: UInt32) {
        let high = UInt16(value >> 16)
        let low = UInt16(value & 0xFFFF)

        if let existing = containers[high] {
            containers[high] = existing.adding(low)
        } else {
            containers[high] = .array([low])
        }
    }

    /// Remove a value from the bitmap
    public mutating func remove(_ value: UInt32) {
        let high = UInt16(value >> 16)
        let low = UInt16(value & 0xFFFF)

        if let existing = containers[high] {
            containers[high] = existing.removing(low)
        }
    }

    /// Check if a value is in the bitmap
    public func contains(_ value: UInt32) -> Bool {
        let high = UInt16(value >> 16)
        let low = UInt16(value & 0xFFFF)

        return containers[high]?.contains(low) ?? false
    }

    /// Total number of values in the bitmap
    public var cardinality: Int {
        containers.values.reduce(0) { $0 + $1.cardinality }
    }

    /// Check if the bitmap is empty
    public var isEmpty: Bool {
        containers.isEmpty
    }

    /// Returns the retained storage represented by this bitmap.
    ///
    /// The value deliberately models the logical container allocations rather
    /// than relying on allocator-specific capacity details. Request accounting
    /// uses it consistently before and after set operations.
    func retainedStorageByteCount() throws -> UInt64 {
        func adding(_ left: UInt64, _ right: UInt64) throws -> UInt64 {
            let (result, overflow) = left.addingReportingOverflow(right)
            guard !overflow else {
                throw RoaringBitmapFormatError.encodedSizeOverflow
            }
            return result
        }

        func multiplying(_ left: UInt64, _ right: UInt64) throws -> UInt64 {
            let (result, overflow) = left.multipliedReportingOverflow(by: right)
            guard !overflow else {
                throw RoaringBitmapFormatError.encodedSizeOverflow
            }
            return result
        }

        var byteCount = UInt64(MemoryLayout<RoaringBitmap>.stride)
        byteCount = try adding(
            byteCount,
            UInt64(MemoryLayout<[UInt16: Container]>.stride)
        )
        for container in containers.values {
            byteCount = try adding(byteCount, 64)
            switch container {
            case .array(let values):
                byteCount = try adding(
                    byteCount,
                    try multiplying(
                        UInt64(MemoryLayout<UInt16>.stride),
                        UInt64(values.count)
                    )
                )
            case .bitmap(let values):
                byteCount = try adding(
                    byteCount,
                    try multiplying(
                        UInt64(MemoryLayout<UInt64>.stride),
                        UInt64(values.count)
                    )
                )
            case .run(let values):
                byteCount = try adding(
                    byteCount,
                    try multiplying(
                        UInt64(
                            MemoryLayout<(start: UInt16, length: UInt16)>
                                .stride
                        ),
                        UInt64(values.count)
                    )
                )
            }
        }
        return byteCount
    }

    // MARK: - Set Operations

    /// Intersection (AND)
    public static func && (lhs: RoaringBitmap, rhs: RoaringBitmap) -> RoaringBitmap {
        var result = RoaringBitmap()
        for (high, containerA) in lhs.containers {
            if let containerB = rhs.containers[high] {
                if let intersection = Container.intersection(containerA, containerB) {
                    result.containers[high] = intersection
                }
            }
        }
        return result
    }

    /// Union (OR)
    public static func || (lhs: RoaringBitmap, rhs: RoaringBitmap) -> RoaringBitmap {
        var result = lhs
        for (high, containerB) in rhs.containers {
            if let containerA = result.containers[high] {
                result.containers[high] = Container.union(containerA, containerB)
            } else {
                result.containers[high] = containerB
            }
        }
        return result
    }

    /// Difference (AND NOT)
    ///
    /// Returns elements in `lhs` that are NOT in `rhs`.
    /// Uses per-container-type optimized operations.
    ///
    /// **Time Complexity**: O(n) for bitmap containers, O(n + m) for array containers
    public static func - (lhs: RoaringBitmap, rhs: RoaringBitmap) -> RoaringBitmap {
        var result = lhs
        for (high, containerB) in rhs.containers {
            if let containerA = result.containers[high] {
                if let diffContainer = Container.difference(containerA, containerB) {
                    result.containers[high] = diffContainer
                } else {
                    result.containers.removeValue(forKey: high)
                }
            }
        }
        return result
    }

    /// XOR (Symmetric Difference)
    ///
    /// Returns elements that are in either bitmap but not in both.
    ///
    /// **Time Complexity**: O(n + m) where n, m are the sizes of the bitmaps
    ///
    /// **Usage**:
    /// ```swift
    /// let a = RoaringBitmap([1, 2, 3])
    /// let b = RoaringBitmap([2, 3, 4])
    /// let xor = a ^ b  // [1, 4]
    /// ```
    public static func ^ (lhs: RoaringBitmap, rhs: RoaringBitmap) -> RoaringBitmap {
        var result = lhs
        for (high, containerB) in rhs.containers {
            if let containerA = result.containers[high] {
                if let xorContainer = Container.symmetricDifference(
                    containerA,
                    containerB
                ) {
                    result.containers[high] = xorContainer
                } else {
                    result.containers.removeValue(forKey: high)
                }
            } else {
                result.containers[high] = containerB
            }
        }
        return result
    }

    /// Complement/NOT within a universe
    ///
    /// Returns all values in [0, universeSize) that are NOT in this bitmap.
    ///
    /// **Note**: The universe size must be specified since RoaringBitmap doesn't
    /// track a fixed universe. Values outside [0, universeSize) are ignored.
    ///
    /// **Time Complexity**: O(universeSize / 65536 + n)
    ///
    /// **Memory**: May create many containers for large universes
    ///
    /// **Usage**:
    /// ```swift
    /// let bitmap = RoaringBitmap([1, 3, 5])
    /// let complement = bitmap.complement(universeSize: 10)  // [0, 2, 4, 6, 7, 8, 9]
    /// ```
    ///
    /// - Parameter universeSize: The size of the universe (exclusive upper bound)
    /// - Returns: A bitmap containing all values in [0, universeSize) not in this bitmap
    public func complement(universeSize: UInt32) -> RoaringBitmap {
        // Create a full universe bitmap and subtract self
        let universe = RoaringBitmap.range(0..<universeSize)
        return universe - self
    }

    /// Create a bitmap containing all values in a range
    ///
    /// **Time Complexity**: O(range.count / 65536)
    ///
    /// **Usage**:
    /// ```swift
    /// let bitmap = RoaringBitmap.range(0..<100)  // [0, 1, 2, ..., 99]
    /// ```
    ///
    /// - Parameter range: The range of values to include
    /// - Returns: A bitmap containing all values in the range
    public static func range(_ range: Range<UInt32>) -> RoaringBitmap {
        var result = RoaringBitmap()

        guard !range.isEmpty else { return result }

        let startHigh = UInt16(range.lowerBound >> 16)
        let endHigh = UInt16((range.upperBound - 1) >> 16)

        for high in startHigh...endHigh {
            let containerStart: UInt16
            let containerEnd: UInt16

            if high == startHigh {
                containerStart = UInt16(range.lowerBound & 0xFFFF)
            } else {
                containerStart = 0
            }

            if high == endHigh {
                containerEnd = UInt16((range.upperBound - 1) & 0xFFFF)
            } else {
                containerEnd = 0xFFFF
            }

            // Create container for this range
            let count = Int(containerEnd) - Int(containerStart) + 1
            if count == 65536 {
                // Full container - use run or bitmap
                result.containers[high] = .run([(start: 0, length: 65535)])
            } else if count > Self.arrayMaxSize {
                // Large range - use run-length encoding
                result.containers[high] = .run([(start: containerStart, length: UInt16(containerEnd - containerStart))])
            } else {
                // Small range - use array
                var arr: [UInt16] = []
                arr.reserveCapacity(count)
                for i in containerStart...containerEnd {
                    arr.append(i)
                }
                result.containers[high] = .array(arr)
            }
        }

        return result
    }

    // MARK: - Iteration

    public struct Iterator: IteratorProtocol {
        private let containers: [(key: UInt16, value: Container)]
        private var containerIndex = 0
        private var valueIterator: ContainerIterator?

        fileprivate init(containers: [UInt16: Container]) {
            self.containers = containers.sorted { $0.key < $1.key }
        }

        public mutating func next() -> UInt32? {
            while true {
                if var iterator = valueIterator {
                    if let value = iterator.next() {
                        valueIterator = iterator
                        return value
                    }
                    valueIterator = nil
                }

                guard containerIndex < containers.count else {
                    return nil
                }
                let entry = containers[containerIndex]
                containerIndex += 1
                valueIterator = ContainerIterator(
                    high: entry.key,
                    container: entry.value
                )
            }
        }
    }

    public func makeIterator() -> Iterator {
        Iterator(containers: containers)
    }

    /// Get all values as an array
    public func toArray() -> [UInt32] {
        var result: [UInt32] = []
        result.reserveCapacity(cardinality)
        for value in self {
            result.append(value)
        }
        return result
    }

    /// Iterate over all values
    public func forEach(_ body: (UInt32) -> Void) {
        for value in self {
            body(value)
        }
    }

    // MARK: - Serialization

    /// Returns the deterministic persisted representation.
    ///
    /// **Binary Format**:
    /// - Header: `RBM`, UInt8 format version, UInt32 container count
    /// - For each container:
    ///   - UInt16 high part
    ///   - UInt8 container type (0=array, 1=bitmap, 2=run)
    ///   - Container data (format depends on type)
    public func serializedBytes() throws(RoaringBitmapFormatError) -> ByteString {
        let sortedContainers = containers.sorted { $0.key < $1.key }
        var byteCount = 8

        func adding(_ amount: Int) throws(RoaringBitmapFormatError) {
            let (sum, overflow) = byteCount.addingReportingOverflow(amount)
            guard !overflow else {
                throw .encodedSizeOverflow
            }
            byteCount = sum
        }

        for (_, container) in sortedContainers {
            try adding(3)
            switch container {
            case .array(let values):
                let (payloadByteCount, overflow) = values.count.multipliedReportingOverflow(by: 2)
                guard !overflow else {
                    throw .encodedSizeOverflow
                }
                try adding(4)
                try adding(payloadByteCount)
            case .bitmap(let words):
                guard words.count == 1024 else {
                    throw .invalidBitmapWordCount(words.count)
                }
                try adding(1024 * 8)
            case .run(let runs):
                let (payloadByteCount, overflow) = runs.count.multipliedReportingOverflow(by: 4)
                guard !overflow else {
                    throw .encodedSizeOverflow
                }
                try adding(4)
                try adding(payloadByteCount)
            }
        }

        return ByteString.copying(count: byteCount) { output in
            var encoder = RoaringBitmapRepresentationEncoder(output: output)
            encoder.write(UInt8(0x52))
            encoder.write(UInt8(0x42))
            encoder.write(UInt8(0x4D))
            encoder.write(UInt8(1))
            encoder.write(UInt32(sortedContainers.count))
            for (high, container) in sortedContainers {
                encoder.write(high)
                switch container {
                case .array(let values):
                    encoder.write(UInt8(0))
                    encoder.write(UInt32(values.count))
                    for value in values {
                        encoder.write(value)
                    }
                case .bitmap(let words):
                    encoder.write(UInt8(1))
                    for word in words {
                        encoder.write(word)
                    }
                case .run(let runs):
                    encoder.write(UInt8(2))
                    encoder.write(UInt32(runs.count))
                    for run in runs {
                        encoder.write(run.start)
                        encoder.write(run.length)
                    }
                }
            }
            precondition(encoder.offset == output.count)
        }
    }

    /// Creates a bitmap by borrowing the persisted bytes for the duration of decoding.
    public init(serializedBytes bytes: ByteString) throws(RoaringBitmapFormatError) {
        let decoded: Result<RoaringBitmap, RoaringBitmapFormatError> = bytes.withUnsafeBytes { input in
            var decoder = RoaringBitmapRepresentationDecoder(input: input)
            do {
                return .success(try decoder.decodeBitmap())
            } catch let error as RoaringBitmapFormatError {
                return .failure(error)
            } catch {
                preconditionFailure("Roaring bitmap decoding threw an unexpected error type")
            }
        }
        switch decoded {
        case .success(let bitmap):
            self = bitmap
        case .failure(let error):
            throw error
        }
    }
}

private struct ContainerIterator {
    let highPart: UInt32
    let container: RoaringBitmap.Container
    private var arrayIndex = 0
    private var bitmapWordIndex = 0
    private var remainingBitmapWord: UInt64 = 0
    private var runIndex = 0
    private var runOffset: UInt32 = 0

    init(high: UInt16, container: RoaringBitmap.Container) {
        self.highPart = UInt32(high) << 16
        self.container = container
    }

    mutating func next() -> UInt32? {
        switch container {
        case .array(let values):
            guard arrayIndex < values.count else {
                return nil
            }
            let value = highPart | UInt32(values[arrayIndex])
            arrayIndex += 1
            return value

        case .bitmap(let words):
            while true {
                if remainingBitmapWord != 0 {
                    let bitIndex = remainingBitmapWord.trailingZeroBitCount
                    remainingBitmapWord &= remainingBitmapWord - 1
                    return highPart | UInt32((bitmapWordIndex - 1) * 64 + bitIndex)
                }
                guard bitmapWordIndex < words.count else {
                    return nil
                }
                remainingBitmapWord = words[bitmapWordIndex]
                bitmapWordIndex += 1
            }

        case .run(let runs):
            guard runIndex < runs.count else {
                return nil
            }
            let run = runs[runIndex]
            let value = highPart | UInt32(run.start) + runOffset
            if runOffset == UInt32(run.length) {
                runIndex += 1
                runOffset = 0
            } else {
                runOffset += 1
            }
            return value
        }
    }
}

// `output` is the final allocation owned by `ByteString.copying`. The encoder exists
// only inside that synchronous initialization closure, so the pointer cannot
// escape and every byte in the allocation is initialized exactly once.
private struct RoaringBitmapRepresentationEncoder {
    let output: UnsafeMutableRawBufferPointer
    private(set) var offset = 0

    mutating func write(_ value: UInt8) {
        output[offset] = value
        offset += 1
    }

    mutating func write(_ value: UInt16) {
        write(UInt8(truncatingIfNeeded: value))
        write(UInt8(truncatingIfNeeded: value >> 8))
    }

    mutating func write(_ value: UInt32) {
        write(UInt8(truncatingIfNeeded: value))
        write(UInt8(truncatingIfNeeded: value >> 8))
        write(UInt8(truncatingIfNeeded: value >> 16))
        write(UInt8(truncatingIfNeeded: value >> 24))
    }

    mutating func write(_ value: UInt64) {
        write(UInt32(truncatingIfNeeded: value))
        write(UInt32(truncatingIfNeeded: value >> 32))
    }
}

// `input` is borrowed from the `ByteString` argument and this decoder is created and
// consumed entirely inside `withUnsafeBytes`. Bounds are checked before every
// load, unaligned loads are explicit, and the pointer never crosses suspension
// or Sendable boundaries.
private struct RoaringBitmapRepresentationDecoder {
    let input: UnsafeRawBufferPointer
    private(set) var offset = 0

    mutating func decodeBitmap() throws(RoaringBitmapFormatError) -> RoaringBitmap {
        let signature0 = try readUInt8()
        let signature1 = try readUInt8()
        let signature2 = try readUInt8()
        guard signature0 == 0x52, signature1 == 0x42, signature2 == 0x4D else {
            throw .invalidSignature
        }
        let formatVersion = try readUInt8()
        guard formatVersion == 1 else {
            throw .unsupportedFormatVersion(formatVersion)
        }

        let containerCount = try readUInt32()
        guard containerCount <= UInt32(UInt16.max) + 1 else {
            throw .invalidContainerCount(containerCount)
        }

        var bitmap = RoaringBitmap()
        for _ in 0..<containerCount {
            let high = try readUInt16()
            guard bitmap.containers[high] == nil else {
                throw .duplicateContainer(high)
            }

            switch try readUInt8() {
            case 0:
                bitmap.containers[high] = try readArrayContainer(high: high)
            case 1:
                bitmap.containers[high] = try readBitmapContainer(high: high)
            case 2:
                bitmap.containers[high] = try readRunContainer(high: high)
            case let type:
                throw .unknownContainerType(type)
            }
        }

        guard offset == input.count else {
            throw .trailingBytes(input.count - offset)
        }
        return bitmap
    }

    private mutating func readArrayContainer(
        high: UInt16
    ) throws(RoaringBitmapFormatError) -> RoaringBitmap.Container {
        let count = try readUInt32()
        guard count > 0, count <= UInt32(UInt16.max) + 1 else {
            if count == 0 {
                throw .emptyContainer(high)
            }
            throw .invalidArrayCount(count)
        }
        try require(Int(count) * 2)

        var values: [UInt16] = []
        values.reserveCapacity(Int(count))
        var previous: UInt16?
        for _ in 0..<count {
            let value = readAvailableUInt16()
            if let previous, value <= previous {
                throw .unorderedArray(container: high)
            }
            values.append(value)
            previous = value
        }
        return .array(values)
    }

    private mutating func readBitmapContainer(
        high: UInt16
    ) throws(RoaringBitmapFormatError) -> RoaringBitmap.Container {
        try require(1024 * 8)
        var words: [UInt64] = []
        words.reserveCapacity(1024)
        var containsValue = false
        for _ in 0..<1024 {
            let word = readAvailableUInt64()
            containsValue = containsValue || word != 0
            words.append(word)
        }
        guard containsValue else {
            throw .emptyContainer(high)
        }
        return .bitmap(words)
    }

    private mutating func readRunContainer(
        high: UInt16
    ) throws(RoaringBitmapFormatError) -> RoaringBitmap.Container {
        let count = try readUInt32()
        guard count > 0, count <= UInt32(UInt16.max) + 1 else {
            if count == 0 {
                throw .emptyContainer(high)
            }
            throw .invalidRunCount(count)
        }
        try require(Int(count) * 4)

        var runs: [(start: UInt16, length: UInt16)] = []
        runs.reserveCapacity(Int(count))
        var previousEnd: Int?
        for _ in 0..<count {
            let start = readAvailableUInt16()
            let length = readAvailableUInt16()
            let end = Int(start) + Int(length)
            guard end <= Int(UInt16.max), previousEnd.map({ Int(start) > $0 }) ?? true else {
                throw .invalidRun(container: high)
            }
            runs.append((start, length))
            previousEnd = end
        }
        return .run(runs)
    }

    private mutating func readUInt8() throws(RoaringBitmapFormatError) -> UInt8 {
        try require(1)
        let value = input[offset]
        offset += 1
        return value
    }

    private mutating func readUInt16() throws(RoaringBitmapFormatError) -> UInt16 {
        try require(2)
        return readAvailableUInt16()
    }

    private mutating func readUInt32() throws(RoaringBitmapFormatError) -> UInt32 {
        try require(4)
        let value = input.loadUnaligned(fromByteOffset: offset, as: UInt32.self)
        offset += 4
        return UInt32(littleEndian: value)
    }

    private mutating func readAvailableUInt16() -> UInt16 {
        let value = input.loadUnaligned(fromByteOffset: offset, as: UInt16.self)
        offset += 2
        return UInt16(littleEndian: value)
    }

    private mutating func readAvailableUInt64() -> UInt64 {
        let value = input.loadUnaligned(fromByteOffset: offset, as: UInt64.self)
        offset += 8
        return UInt64(littleEndian: value)
    }

    private func require(_ byteCount: Int) throws(RoaringBitmapFormatError) {
        let available = input.count - offset
        guard byteCount <= available else {
            throw .truncated(
                offset: offset,
                requiredByteCount: byteCount,
                availableByteCount: available
            )
        }
    }
}
