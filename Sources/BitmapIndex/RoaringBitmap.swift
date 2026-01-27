// RoaringBitmap.swift
// BitmapIndex - Roaring Bitmap implementation for efficient set operations
//
// Reference: Lemire et al., "Roaring Bitmaps: Implementation of an Optimized
// Software Library", Software: Practice and Experience, 2016
// https://arxiv.org/abs/1603.06549

import Foundation

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
/// - Cardinality: O(containers) with cached counts
/// - Memory: Adaptive based on density
public struct RoaringBitmap: Sendable, Codable, Equatable {
    /// Container threshold: switch from array to bitmap at 4096 elements
    private static let arrayMaxSize = 4096

    /// Containers indexed by high 16 bits
    private var containers: [UInt16: Container]

    /// Container storage types
    enum Container: Sendable, Codable, Equatable {
        case array([UInt16])           // Sorted array of low 16 bits
        case bitmap([UInt64])          // 1024 × 64-bit words
        case run([(start: UInt16, length: UInt16)])  // Run-length encoded

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

        /// Check if value is in container
        func contains(_ value: UInt16) -> Bool {
            switch self {
            case .array(let arr):
                return arr.contains(value)
            case .bitmap(let bits):
                let wordIndex = Int(value) / 64
                let bitIndex = Int(value) % 64
                return (bits[wordIndex] & (1 << bitIndex)) != 0
            case .run(let runs):
                for (start, length) in runs {
                    if value >= start && value <= start &+ length {
                        return true
                    }
                    if value < start {
                        return false
                    }
                }
                return false
            }
        }

        /// Add value to container, return new container (may change type)
        func adding(_ value: UInt16) -> Container {
            switch self {
            case .array(var arr):
                if arr.contains(value) {
                    return .array(arr)
                }
                arr.append(value)
                arr.sort()
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
                // Simplified: convert to array, add, reconvert if needed
                var arr = Container.runToArray(runs)
                if !arr.contains(value) {
                    arr.append(value)
                    arr.sort()
                }
                if arr.count > RoaringBitmap.arrayMaxSize {
                    return Container.arrayToBitmap(arr)
                }
                return .array(arr)
            }
        }

        /// Remove value from container, return new container (may change type)
        func removing(_ value: UInt16) -> Container? {
            switch self {
            case .array(var arr):
                arr.removeAll { $0 == value }
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
                var arr = Container.runToArray(runs)
                arr.removeAll { $0 == value }
                return arr.isEmpty ? nil : .array(arr)
            }
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
            for (start, length) in runs {
                for i in 0...length {
                    arr.append(start &+ i)
                }
            }
            return arr
        }

        // MARK: - Set Operations

        /// Intersection (AND)
        static func intersection(_ a: Container, _ b: Container) -> Container? {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                let setB = Set(arrB)
                let result = arrA.filter { setB.contains($0) }
                return result.isEmpty ? nil : .array(result)

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
                // Convert runs to arrays and intersect
                let arrA = a.toArray()
                let setB = Set(b.toArray())
                let result = arrA.filter { setB.contains($0) }
                return result.isEmpty ? nil : .array(result)
            }
        }

        /// Union (OR)
        static func union(_ a: Container, _ b: Container) -> Container {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                var result = Set(arrA)
                result.formUnion(arrB)
                let sorted = result.sorted()
                if sorted.count > RoaringBitmap.arrayMaxSize {
                    return arrayToBitmap(sorted)
                }
                return .array(sorted)

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
                let arrA = a.toArray()
                let arrB = b.toArray()
                var result = Set(arrA)
                result.formUnion(arrB)
                let sorted = result.sorted()
                if sorted.count > RoaringBitmap.arrayMaxSize {
                    return arrayToBitmap(sorted)
                }
                return .array(sorted)
            }
        }

        /// Symmetric Difference (XOR)
        ///
        /// Returns elements that are in exactly one of the two containers.
        static func symmetricDifference(_ a: Container, _ b: Container) -> Container? {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                let setA = Set(arrA)
                let setB = Set(arrB)
                let result = setA.symmetricDifference(setB).sorted()
                return result.isEmpty ? nil : .array(result)

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
                // Convert runs to arrays and compute XOR
                let setA = Set(a.toArray())
                let setB = Set(b.toArray())
                let result = setA.symmetricDifference(setB).sorted()
                if result.isEmpty { return nil }
                if result.count > RoaringBitmap.arrayMaxSize {
                    return arrayToBitmap(result)
                }
                return .array(result)
            }
        }

        /// Difference (AND NOT)
        ///
        /// Returns elements in `a` that are NOT in `b`.
        ///
        /// **Time Complexity**:
        /// - array-array: O(n + m) using Set lookup
        /// - array-bitmap: O(n) with O(1) bitmap lookup per element
        /// - bitmap-array: O(m) for clearing bits + O(1024) for counting
        /// - bitmap-bitmap: O(1024) bitwise AND NOT
        ///
        /// Reference: Lemire et al., "Roaring Bitmaps", Section 5.3
        static func difference(_ a: Container, _ b: Container) -> Container? {
            switch (a, b) {
            case (.array(let arrA), .array(let arrB)):
                let setB = Set(arrB)
                let result = arrA.filter { !setB.contains($0) }
                return result.isEmpty ? nil : .array(result)

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
                // Convert runs to arrays and compute difference
                let arrA = a.toArray()
                let setB = Set(b.toArray())
                let result = arrA.filter { !setB.contains($0) }
                return result.isEmpty ? nil : .array(result)
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

        // MARK: - Codable

        enum CodingKeys: String, CodingKey {
            case type, data
        }

        init(from decoder: Decoder) throws {
            let container = try decoder.container(keyedBy: CodingKeys.self)
            let type = try container.decode(String.self, forKey: .type)

            switch type {
            case "array":
                let arr = try container.decode([UInt16].self, forKey: .data)
                self = .array(arr)
            case "bitmap":
                let bits = try container.decode([UInt64].self, forKey: .data)
                self = .bitmap(bits)
            case "run":
                let runs = try container.decode([[UInt16]].self, forKey: .data)
                self = .run(runs.map { ($0[0], $0[1]) })
            default:
                throw DecodingError.dataCorruptedError(forKey: .type, in: container, debugDescription: "Unknown container type")
            }
        }

        func encode(to encoder: Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)

            switch self {
            case .array(let arr):
                try container.encode("array", forKey: .type)
                try container.encode(arr, forKey: .data)
            case .bitmap(let bits):
                try container.encode("bitmap", forKey: .type)
                try container.encode(bits, forKey: .data)
            case .run(let runs):
                try container.encode("run", forKey: .type)
                try container.encode(runs.map { [$0.start, $0.length] }, forKey: .data)
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
        self.containers = [:]
        for value in values {
            add(value)
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
        var result = RoaringBitmap()

        // Get all unique high parts
        let allHighParts = Set(lhs.containers.keys).union(rhs.containers.keys)

        for high in allHighParts {
            let containerA = lhs.containers[high]
            let containerB = rhs.containers[high]

            switch (containerA, containerB) {
            case (.some(let a), .some(let b)):
                // Both have this container - compute XOR
                if let xorContainer = Container.symmetricDifference(a, b) {
                    result.containers[high] = xorContainer
                }
            case (.some(let a), .none):
                // Only in lhs
                result.containers[high] = a
            case (.none, .some(let b)):
                // Only in rhs
                result.containers[high] = b
            case (.none, .none):
                // Neither (shouldn't happen)
                break
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

    /// Get all values as an array
    public func toArray() -> [UInt32] {
        var result: [UInt32] = []
        for (high, container) in containers.sorted(by: { $0.key < $1.key }) {
            let highPart = UInt32(high) << 16
            for low in container.toArray() {
                result.append(highPart | UInt32(low))
            }
        }
        return result
    }

    /// Iterate over all values
    public func forEach(_ body: (UInt32) -> Void) {
        for (high, container) in containers.sorted(by: { $0.key < $1.key }) {
            let highPart = UInt32(high) << 16
            for low in container.toArray() {
                body(highPart | UInt32(low))
            }
        }
    }

    // MARK: - Serialization

    /// Serialize to bytes
    public func serialize() throws -> Data {
        let encoder = JSONEncoder()
        return try encoder.encode(self)
    }

    /// Deserialize from bytes
    public static func deserialize(_ data: Data) throws -> RoaringBitmap {
        let decoder = JSONDecoder()
        return try decoder.decode(RoaringBitmap.self, from: data)
    }
}
