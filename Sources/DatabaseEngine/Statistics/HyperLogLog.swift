// HyperLogLog.swift
// Statistics - Probabilistic cardinality estimation

import Foundation

/// HyperLogLog cardinality estimator
///
/// Provides approximate distinct value counting with ~2% standard error
/// using only ~16KB of memory, regardless of cardinality.
///
/// **Usage**:
/// ```swift
/// var hll = HyperLogLog()
/// hll.add("value1")
/// hll.add("value2")
/// hll.add("value1")  // duplicate
/// print(hll.cardinality())  // ~2
/// ```
///
/// **Accuracy**: ±2% standard error for cardinalities > 1000
/// **Memory**: 16,384 bytes (16KB)
public struct HyperLogLog: Sendable, Codable, Equatable {

    // MARK: - Constants

    /// Precision parameter (2^14 = 16,384 registers)
    private static let precision: Int = 14

    /// Number of registers
    private static let registerCount: Int = 1 << precision

    /// Mask for extracting register index
    private static let registerMask: UInt64 = UInt64(registerCount - 1)

    /// Alpha constant for harmonic mean correction
    private static var alpha: Double {
        // Alpha values for different precisions (standard HyperLogLog)
        // For precision 14: alpha ≈ 0.7213 / (1 + 1.079 / 16384)
        0.7213 / (1.0 + 1.079 / Double(registerCount))
    }

    // MARK: - Storage

    /// Register array storing maximum leading zeros observed
    private var registers: [UInt8]

    // MARK: - Initialization

    /// Create an empty HyperLogLog estimator
    public init() {
        self.registers = [UInt8](repeating: 0, count: Self.registerCount)
    }

    /// Create from existing register data
    internal init(registers: [UInt8]) {
        precondition(registers.count == Self.registerCount)
        self.registers = registers
    }

    // MARK: - Adding Values

    /// Add a hashable value to the estimator
    public mutating func add<T: Hashable>(_ value: T) {
        let hash = stableHash(value)
        addHash(hash)
    }

    /// Add a string value
    public mutating func add(_ value: String) {
        let hash = fnv1aHash(value)
        addHash(hash)
    }

    /// Add an integer value
    public mutating func add(_ value: Int) {
        let hash = fnv1aHash(value)
        addHash(hash)
    }

    /// Add an Int64 value
    public mutating func add(_ value: Int64) {
        let hash = fnv1aHash(value)
        addHash(hash)
    }

    /// Add a pre-computed hash value
    private mutating func addHash(_ hash: UInt64) {
        // Use lower bits for register index
        let registerIndex = Int(hash & Self.registerMask)

        // Use remaining bits to count leading zeros
        let remainingBits = hash >> Self.precision
        let leadingZeros = UInt8(remainingBits.leadingZeroBitCount - Self.precision + 1)

        // Keep the maximum
        registers[registerIndex] = max(registers[registerIndex], leadingZeros)
    }

    // MARK: - Cardinality Estimation

    /// Estimate the number of distinct values added
    ///
    /// Returns the estimated cardinality with ~2% standard error.
    public func cardinality() -> Int64 {
        // Calculate harmonic mean of 2^(-register)
        var harmonicSum: Double = 0
        var zeroCount = 0

        for register in registers {
            if register == 0 {
                zeroCount += 1
            }
            harmonicSum += pow(2.0, -Double(register))
        }

        // Raw estimate using harmonic mean
        let m = Double(Self.registerCount)
        var estimate = Self.alpha * m * m / harmonicSum

        // Apply corrections for different ranges
        estimate = applyCorrections(estimate: estimate, zeroCount: zeroCount)

        return Int64(estimate.rounded())
    }

    /// Apply small and large range corrections
    private func applyCorrections(estimate: Double, zeroCount: Int) -> Double {
        let m = Double(Self.registerCount)

        // Small range correction (linear counting)
        if estimate <= 2.5 * m && zeroCount > 0 {
            // Use linear counting for small cardinalities
            return m * log(m / Double(zeroCount))
        }

        // Large range correction (hash collision)
        let twoTo32: Double = pow(2.0, 32)
        if estimate > twoTo32 / 30.0 {
            return -twoTo32 * log(1.0 - estimate / twoTo32)
        }

        return estimate
    }

    // MARK: - Merging

    /// Merge another HyperLogLog into this one
    ///
    /// After merging, this estimator represents the union of both sets.
    public mutating func merge(with other: HyperLogLog) {
        for i in 0..<Self.registerCount {
            registers[i] = max(registers[i], other.registers[i])
        }
    }

    /// Create a merged HyperLogLog from two estimators
    public func merged(with other: HyperLogLog) -> HyperLogLog {
        var result = self
        result.merge(with: other)
        return result
    }

    // MARK: - Utility

    /// Clear all registers
    public mutating func clear() {
        registers = [UInt8](repeating: 0, count: Self.registerCount)
    }

    /// Check if empty (no values added)
    public var isEmpty: Bool {
        registers.allSatisfy { $0 == 0 }
    }

    /// Serialized size in bytes
    public static var serializedSize: Int {
        registerCount
    }

    // MARK: - Hash Functions

    /// FNV-1a hash for strings
    private func fnv1aHash(_ value: String) -> UInt64 {
        var hash: UInt64 = 0xcbf29ce484222325  // FNV offset basis
        let prime: UInt64 = 0x100000001b3       // FNV prime

        for byte in value.utf8 {
            hash ^= UInt64(byte)
            hash = hash &* prime
        }

        return hash
    }

    /// FNV-1a hash for integers
    private func fnv1aHash(_ value: Int) -> UInt64 {
        fnv1aHash(Int64(value))
    }

    /// FNV-1a hash for Int64
    private func fnv1aHash(_ value: Int64) -> UInt64 {
        var hash: UInt64 = 0xcbf29ce484222325
        let prime: UInt64 = 0x100000001b3

        var v = UInt64(bitPattern: value)
        for _ in 0..<8 {
            hash ^= v & 0xFF
            hash = hash &* prime
            v >>= 8
        }

        return hash
    }

    /// Stable hash for any Hashable type
    private func stableHash<T: Hashable>(_ value: T) -> UInt64 {
        // Use string representation for stability across runs
        let str = String(describing: value)
        return fnv1aHash(str)
    }
}

// MARK: - Codable Implementation

extension HyperLogLog {
    private enum CodingKeys: String, CodingKey {
        case registers
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let data = try container.decode(Data.self, forKey: .registers)
        self.registers = [UInt8](data)

        guard registers.count == Self.registerCount else {
            throw DecodingError.dataCorruptedError(
                forKey: .registers,
                in: container,
                debugDescription: "Invalid register count: \(registers.count), expected \(Self.registerCount)"
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(Data(registers), forKey: .registers)
    }
}
