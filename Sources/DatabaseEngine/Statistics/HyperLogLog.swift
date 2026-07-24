import DatabaseTypes
#if canImport(Darwin)
import Darwin
#elseif canImport(Glibc)
import Glibc
#elseif canImport(WASILibc)
import WASILibc
#endif
import DatabaseValue

/// HyperLogLog cardinality estimator
///
/// Memory-efficient probabilistic cardinality estimation using the HyperLogLog algorithm.
/// The default estimator uses 16 KiB in memory and can be persisted in a
/// 12 KiB six-bit register frame with approximately 0.81% standard error.
///
/// **Algorithm**:
/// HyperLogLog works by hashing each element and observing the pattern of leading zeros
/// in the hash. The maximum number of leading zeros observed is used to estimate the
/// cardinality. Multiple registers (2^14 = 16,384) are used for accuracy.
///
/// **Usage**:
/// ```swift
/// var hll = HyperLogLog()
///
/// // Add values
/// for user in users {
///     try hll.add(.string(user.email))
/// }
///
/// // Get estimated cardinality
/// let uniqueCount = try hll.cardinality()
/// print("Estimated unique emails: \(uniqueCount)")
///
/// // Merge multiple estimators
/// var hll2 = HyperLogLog()
/// // ... add values to hll2
/// hll.merge(hll2)
/// ```
///
/// Persistence belongs to the hosting runtime's bounded binary codec. JSON is
/// not a canonical database storage representation.
///
/// **References**:
/// - P. Flajolet et al., "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm"
/// - http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
public struct HyperLogLog: Sendable, Codable, Hashable {
    public static let supportedPrecision = 4...18

    // MARK: - Properties

    /// Number of register-index bits.
    public let precision: Int

    /// Registers storing the maximum number of leading zeros + 1 for each bucket
    /// Each register stores values 0-63 (6 bits), but we use UInt8 for simplicity
    private var registers: [UInt8]

    // MARK: - Initialization

    /// Initialize a new HyperLogLog estimator
    public init() {
        self.precision = 14
        self.registers = Array(repeating: 0, count: 1 << precision)
    }

    /// Initialize an estimator with an explicit precision.
    public init(precision: Int) throws(HyperLogLogError) {
        guard Self.supportedPrecision.contains(precision) else {
            throw .invalidPrecision(precision)
        }
        self.precision = precision
        self.registers = Array(repeating: 0, count: 1 << precision)
    }

    /// Restores an estimator from its exact register state without an
    /// additional array copy. The caller transfers the array's value ownership.
    public init(
        precision: Int,
        registers: [UInt8]
    ) throws(HyperLogLogError) {
        guard Self.supportedPrecision.contains(precision) else {
            throw .invalidPrecision(precision)
        }
        let expectedCount = 1 << precision
        guard registers.count == expectedCount else {
            throw .invalidRegisterCount(
                expected: expectedCount,
                actual: registers.count
            )
        }
        let maximum = UInt8(65 - precision)
        for (index, register) in registers.enumerated() where register > maximum {
            throw .invalidRegisterValue(
                index: index,
                value: register,
                maximum: maximum
            )
        }
        self.precision = precision
        self.registers = registers
    }

    // MARK: - Public API

    /// Add a value to the estimator
    ///
    /// The value is hashed and the hash is used to update the appropriate register.
    ///
    /// - Parameter value: The value to add
    public mutating func add(
        _ value: FieldValue
    ) throws(RDFTermCodecError) {
        let hash = try value.stableHash()
        addHash(hash)
    }

    /// Add a pre-computed hash to the estimator
    ///
    /// Useful when you already have a hash value from another source.
    ///
    /// - Parameter hash: 64-bit hash value
    public mutating func addHash(_ hash: UInt64) {
        // Use lower bits for register index
        let indexMask: UInt64 = (1 << precision) - 1
        let registerIndex = Int(hash & indexMask)

        // Count leading zeros in remaining bits (upper 50 bits after using 14 for index)
        let remainingBits = hash >> precision
        let effectiveBits = 64 - precision

        let leadingZeros: Int
        if remainingBits == 0 {
            // All remaining bits are zero
            leadingZeros = effectiveBits
        } else {
            // leadingZeroBitCount counts from MSB of 64-bit value
            // After right shift by indexBits, the upper indexBits positions are 0
            // We need leading zeros within the effective 50 bits
            leadingZeros = remainingBits.leadingZeroBitCount - precision
        }

        // rho(w) = position of leftmost 1-bit, which is leadingZeros + 1
        // Clamp to maximum value that fits in UInt8, minimum 1
        let rho = UInt8(min(max(leadingZeros + 1, 1), 255))

        // Update register with maximum
        registers[registerIndex] = max(registers[registerIndex], rho)
    }

    /// Estimate the cardinality (number of distinct elements)
    ///
    /// - Returns: Estimated number of distinct elements added
    public func cardinality() throws(HyperLogLogError) -> Int64 {
        // Raw HyperLogLog estimate: alpha * m^2 / sum(2^(-M[j]))
        let harmonicMean = registers.reduce(0.0) { sum, register in
            sum + pow(2.0, -Double(register))
        }

        let m = Double(registers.count)
        let alpha = 0.7213 / (1.0 + 1.079 / m)
        var estimate = alpha * m * m / harmonicMean

        // Small range correction (linear counting)
        // When estimate < 2.5 * m, use linear counting for better accuracy
        if estimate <= 2.5 * m {
            // Count registers that are still zero
            var zeroCount = 0
            for register in registers where register == 0 {
                zeroCount += 1
            }
            if zeroCount > 0 {
                // Linear counting: m * ln(m / V) where V = number of zero registers
                estimate = m * log(m / Double(zeroCount))
            }
        }

        // Large-range correction must use the complete 64-bit hash domain.
        // Applying the historical 32-bit correction here can produce NaN and
        // trap during integer conversion for a valid 64-bit register state.
        let hashSpace = 18_446_744_073_709_551_616.0
        let threshold = hashSpace / 30.0
        if estimate > threshold {
            guard estimate < hashSpace else {
                throw .cardinalityOutOfRange
            }
            estimate = -hashSpace * log1p(-estimate / hashSpace)
        }

        guard estimate.isFinite,
              estimate >= 0,
              let cardinality = Int64(exactly: estimate.rounded()) else {
            throw .cardinalityOutOfRange
        }
        return cardinality
    }

    /// Merge another HyperLogLog estimator into this one
    ///
    /// After merging, this estimator will contain the union of both sets.
    /// The merged cardinality will be approximately the cardinality of
    /// the union of elements from both estimators.
    ///
    /// - Parameter other: Another HyperLogLog estimator
    public mutating func merge(
        _ other: HyperLogLog
    ) throws(HyperLogLogError) {
        guard precision == other.precision else {
            throw .precisionMismatch(
                expected: precision,
                actual: other.precision
            )
        }

        for i in registers.indices {
            registers[i] = max(registers[i], other.registers[i])
        }
    }

    /// Create a new HyperLogLog by merging two estimators
    ///
    /// - Parameters:
    ///   - lhs: First estimator
    ///   - rhs: Second estimator
    /// - Returns: New estimator containing the union
    public static func merged(
        _ lhs: HyperLogLog,
        _ rhs: HyperLogLog
    ) throws(HyperLogLogError) -> HyperLogLog {
        var result = lhs
        try result.merge(rhs)
        return result
    }

    /// Reset the estimator to empty state
    public mutating func reset() {
        registers = Array(repeating: 0, count: 1 << precision)
    }

    /// Check if the estimator is empty (no elements added)
    public var isEmpty: Bool {
        registers.allSatisfy { $0 == 0 }
    }

    // MARK: - Statistics

    /// Get the estimated relative error
    ///
    /// For HyperLogLog with 16,384 registers, the standard error is approximately 0.81%.
    ///
    /// - Returns: Estimated relative error (e.g., 0.0081 for 0.81%)
    public var estimatedRelativeError: Double {
        // Standard error = 1.04 / sqrt(m)
        return 1.04 / sqrt(Double(registers.count))
    }

    /// Get memory usage in bytes
    ///
    /// - Returns: Number of bytes used by registers
    public var memorySizeInBytes: Int {
        return registers.count
    }

    /// Borrows the canonical register storage for zero-intermediate-copy codecs.
    /// The pointer is valid only for the duration of `body` and must not escape.
    public func withUnsafeRegisters<Result>(
        _ body: (UnsafeBufferPointer<UInt8>) throws -> Result
    ) rethrows -> Result {
        try registers.withUnsafeBufferPointer(body)
    }
}

// MARK: - Codable

extension HyperLogLog {
    enum CodingKeys: String, CodingKey {
        case precision
        case registers
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        let precision = try container.decode(Int.self, forKey: .precision)
        guard Self.supportedPrecision.contains(precision) else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [CodingKeys.precision],
                    debugDescription: "Unsupported HyperLogLog precision: \(precision)"
                )
            )
        }
        let registers = try container.decode([UInt8].self, forKey: .registers)
        do {
            try self.init(precision: precision, registers: registers)
        } catch let error {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: [CodingKeys.registers],
                    debugDescription: "Invalid HyperLogLog register state: \(error)"
                )
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(precision, forKey: .precision)
        try container.encode(registers, forKey: .registers)
    }
}

// MARK: - CustomStringConvertible

extension HyperLogLog: CustomStringConvertible {
    public var description: String {
        let cardinalityDescription: String
        do {
            cardinalityDescription = String(try cardinality())
        } catch {
            cardinalityDescription = "out-of-range"
        }
        let error = estimatedRelativeError * 100
        return "HyperLogLog(cardinality: ~\(cardinalityDescription), errorPercent: \(error))"
    }
}
