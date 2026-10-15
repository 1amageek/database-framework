import DatabaseKit
import DatabaseTypes

/// Immutable widened decimal ownership for SQL intermediate arithmetic.
struct CanonicalSQLDecimalWorkspaceValue: Sendable {
    private final class Storage: Sendable {
        let digits: [UInt8]
        let reservation: DatabaseIntermediateReservation
        init(digits: consuming [UInt8], reservation: DatabaseIntermediateReservation) {
            self.digits = digits
            self.reservation = reservation
        }
    }

    private let storage: Storage
    let sign: Int
    let scale: Int64
    var isZero: Bool { sign == 0 }
    var decimalDigitCount: Int64 { Int64(storage.digits.count) }
    var workMeter: DatabaseWorkMeter { storage.reservation.workMeter }
    private var digits: [UInt8] { storage.digits }

    var leadingBase10000: (weight: Int64, digit: Int) {
        guard !isZero else { return (0, 0) }
        let decimalWeight = decimalDigitCount - scale - 1
        let weight = Self.floorDivision(decimalWeight, by: 4)
        let count = Int(decimalWeight - weight * 4 + 1)
        var digit = 0
        for offset in 0..<count {
            digit = digit * 10 + (offset < digits.count ? Int(digits[digits.count - 1 - offset]) : 0)
        }
        return (weight, digit)
    }

    func leadingEstimate(groups: Int) -> (significand: Double, exponent: Int64) {
        guard !isZero else { return (0, 0) }
        let leading = leadingBase10000
        var significand = Double(leading.digit)
        var exponent = leading.weight * 4
        if groups > 1 {
            for group in 1..<groups {
                let nextExponent = (leading.weight - Int64(group)) * 4
                guard nextExponent + 3 >= -scale else { break }
                var digit = 0
                for offset in stride(from: 3, through: 0, by: -1) {
                    digit = digit * 10 + Self.digit(digits, at: nextExponent + Int64(offset) + scale)
                }
                significand = significand * 10_000 + Double(digit)
                exponent -= 4
            }
        }
        return (significand, exponent)
    }

    init(_ value: ExactDecimal, workMeter: DatabaseWorkMeter) throws {
        var magnitude = value.coefficient.magnitude
        let reservation = try Self.reserve(capacity: 39, workMeter: workMeter)
        var digits: [UInt8] = []
        digits.reserveCapacity(64)
        while magnitude != 0 {
            digits.append(UInt8(magnitude % 10))
            magnitude /= 10
        }
        self.storage = Storage(digits: digits, reservation: reservation)
        self.sign = value.coefficient == 0 ? 0 : (value.coefficient < 0 ? -1 : 1)
        self.scale = Int64(value.scale)
    }

    private init(digits: consuming [UInt8], sign: Int, scale: Int64,
                 reservation: DatabaseIntermediateReservation) throws {
        var digits = digits
        while digits.last == 0 {
            try reservation.workMeter.consume(at: .expressionEvaluation)
            digits.removeLast()
        }
        var removed = 0
        while removed < digits.count && digits[removed] == 0 {
            try reservation.workMeter.consume(at: .expressionEvaluation)
            removed += 1
        }
        if removed > 0 { digits.removeFirst(removed) }
        let zero = digits.isEmpty
        self.storage = Storage(digits: digits, reservation: reservation)
        self.sign = zero ? 0 : sign
        self.scale = zero ? 0 : scale - Int64(removed)
    }

    func negated() -> Self {
        Self(storage: storage, sign: -sign, scale: scale)
    }

    private init(storage: Storage, sign: Int, scale: Int64) {
        self.storage = storage
        self.sign = sign
        self.scale = scale
    }

    func adding(_ other: Self) throws -> Self {
        try workMeter.checkpoint(at: .expressionEvaluation)
        if isZero { return other }
        if other.isZero { return self }
        let targetScale = max(scale, other.scale)
        let leftShift = targetScale - scale
        let rightShift = targetScale - other.scale
        let count = max(Int64(digits.count) + leftShift, Int64(other.digits.count) + rightShift) + 1
        let reservation = try Self.reserve(capacity: count, workMeter: workMeter)
        var output = try Self.zeroDigits(Int(count), workMeter: workMeter)
        let comparison = try magnitudeComparison(to: other)
        let resultSign = sign == other.sign ? sign : (comparison >= 0 ? sign : other.sign)
        var carry = 0
        for index in 0..<Int(count) {
            try workMeter.consume(at: .expressionEvaluation)
            let left = Self.digit(digits, at: Int64(index) - leftShift)
            let right = Self.digit(other.digits, at: Int64(index) - rightShift)
            var total = sign == other.sign ? left + right + carry :
                (comparison >= 0 ? left - right : right - left) + carry
            if total < 0 { total += 10; carry = -1 }
            else { carry = total / 10; total %= 10 }
            output[index] = UInt8(total)
        }
        return try Self(digits: output, sign: resultSign, scale: targetScale, reservation: reservation)
    }

    func subtracting(_ other: Self) throws -> Self { try adding(other.negated()) }

    func multiplied(by other: Self) throws -> Self {
        try workMeter.checkpoint(at: .expressionEvaluation)
        if isZero { return self }
        if other.isZero { return other }
        let count = digits.count + other.digits.count
        let reservation = try Self.reserve(capacity: Int64(count), workMeter: workMeter)
        var output = try Self.zeroDigits(count, workMeter: workMeter)
        for left in digits.indices {
            var carry = 0
            for right in other.digits.indices {
                try workMeter.consume(at: .expressionEvaluation)
                let index = left + right
                let total = Int(output[index]) + Int(digits[left]) * Int(other.digits[right]) + carry
                output[index] = UInt8(total % 10)
                carry = total / 10
            }
            output[left + other.digits.count] = UInt8(carry)
        }
        return try Self(digits: output, sign: sign * other.sign, scale: scale + other.scale, reservation: reservation)
    }

    func divided(by divisor: UInt64, toScale: Int32,
                 rounding: DecimalRoundingRule = .toNearestOrAwayFromZero) throws -> Self {
        let other = try Self(ExactDecimal(coefficient: Int128(divisor), scale: 0), workMeter: workMeter)
        return try divided(by: other, toScale: toScale, rounding: rounding)
    }

    func divided(by other: Self, toScale: Int32,
                 rounding: DecimalRoundingRule = .toNearestOrAwayFromZero) throws -> Self {
        guard !other.isZero else { throw ExactDecimalError.divisionByZero }
        try workMeter.checkpoint(at: .expressionEvaluation)
        if isZero { return self }
        let shift = Int64(toScale) + other.scale - scale
        let numeratorCount = Int64(digits.count) + max(0, shift)
        let denominatorCount = Int64(other.digits.count) + max(0, -shift)
        let reservation = try Self.reserve(capacity: numeratorCount + 1, workMeter: workMeter)
        let denominatorReservation = try Self.reserve(capacity: denominatorCount, workMeter: workMeter)
        defer { denominatorReservation.release() }
        let remainderReservation = try Self.reserve(capacity: denominatorCount + 1, workMeter: workMeter)
        defer { remainderReservation.release() }
        var denominator = try Self.zeroDigits(Int(denominatorCount), workMeter: workMeter)
        for index in other.digits.indices { denominator[index + Int(max(0, -shift))] = other.digits[index] }
        var remainder: [UInt8] = []
        remainder.reserveCapacity(Self.capacity(Int(denominatorCount + 1)))
        var quotient = try Self.zeroDigits(Int(numeratorCount + 1), workMeter: workMeter)
        for index in stride(from: Int(numeratorCount) - 1, through: 0, by: -1) {
            try workMeter.consume(at: .expressionEvaluation)
            remainder.insert(UInt8(Self.digit(digits, at: Int64(index) - max(0, shift))), at: 0)
            Self.trim(&remainder)
            var digit: UInt8 = 0
            while try Self.compare(remainder, denominator, workMeter: workMeter) >= 0 {
                try Self.subtract(&remainder, denominator, workMeter: workMeter)
                digit += 1
            }
            quotient[index] = digit
        }
        let discarded = !remainder.isEmpty
        var roundUp = false
        switch rounding {
        case .towardZero: break
        case .up: roundUp = discarded && sign * other.sign > 0
        case .down: roundUp = discarded && sign * other.sign < 0
        case .toNearestOrAwayFromZero:
            var carry = 0
            for index in remainder.indices {
                try workMeter.consume(at: .expressionEvaluation)
                let value = Int(remainder[index]) * 2 + carry
                remainder[index] = UInt8(value % 10); carry = value / 10
            }
            if carry != 0 { remainder.append(UInt8(carry)) }
            roundUp = try Self.compare(remainder, denominator, workMeter: workMeter) >= 0
        }
        if roundUp {
            var index = 0
            while quotient[index] == 9 {
                try workMeter.consume(at: .expressionEvaluation)
                quotient[index] = 0; index += 1
            }
            quotient[index] += 1
        }
        return try Self(digits: quotient, sign: sign * other.sign, scale: Int64(toScale), reservation: reservation)
    }

    func rounded(toScale: Int32, rounding: DecimalRoundingRule = .toNearestOrAwayFromZero) throws -> Self {
        if Int64(toScale) >= scale { return self }
        return try divided(by: 1, toScale: toScale, rounding: rounding)
    }

    func exactDecimal() throws -> ExactDecimal {
        guard digits.count <= 39, let resultScale = Int32(exactly: scale) else { throw ExactDecimalError.numericOverflow }
        var magnitude: UInt128 = 0
        for digit in digits.reversed() {
            let multiplication = magnitude.multipliedReportingOverflow(by: 10)
            let addition = multiplication.partialValue.addingReportingOverflow(UInt128(digit))
            guard !multiplication.overflow, !addition.overflow else { throw ExactDecimalError.numericOverflow }
            magnitude = addition.partialValue
        }
        let limit = UInt128(Int128.max) + (sign < 0 ? 1 : 0)
        guard magnitude <= limit else { throw ExactDecimalError.numericOverflow }
        let coefficient = sign < 0 ? (magnitude == limit ? Int128.min : -Int128(magnitude)) : Int128(magnitude)
        return ExactDecimal(coefficient: coefficient, scale: resultScale)
    }

    func comparison(to other: Self) throws -> Int {
        if sign != other.sign { return sign < other.sign ? -1 : 1 }
        return try sign * magnitudeComparison(to: other)
    }

    func remainder(dividingBy other: Self) throws -> Self {
        guard !other.isZero else { throw ExactDecimalError.divisionByZero }
        try workMeter.checkpoint(at: .expressionEvaluation)
        if try magnitudeComparison(to: other) < 0 { return self }
        let quotient = try divided(by: other, toScale: 0, rounding: .towardZero)
        return try subtracting(quotient.multiplied(by: other))
    }

    func magnitudeComparison(to other: Self) throws -> Int {
        try workMeter.consume(at: .expressionEvaluation)
        if isZero { return other.isZero ? 0 : -1 }
        if other.isZero { return 1 }
        let leftWeight = decimalDigitCount - scale
        let rightWeight = other.decimalDigitCount - other.scale
        if leftWeight != rightWeight { return leftWeight < rightWeight ? -1 : 1 }
        for offset in 0..<max(digits.count, other.digits.count) {
            try workMeter.consume(at: .expressionEvaluation)
            let left = Self.digit(digits, at: Int64(digits.count - 1 - offset))
            let right = Self.digit(other.digits, at: Int64(other.digits.count - 1 - offset))
            if left != right { return left < right ? -1 : 1 }
        }
        return 0
    }

    private static func reserve(capacity count: Int64, workMeter: DatabaseWorkMeter) throws -> DatabaseIntermediateReservation {
        guard count >= 0, count <= Int64(Int.max / 2) else {
            throw DatabaseWorkLimitError.maximumIntermediateBytes(stage: .expressionEvaluation,
                consumed: workMeter.retainedIntermediateBytes, requested: UInt64.max,
                maximum: workMeter.budget.maximumIntermediateBytes)
        }
        let layout = try DatabaseRetainedArrayLayout.forElement(UInt8.self)
        let growth = try layout.growth(from: 0, toFit: Int(count))
        let reservation = try workMeter.reserveIntermediate(bytes: growth.additionalByteCount + layout.containerByteCount + layout.sharedOwnerByteCount,
                                                 at: .expressionEvaluation)
        try workMeter.consume(UInt64(count), at: .expressionEvaluation)
        return reservation
    }

    private static func capacity(_ count: Int) -> Int {
        var capacity = 1
        while capacity < count { capacity *= 2 }
        return capacity
    }

    private static func zeroDigits(_ count: Int, workMeter: DatabaseWorkMeter) throws -> [UInt8] {
        var result: [UInt8] = []
        result.reserveCapacity(capacity(count))
        for index in 0..<count {
            // One fixed-width coefficient is the bounded checkpoint interval.
            if index % 39 == 0 { try workMeter.checkpoint(at: .expressionEvaluation) }
            result.append(0)
        }
        return result
    }

    private static func digit(_ digits: [UInt8], at index: Int64) -> Int {
        index >= 0 && index < Int64(digits.count) ? Int(digits[Int(index)]) : 0
    }
    private static func floorDivision(_ value: Int64, by divisor: Int64) -> Int64 {
        value >= 0 ? value / divisor : (value - divisor + 1) / divisor
    }
    private static func trim(_ digits: inout [UInt8]) {
        while digits.last == 0 { digits.removeLast() }
    }
    private static func compare(_ left: [UInt8], _ right: [UInt8], workMeter: DatabaseWorkMeter) throws -> Int {
        try workMeter.consume(at: .expressionEvaluation)
        if left.count != right.count { return left.count < right.count ? -1 : 1 }
        for index in left.indices.reversed() {
            try workMeter.consume(at: .expressionEvaluation)
            if left[index] != right[index] { return left[index] < right[index] ? -1 : 1 }
        }
        return 0
    }
    private static func subtract(_ left: inout [UInt8], _ right: [UInt8], workMeter: DatabaseWorkMeter) throws {
        var borrow = 0
        for index in left.indices {
            try workMeter.consume(at: .expressionEvaluation)
            var result = Int(left[index]) - (index < right.count ? Int(right[index]) : 0) - borrow
            borrow = result < 0 ? 1 : 0
            if result < 0 { result += 10 }
            left[index] = UInt8(result)
        }
        trim(&left)
    }
}
