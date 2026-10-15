import DatabaseKit
import DatabaseTypes

/// Incremental numeric reduction using the input type selected by SQL binding.
@_spi(DatabaseExecution)
public struct DatabaseNumericAggregateAccumulator: Sendable {
    public enum Failure: Error, Sendable, Equatable {
        case incompatibleNumericKinds
        case nonNumericValue
        case nonFiniteValue
        case numericOverflow
        case resultNotRepresentable
    }

    private let sourceType: QueryColumnType
    private let inputType: QueryColumnType
    private let workMeter: DatabaseWorkMeter
    private var exactSum: CanonicalSQLDecimalWorkspaceValue?
    private var floatingSum: Double = 0
    private var realSum: Float = 0
    private var realSumOverflowed = false
    private var count: UInt64 = 0
    private var maximumDisplayScale: Int32 = 0

    public init(inputType: QueryColumnType, workMeter: DatabaseWorkMeter) {
        self.sourceType = inputType
        self.inputType = CanonicalQueryTypeBinder.numericAggregateInputType(inputType) ?? inputType
        self.workMeter = workMeter
    }

    public mutating func add(_ value: QueryValue) throws {
        try workMeter.consume(at: .aggregateInput)
        guard !value.isNull else { return }
        let nextCount = count.addingReportingOverflow(1)
        guard !nextCount.overflow else { throw Failure.numericOverflow }
        do {
            let value = try selectedValue(value)
            switch (inputType, value) {
            case (.sql(.smallint), .primitive(.int16(let value))):
                try addExact(ExactDecimal(coefficient: Int128(value), scale: 0), displayScale: 0)
            case (.sql(.integer), .primitive(.int32(let value))):
                try addExact(ExactDecimal(coefficient: Int128(value), scale: 0), displayScale: 0)
            case (.sql(.bigint), .primitive(.int64(let value))):
                try addExact(ExactDecimal(coefficient: Int128(value), scale: 0), displayScale: 0)
            case (.sql(.decimal), .numeric), (.sql(.decimal), .primitive(.decimal)):
                let numeric = try CanonicalSQLNumericEvaluator.numeric(value)
                try addExact(numeric.value, displayScale: numeric.displayScale)
            case (.sql(.real), .primitive(.float32(let value))):
                guard value.isFinite else { throw Failure.nonFiniteValue }
                let widened = floatingSum + Double(value)
                guard widened.isFinite else { throw Failure.numericOverflow }
                // SUM(real) has a real transition; AVG(real) has a double transition.
                // Keep the overflow flag independently so AVG remains usable.
                let narrowed = realSum + value
                realSumOverflowed = realSumOverflowed || !narrowed.isFinite
                realSum = narrowed
                floatingSum = widened
            case (.sql(.doublePrecision), .primitive(.float64(let value))):
                guard value.isFinite else { throw Failure.nonFiniteValue }
                let sum = floatingSum + value
                guard sum.isFinite else { throw Failure.numericOverflow }
                floatingSum = sum
            default:
                switch value {
                case .numeric, .primitive(.int8), .primitive(.int16), .primitive(.int32),
                     .primitive(.int64), .primitive(.uint8), .primitive(.uint16),
                     .primitive(.uint32), .primitive(.uint64), .primitive(.decimal),
                     .primitive(.float32), .primitive(.float64):
                    throw Failure.incompatibleNumericKinds
                default:
                    throw Failure.nonNumericValue
                }
            }
        } catch is ExactDecimalError {
            throw Failure.numericOverflow
        } catch is QueryValueError {
            throw Failure.numericOverflow
        }
        count = nextCount.partialValue
    }

    private func selectedValue(_ value: QueryValue) throws -> QueryValue {
        guard case .primitive = sourceType else { return value }
        guard QueryValueValidator.accepts(value, type: sourceType) else { throw Failure.incompatibleNumericKinds }
        if case .primitive(.float32(let number)) = value, !number.isFinite { throw Failure.nonFiniteValue }
        if case .primitive(.float64(let number)) = value, !number.isFinite { throw Failure.nonFiniteValue }
        return try CanonicalQueryTypeBinder.normalize(value, to: inputType)
    }

    private mutating func addExact(_ value: ExactDecimal, displayScale: Int32) throws {
        let input = try CanonicalSQLDecimalWorkspaceValue(value, workMeter: workMeter)
        let sum = try exactSum.map { try $0.adding(input) } ?? input
        exactSum = sum
        maximumDisplayScale = max(maximumDisplayScale, displayScale)
    }

    public func sum() throws -> QueryValue? {
        try workMeter.checkpoint(at: .aggregateInput)
        guard count > 0 else { return nil }
        switch inputType {
        case .sql(.real):
            guard !realSumOverflowed else { throw Failure.numericOverflow }
            return .primitive(.float32(realSum))
        case .sql(.doublePrecision):
            return .primitive(.float64(floatingSum))
        case .sql(.smallint), .sql(.integer):
            let decimal = try finalDecimal()
            guard decimal >= ExactDecimal(coefficient: Int128(Int64.min), scale: 0),
                  decimal <= ExactDecimal(coefficient: Int128(Int64.max), scale: 0),
                  decimal.scale <= 0 else { throw Failure.numericOverflow }
            var integer = decimal.coefficient
            for _ in 0..<(-Int64(decimal.scale)) {
                try workMeter.consume(at: .aggregateInput)
                integer *= 10
            }
            guard let result = Int64(exactly: integer) else { throw Failure.numericOverflow }
            return .primitive(.int64(result))
        case .sql(.bigint), .sql(.decimal):
            return try numericResult(finalDecimal(), displayScale: maximumDisplayScale)
        default:
            throw Failure.incompatibleNumericKinds
        }
    }

    public func average() throws -> QueryValue? {
        try workMeter.checkpoint(at: .aggregateInput)
        guard count > 0 else { return nil }
        switch inputType {
        case .sql(.real), .sql(.doublePrecision):
            let result = floatingSum / Double(count)
            guard result.isFinite else { throw Failure.numericOverflow }
            return .primitive(.float64(result))
        case .sql(.smallint), .sql(.integer), .sql(.bigint), .sql(.decimal):
            guard let exactSum else { throw Failure.resultNotRepresentable }
            let numerator = exactSum.leadingBase10000
            let denominator = CanonicalSQLNumericScale.leading(ExactDecimal(coefficient: Int128(count), scale: 0))
            let scale = CanonicalSQLNumericScale.divisionScale(
                leftWeight: numerator.weight, leftDigit: numerator.digit, leftDisplayScale: maximumDisplayScale,
                rightWeight: denominator.weight, rightDigit: denominator.digit, rightDisplayScale: 0)
            do {
                let rounded = try exactSum.divided(by: count, toScale: scale)
                return try numericResult(rounded.exactDecimal(), displayScale: scale)
            } catch is ExactDecimalError {
                throw Failure.numericOverflow
            }
        default:
            throw Failure.incompatibleNumericKinds
        }
    }

    private func finalDecimal() throws -> ExactDecimal {
        guard let exactSum else { throw Failure.resultNotRepresentable }
        do { return try exactSum.exactDecimal() }
        catch is ExactDecimalError { throw Failure.numericOverflow }
    }

    private func numericResult(_ decimal: ExactDecimal, displayScale: Int32) throws -> QueryValue {
        do { return .numeric(try QueryNumericValue(value: decimal, displayScale: displayScale)) }
        catch { throw Failure.numericOverflow }
    }
}
