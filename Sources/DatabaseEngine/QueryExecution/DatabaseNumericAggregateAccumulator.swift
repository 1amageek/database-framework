import DatabaseTypes

/// Canonical numeric state used when an aggregate is reduced incrementally.
/// Integer inputs remain exact through an Int128 accumulator, decimal inputs
/// use `ExactDecimal`, and floating-point inputs use compensated summation.
/// Exact integer and decimal inputs may be combined; mixing either exact kind
/// with floating point is rejected instead of silently changing numeric
/// semantics.
@_spi(DatabaseExecution)
public struct DatabaseNumericAggregateAccumulator: Sendable {
    public enum Failure: Error, Sendable, Equatable {
        case incompatibleNumericKinds
        case nonNumericValue
        case nonFiniteValue
        case numericOverflow
        case resultNotRepresentable
    }

    private struct IntegerState: Sendable {
        var total: Int128 = 0
        var count: Int128 = 0
        var sawSigned = false
        var sawUnsigned = false
    }

    private struct FloatingPointState: Sendable {
        var sum = 0.0
        var compensation = 0.0
        var count: Int128 = 0
    }

    private struct DecimalState: Sendable {
        var total: ExactDecimal
        var count: Int128
    }

    private enum State: Sendable {
        case empty
        case integer(IntegerState)
        case decimal(DecimalState)
        case floatingPoint(FloatingPointState)
    }

    private var state: State = .empty

    public init() {}

    public mutating func add(_ value: FieldValue) throws(Failure) {
        switch value {
        case .null:
            return
        case .int8(let value):
            try addInteger(Int128(value), signed: true)
        case .int16(let value):
            try addInteger(Int128(value), signed: true)
        case .int32(let value):
            try addInteger(Int128(value), signed: true)
        case .int64(let value):
            try addInteger(Int128(value), signed: true)
        case .uint8(let value):
            try addInteger(Int128(value), signed: false)
        case .uint16(let value):
            try addInteger(Int128(value), signed: false)
        case .uint32(let value):
            try addInteger(Int128(value), signed: false)
        case .uint64(let value):
            try addInteger(Int128(value), signed: false)
        case .decimal(let value):
            try addDecimal(value)
        case .float32(let value):
            try addFloatingPoint(Double(value))
        case .float64(let value):
            try addFloatingPoint(value)
        default:
            throw .nonNumericValue
        }
    }

    public func sum() throws(Failure) -> FieldValue? {
        switch state {
        case .empty:
            return nil
        case .integer(let state):
            return try integerResult(state.total, state: state)
        case .decimal(let state):
            return .decimal(state.total)
        case .floatingPoint(let state):
            let value = state.sum + state.compensation
            guard value.isFinite else { throw .numericOverflow }
            return .float64(value)
        }
    }

    public func average() throws(Failure) -> FieldValue? {
        switch state {
        case .empty:
            return nil
        case .integer(let state):
            let quotient = state.total / state.count
            let remainder = state.total % state.count
            if remainder == 0 {
                return try integerResult(quotient, state: state)
            }
            guard let total = Double(exactly: state.total),
                  let count = Double(exactly: state.count) else {
                throw .resultNotRepresentable
            }
            let value = total / count
            guard value.isFinite else { throw .numericOverflow }
            return .float64(value)
        case .decimal(let state):
            do {
                return .decimal(
                    try state.total.dividing(
                        by: ExactDecimal(
                            coefficient: state.count,
                            scale: 0
                        )
                    )
                )
            } catch ExactDecimalError.inexactResult {
                throw .resultNotRepresentable
            } catch {
                throw .numericOverflow
            }
        case .floatingPoint(let state):
            guard let count = Double(exactly: state.count) else {
                throw .resultNotRepresentable
            }
            let value = (state.sum + state.compensation) / count
            guard value.isFinite else { throw .numericOverflow }
            return .float64(value)
        }
    }

    private mutating func addInteger(
        _ value: Int128,
        signed: Bool
    ) throws(Failure) {
        switch state {
        case .empty:
            var next = IntegerState()
            try Self.accumulate(value, signed: signed, into: &next)
            state = .integer(next)
        case .integer(var next):
            try Self.accumulate(value, signed: signed, into: &next)
            state = .integer(next)
        case .decimal(var next):
            try Self.accumulate(
                ExactDecimal(coefficient: value, scale: 0),
                into: &next
            )
            state = .decimal(next)
        case .floatingPoint:
            throw .incompatibleNumericKinds
        }
    }

    private mutating func addFloatingPoint(
        _ value: Double
    ) throws(Failure) {
        guard value.isFinite else { throw .nonFiniteValue }
        switch state {
        case .empty:
            var next = FloatingPointState()
            try Self.accumulate(value, into: &next)
            state = .floatingPoint(next)
        case .integer:
            throw .incompatibleNumericKinds
        case .decimal:
            throw .incompatibleNumericKinds
        case .floatingPoint(var next):
            try Self.accumulate(value, into: &next)
            state = .floatingPoint(next)
        }
    }

    private mutating func addDecimal(
        _ value: ExactDecimal
    ) throws(Failure) {
        switch state {
        case .empty:
            state = .decimal(DecimalState(total: value, count: 1))
        case .integer(let integer):
            var next = DecimalState(
                total: ExactDecimal(coefficient: integer.total, scale: 0),
                count: integer.count
            )
            try Self.accumulate(value, into: &next)
            state = .decimal(next)
        case .decimal(var next):
            try Self.accumulate(value, into: &next)
            state = .decimal(next)
        case .floatingPoint:
            throw .incompatibleNumericKinds
        }
    }

    private static func accumulate(
        _ value: ExactDecimal,
        into state: inout DecimalState
    ) throws(Failure) {
        let count = state.count.addingReportingOverflow(1)
        guard !count.overflow else { throw .numericOverflow }
        do {
            state.total = try state.total.adding(value)
        } catch {
            throw .numericOverflow
        }
        state.count = count.partialValue
    }

    private static func accumulate(
        _ value: Int128,
        signed: Bool,
        into state: inout IntegerState
    ) throws(Failure) {
        let total = state.total.addingReportingOverflow(value)
        let count = state.count.addingReportingOverflow(1)
        guard !total.overflow, !count.overflow else {
            throw .numericOverflow
        }
        state.total = total.partialValue
        state.count = count.partialValue
        state.sawSigned = state.sawSigned || signed
        state.sawUnsigned = state.sawUnsigned || !signed
    }

    private static func accumulate(
        _ value: Double,
        into state: inout FloatingPointState
    ) throws(Failure) {
        let next = state.sum + value
        guard next.isFinite else { throw .numericOverflow }
        if abs(state.sum) >= abs(value) {
            state.compensation += (state.sum - next) + value
        } else {
            state.compensation += (value - next) + state.sum
        }
        guard state.compensation.isFinite else { throw .numericOverflow }
        let count = state.count.addingReportingOverflow(1)
        guard !count.overflow else { throw .numericOverflow }
        state.sum = next
        state.count = count.partialValue
    }

    private func integerResult(
        _ value: Int128,
        state: IntegerState
    ) throws(Failure) -> FieldValue {
        if state.sawSigned && !state.sawUnsigned {
            guard let result = Int64(exactly: value) else {
                throw .numericOverflow
            }
            return .int64(result)
        }
        if state.sawUnsigned && !state.sawSigned {
            guard let result = UInt64(exactly: value) else {
                throw .numericOverflow
            }
            return .uint64(result)
        }
        if let result = Int64(exactly: value) {
            return .int64(result)
        }
        guard let result = UInt64(exactly: value) else {
            throw .numericOverflow
        }
        return .uint64(result)
    }
}
