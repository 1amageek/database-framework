import DatabaseKit

/// The selected S05 iterators stay linear and owned by one invocation actor.
struct SQLPatternRowCursor: ~Copyable {
    private var matches: SQLPatternMatchIterator?
    private var splits: SQLPatternSplitIterator?
    private let global: Bool
    private let limits: QueryValueLimits
    private let meter: DatabaseWorkMeter
    private var emitted = false

    init(selected: CanonicalBoundTableFunction, values: [QueryValue], leases: [DatabaseQueryPayloadLease],
        meter: DatabaseWorkMeter, limits: QueryValueLimits) throws {
        guard values.count == selected.bound.children.count, (2...3).contains(values.count) else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "pattern row selected arity")
        }
        var preparedMatches: SQLPatternMatchIterator?
        var preparedSplits: SQLPatternSplitIterator?
        var isGlobal = false
        if !values.contains(where: { $0.isNull }) {
            func input(_ index: Int) throws -> SQLPatternInput {
                guard case .primitive(.string(let value)) = values[index] else {
                    throw DatabaseExpressionEvaluationError.typeMismatch(operation: "pattern row selected text")
                }
                return try leases.isEmpty ? SQLPatternInput.admitting(value, workMeter: meter)
                    : SQLPatternInput.retaining(value, leases: leases, workMeter: meter)
            }
            let subject = try input(0), pattern = try input(1)
            let flags = try values.count > 2 ? input(2) : nil
            let operation: SQLPatternOperation = selected.kind == .patternMatches ? .matches : .split
            let program = try SQLPatternProgram.compile(pattern: pattern, syntax: .ere(caseInsensitive: false), flags: flags, operation: operation)
            isGlobal = program.isGlobal
            if selected.kind == .patternMatches { preparedMatches = try program.makeMatchIterator(in: subject) }
            else { preparedSplits = try program.makeSplitIterator(in: subject) }
        }
        self.matches = consume preparedMatches
        self.splits = consume preparedSplits
        self.global = isGlobal
        self.limits = limits
        self.meter = meter
    }

    mutating func next() throws -> (values: [QueryValue], leases: [DatabaseQueryPayloadLease])? {
        if matches != nil {
            guard !emitted || global, let match = try matches?.next() else { close(); return nil }
            emitted = true
            let result = try match.materializeTextArray(limits: limits)
            return try sqlTableFunctionValue(.array(result.value), lease: result.lease, meter: meter)
        }
        if let slice = try splits?.next() {
            let result = try slice.materialize()
            return try sqlTableFunctionValue(result.withValue { .primitive(copy $0) },
                lease: result.retainedReservation.map { .init(retaining: $0) }, meter: meter)
        }
        close(); return nil
    }

    mutating func close() { matches = nil; splits = nil }
}
