import DatabaseKit

func makeSQLTableFunctionProducer(schema: CanonicalRelationSchema, selected: CanonicalBoundTableFunction,
    operands: [DatabaseContext.ProjectedQueryValue], temporalEnvironment: SQLTemporalEnvironment,
    workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> any SQLRelationProducer {
    do {
        return try SQLTableFunctionProducer(schema: schema, selected: selected, operands: operands,
            environment: temporalEnvironment, meter: workMeter, limits: limits)
    } catch { throw sqlTableFunctionFailure(error, selected: selected) }
}

/// Synchronous lower cursors never leave this actor's isolation domain.
actor SQLTableFunctionProducer: SQLRelationProducer {
    nonisolated let schema: CanonicalRelationSchema
    private let selected: CanonicalBoundTableFunction
    private let meter: DatabaseWorkMeter
    private var arrayCursor: SQLArrayRowCursor?
    private var jsonCursor: SQLJSONRowCursor?
    private var patternCursor: SQLPatternRowCursor?
    private var seriesCursor: SQLSeriesCursor?
    private var scalar: [QueryValue]?
    private var inputLeases: [DatabaseQueryPayloadLease]
    private var ownership: DatabaseIntermediateReservation?
    private var ordinal: Int64 = 0
    private var terminal = false
    private var failure: (any Error)?

    init(schema: CanonicalRelationSchema, selected: CanonicalBoundTableFunction,
        operands: [DatabaseContext.ProjectedQueryValue], environment: SQLTemporalEnvironment,
        meter: DatabaseWorkMeter, limits: QueryValueLimits) throws {
        let expected = selected.kind == .scalar ? 1 : selected.bound.children.count
        guard operands.count == expected, schema.layout.slots.count == selected.columns.count else {
            throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected table function frame")
        }
        var values = try SQLArrayBuffer<QueryValue>(count: operands.count,
            extraBytes: UInt64(MemoryLayout<SQLSeriesCursor>.stride + MemoryLayout<SQLArrayRowCursor>.stride
                + MemoryLayout<SQLJSONRowCursor>.stride + MemoryLayout<SQLPatternRowCursor>.stride), meter: meter)
        var leases = try SQLArrayBuffer<DatabaseQueryPayloadLease>(meter: meter)
        for operand in operands {
            try values.append(operand.value)
            for lease in operand.leases {
                guard lease.workMeter === meter else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "table function input meter") }
                try leases.append(lease)
            }
            if operand.leases.isEmpty {
                let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: operand.value, workMeter: meter, stage: .expressionEvaluation)
                let claim = try meter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
                try leases.append(DatabaseQueryPayloadLease(retaining: claim))
            }
        }
        let retainedValues = values.finish(), retainedLeases = leases.finish()
        try retainedValues.reservation.absorbAll(from: retainedLeases.reservation)
        var preparedArray: SQLArrayRowCursor?
        var preparedJSON: SQLJSONRowCursor?
        var preparedPattern: SQLPatternRowCursor?
        var preparedSeries: SQLSeriesCursor?
        var preparedScalar: [QueryValue]?
        switch selected.kind {
        case .array:
            preparedArray = try CanonicalSQLArrayEvaluator.makeRowCursor(selected.bound, operands: retainedValues.values,
                inputLeases: retainedLeases.values, temporalEnvironment: environment, workMeter: meter, limits: limits)
        case .json:
            preparedJSON = try CanonicalSQLJSONEvaluator.makeRowCursor(selected.bound, operands: retainedValues.values,
                inputLeases: retainedLeases.values, temporalEnvironment: environment, workMeter: meter, limits: limits)
        case .patternMatches, .patternSplit:
            preparedPattern = try SQLPatternRowCursor(selected: selected, values: retainedValues.values,
                leases: retainedLeases.values, meter: meter, limits: limits)
        case .series:
            preparedSeries = try SQLSeriesCursor(bound: selected.bound, values: retainedValues.values, environment: environment, meter: meter)
        case .scalar: preparedScalar = retainedValues.values
        }
        self.schema = schema; self.selected = selected; self.meter = meter
        arrayCursor = consume preparedArray; jsonCursor = consume preparedJSON
        patternCursor = consume preparedPattern; seriesCursor = consume preparedSeries; scalar = preparedScalar
        inputLeases = retainedLeases.values; ownership = retainedValues.reservation
    }

    func next() throws -> CanonicalRelation? {
        if let failure { throw failure }
        guard !terminal else { return nil }
        do {
            try meter.consume(at: .expressionEvaluation)
            let output: (values: [QueryValue], leases: [DatabaseQueryPayloadLease])?
            switch selected.kind {
            case .array: output = try arrayCursor?.next()
            case .json: output = try jsonCursor?.next()
            case .patternMatches, .patternSplit: output = try patternCursor?.next()
            case .series:
                if let value = try seriesCursor?.next() { output = try sqlTableFunctionValue(value, retaining: inputLeases, meter: meter) }
                else { output = nil }
            case .scalar:
                output = scalar.map { ($0, inputLeases) }; scalar = nil
            }
            guard let output else { finish(); return nil }
            var cells = try SQLArrayBuffer<QueryValue>(count: selected.columns.count, meter: meter)
            if selected.kind == .array, selectedFunctionName(selected) == "unnest" {
                for index in output.values.indices {
                    guard let type = CanonicalQueryTypeBinder.arrayElement(selected.bound.children[index].type) else {
                        throw DatabaseExpressionEvaluationError.typeMismatch(operation: "selected UNNEST array type")
                    }
                    try append(output.values[index], type: type, to: &cells)
                }
            } else if selected.kind == .scalar {
                guard let value = output.values.first else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "scalar table value") }
                try append(value, type: selected.bound.type, to: &cells)
            } else {
                for value in output.values { try cells.append(value) }
            }
            if selected.withOrdinality {
                let next = ordinal.addingReportingOverflow(1)
                guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                ordinal = next.partialValue
                try cells.append(.primitive(.int64(ordinal)))
            }
            guard cells.values.count == schema.layout.slots.count else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "table output arity") }
            var claims = try SQLArrayBuffer<DatabaseQueryPayloadLease>(count: output.leases.count + 1, meter: meter)
            for lease in output.leases { try claims.append(lease) }
            let retainedCells = cells.finish()
            try claims.append(DatabaseQueryPayloadLease(retaining: retainedCells.reservation, and: claims.reservation))
            let row = CanonicalSourceRow(values: retainedCells.values, layout: schema.layout, payloadLeases: claims.finish().values)
            let result = try sqlSingleRow(row, schema: schema, meter: meter)
            try meter.checkpoint(at: .expressionEvaluation)
            return result
        } catch {
            let error = sqlTableFunctionFailure(error, selected: selected)
            failure = error; finish(); throw error
        }
    }

    func close() { finish() }

    private func finish() {
        guard !terminal else { return }
        terminal = true
        arrayCursor = nil; jsonCursor = nil; patternCursor = nil; seriesCursor = nil
        scalar = nil; inputLeases = []; ownership = nil
    }

    private func append(_ value: QueryValue, type: QueryColumnType, to cells: inout SQLArrayBuffer<QueryValue>) throws {
        if case .sql(.composite(let descriptor)) = type {
            if value.isNull { for _ in descriptor.columns { try cells.append(.primitive(.null)) } }
            else if case .composite(let composite) = value {
                guard composite.type == descriptor else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "table composite descriptor") }
                for field in composite.fields { try cells.append(field) }
            } else { throw DatabaseExpressionEvaluationError.typeMismatch(operation: "table composite value") }
        } else { try cells.append(value) }
    }
}

private func selectedFunctionName(_ selected: CanonicalBoundTableFunction) -> String {
    if case .function(let call) = selected.bound.expression { return call.name }
    return "table function"
}

/// Admits each one-cell carrier before allocating its vectors.
func sqlTableFunctionValue(_ value: QueryValue, retaining inputLeases: [DatabaseQueryPayloadLease] = [],
    lease: DatabaseQueryPayloadLease? = nil, meter: DatabaseWorkMeter) throws -> (values: [QueryValue], leases: [DatabaseQueryPayloadLease]) {
    var values = try SQLArrayBuffer<QueryValue>(count: 1, meter: meter)
    var leases = try SQLArrayBuffer<DatabaseQueryPayloadLease>(count: inputLeases.count + (lease == nil ? 1 : 2), meter: meter)
    try values.append(value)
    for inputLease in inputLeases { try leases.append(inputLease) }
    if let lease { try leases.append(lease) }
    let retained = values.finish()
    try leases.append(DatabaseQueryPayloadLease(retaining: retained.reservation, and: leases.reservation))
    return (retained.values, leases.finish().values)
}

/// This is the same semantic boundary used by selected scalar expression evaluation.
func sqlTableFunctionFailure(_ error: any Error, selected: CanonicalBoundTableFunction) -> any Error {
    let mapped = CanonicalSQLPatternEvaluator.mapFailure(error, function: selectedFunctionName(selected))
    switch mapped {
    case let cause as SQLTimeZoneError:
        return CanonicalReadError.expressionEvaluation(.timeZone(cause), occurrence: selected.bound.occurrence)
    case let cause as DatabaseExpressionEvaluationError:
        return CanonicalReadError.expressionEvaluation(cause, occurrence: selected.bound.occurrence)
    case let cause as QueryValueError:
        return CanonicalReadError.expressionEvaluation(.queryValue(cause), occurrence: selected.bound.occurrence)
    case CanonicalReadError.expressionEvaluation(let cause, let occurrence):
        return CanonicalReadError.expressionEvaluation(cause, occurrence: occurrence ?? selected.bound.occurrence)
    default: return mapped
    }
}
