import DatabaseKit

/// Unique synchronous iteration; returned rows keep independent producer claims.
struct SQLArrayRowCursor: ~Copyable {
    private enum Operation { case unnest, subscripts(lower: Int32, length: Int32, reverse: Bool) }
    private struct State {
        let values: [QueryValue]
        let inputLeases: [DatabaseQueryPayloadLease]
        let inputClaim: DatabaseIntermediateReservation
        let columns: [QueryColumn]
        let metadataClaim: DatabaseIntermediateReservation
        let meter: DatabaseWorkMeter
        let operation: Operation
        let count: Int
        var index: Int = 0
    }
    private enum Lifecycle { case open(State), closed }
    private var lifecycle: Lifecycle

    init(bound: CanonicalBoundExpression, operands: [QueryValue], inputLeases: [DatabaseQueryPayloadLease],
        temporalEnvironment: SQLTemporalEnvironment?, workMeter: DatabaseWorkMeter, limits: QueryValueLimits) throws {
        guard case .function(let call) = bound.expression, operands.count == bound.children.count else {
            throw SQLArrayStorage.mismatch("selected array row function")
        }
        for lease in inputLeases where lease.workMeter !== workMeter {
            throw SQLArrayStorage.mismatch("array cursor input meter")
        }
        var needsInputClaim = false
        for index in operands.indices {
            try SQLArrayStorage.validate(operands[index], type: bound.children[index].type, meter: workMeter)
            if case .array(let array) = operands[index] {
                let owner = array.storageFootprint
                if owner.retainedStorageByteCount > 0 || owner.leafRetainedByteCount > 0
                    || owner.typeStorageFootprint.retainedStorageByteCount > 0
                    || owner.typeStorageFootprint.utf8ByteCount > 0 { needsInputClaim = true }
            }
        }
        guard !needsInputClaim || !inputLeases.isEmpty else { throw SQLArrayStorage.mismatch("array cursor missing input claims") }
        let operation: Operation, count: Int, columnCount: Int
        switch call.name {
        case "unnest":
            guard !operands.isEmpty else { throw SQLArrayStorage.mismatch("UNNEST arity") }
            var longest = 0
            for index in operands.indices {
                let array = try SQLArrayStorage.array(operands[index], type: bound.children[index].type)
                longest = max(longest, array?.elements.count ?? 0)
            }
            operation = .unnest; count = longest; columnCount = operands.count
        case "generate_subscripts":
            guard operands.count == 2 || operands.count == 3 else { throw SQLArrayStorage.mismatch("GENERATE_SUBSCRIPTS arity") }
            var lower: Int32 = 1, length: Int32 = 0, reverse = false
            if let array = try SQLArrayStorage.array(operands[0], type: bound.children[0].type), !operands[1].isNull {
                let dimension = try SQLArrayStorage.integer(operands[1])
                if dimension > 0, Int(dimension) <= array.dimensions.count {
                    lower = array.dimensions[Int(dimension) - 1].lowerBound
                    length = array.dimensions[Int(dimension) - 1].length
                }
            }
            if operands.count == 3 {
                if operands[2].isNull { length = 0 }
                else if case .primitive(.bool(let flag)) = operands[2] { reverse = flag }
                else { throw SQLArrayStorage.mismatch("GENERATE_SUBSCRIPTS reverse") }
            }
            operation = .subscripts(lower: lower, length: length, reverse: reverse)
            count = Int(length); columnCount = 1
        default: throw SQLArrayStorage.mismatch("selected array row function name")
        }
        guard UInt64(columnCount) <= limits.maximumCollectionElements else {
            throw QueryValueError.resourceLimit(resource: "cursor columns", actual: UInt64(columnCount), maximum: limits.maximumCollectionElements)
        }
        var inputs = try SQLArrayBuffer<QueryValue>(count: operands.count, extraBytes: UInt64(MemoryLayout<State>.stride), meter: workMeter)
        var claims = try SQLArrayBuffer<DatabaseQueryPayloadLease>(count: inputLeases.count, meter: workMeter)
        var columns = try SQLArrayBuffer<QueryColumn>(count: columnCount, meter: workMeter)
        for value in operands { try inputs.append(value) }
        for lease in inputLeases { try claims.append(lease) }
        for index in 0..<columnCount {
            guard let number = UInt32(exactly: index + 1) else { throw SQLArrayStorage.failure(.arraySubscriptOutOfRange) }
            let type: QueryColumnType = call.name == "unnest" ? .sql(try SQLArrayStorage.elementType(bound.children[index].type)) : .sql(.integer)
            if case .sql(.composite(let descriptor)) = type {
                let footprint = try descriptor.measure()
                try workMeter.consume(footprint.nodeCount, at: .expressionEvaluation)
                try columns.reservation.reserveAdditional(bytes: footprint.retainedByteCount, at: .expressionEvaluation)
            }
            try columns.reservation.reserveAdditional(bytes: UInt64(call.name.utf8.count), at: .expressionEvaluation)
            try columns.append(QueryColumn(number: number, name: call.name, type: type, isNullable: call.name == "unnest"))
        }
        let retainedInputs = inputs.finish(), retainedClaims = claims.finish(), retainedColumns = columns.finish()
        try retainedInputs.reservation.absorbAll(from: retainedClaims.reservation)
        let state = State(values: retainedInputs.values, inputLeases: retainedClaims.values,
            inputClaim: retainedInputs.reservation, columns: retainedColumns.values,
            metadataClaim: retainedColumns.reservation, meter: workMeter, operation: operation, count: count)
        // The nonoptional lifecycle has no implicit initial value to destroy on failure.
        // Install self only after every fallible local owner is complete.
        self.lifecycle = .open(state)
    }

    func metadata() throws -> (columns: [QueryColumn], lease: DatabaseQueryPayloadLease) {
        guard case .open(let state) = lifecycle else { throw SQLArrayStorage.mismatch("closed SQL array cursor metadata") }
        try state.meter.checkpoint(at: .expressionEvaluation)
        return (state.columns, DatabaseQueryPayloadLease(retaining: state.metadataClaim))
    }

    mutating func next() throws -> (values: [QueryValue], leases: [DatabaseQueryPayloadLease])? {
        guard case .open(var current) = lifecycle else { return nil }
        do {
            try current.meter.checkpoint(at: .expressionEvaluation)
            guard current.index < current.count else { close(); return nil }
            var values = try SQLArrayBuffer<QueryValue>(count: current.columns.count, meter: current.meter)
            try values.reservation.reserveAdditional(rows: 1, at: .expressionEvaluation)
            var leases = try SQLArrayBuffer<DatabaseQueryPayloadLease>(count: try SQLArrayStorage.checkedAdd(current.inputLeases.count, 1), meter: current.meter)
            switch current.operation {
            case .unnest:
                for value in current.values {
                    if case .array(let array) = value, current.index < array.elements.count { try values.append(array.elements[current.index]) }
                    else { try values.append(SQLArrayStorage.null) }
                }
            case .subscripts(let lower, let length, let reverse):
                let offset = reverse ? Int64(length) - 1 - Int64(current.index) : Int64(current.index)
                guard let value = Int32(exactly: Int64(lower) + offset) else { throw SQLArrayStorage.failure(.arraySubscriptOutOfRange) }
                try values.append(.primitive(.int32(value)))
            }
            for lease in current.inputLeases { try leases.append(lease) }
            let retainedValues = values.finish()
            try leases.append(DatabaseQueryPayloadLease(retaining: retainedValues.reservation, and: leases.reservation))
            let retainedLeases = leases.finish()
            current.index += 1
            lifecycle = .open(current)
            return (retainedValues.values, retainedLeases.values)
        } catch { close(); throw error }
    }

    mutating func close() { lifecycle = .closed }
}
