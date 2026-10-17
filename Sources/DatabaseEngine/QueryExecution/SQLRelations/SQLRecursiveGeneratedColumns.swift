import DatabaseKit
import DatabaseTypes

/// Selected generated columns consume evaluated targets and hidden parent cells.
struct SQLRecursiveGeneratedColumns: Sendable {
    struct Search: Sendable {
        let keySlots: [Int]
        let recordType: QueryCompositeType
        let breadthFirst: Bool
    }
    struct Cycle: Sendable {
        let keySlots: [Int]
        let recordType: QueryCompositeType
        let markType: QueryColumnType
        let cycleValue: QueryValue
        let defaultValue: QueryValue
    }
    let search: Search?
    let cycle: Cycle?

    func project(_ input: CanonicalRelation, seed: Bool, userColumnCount: Int,
        schema: CanonicalRelationSchema, environment: SQLTemporalEnvironment?,
        meter: DatabaseWorkMeter, limits: QueryValueLimits) throws -> CanonicalRelation {
        let hiddenCount = seed ? 0 : (search == nil ? 0 : 1) + (cycle == nil ? 0 : 1)
        guard input.rows.count == 1, input.rows[0].values.count == userColumnCount + hiddenCount else {
            throw mismatch("recursive generated input arity")
        }
        let row = input.rows[0]
        var values = try SQLArrayBuffer<QueryValue>(count: schema.layout.slots.count, meter: meter)
        var leases = try SQLArrayBuffer<DatabaseQueryPayloadLease>(meter: meter)
        try leases.append(input.rows.payloadLease)
        for lease in row.payloadLeases { try leases.append(lease) }
        for slot in 0..<userColumnCount { try values.append(row.values[slot]) }
        var hidden = userColumnCount
        if let search {
            let parent = seed ? nil : row.values[hidden]
            if !seed { hidden += 1 }
            let value: (value: QueryValue, lease: DatabaseQueryPayloadLease?)
            if search.breadthFirst {
                var depth: Int64 = 0
                if let parent {
                    guard case .composite(let record) = parent,
                          case .primitive(.int64(let previous)) = record.fields.first else {
                        throw mismatch("recursive breadth parent")
                    }
                    let next = previous.addingReportingOverflow(1)
                    guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
                    depth = next.partialValue
                }
                value = try record(slots: search.keySlots, row: row, type: search.recordType,
                    depth: depth, meter: meter, limits: limits)
            } else {
                let key = try record(slots: search.keySlots, row: row, type: search.recordType,
                    meter: meter, limits: limits)
                value = try appending(key.value, to: parent, type: search.recordType,
                    meter: meter, limits: limits)
                withExtendedLifetime(key) {}
            }
            try values.append(value.value)
            if let lease = value.lease { try leases.append(lease) }
        }
        if let cycle {
            let parent = seed ? nil : row.values[hidden]
            let key = try record(slots: cycle.keySlots, row: row, type: cycle.recordType,
                meter: meter, limits: limits)
            var repeated = false
            if let parent {
                guard case .array(let path) = parent else { throw mismatch("recursive cycle parent") }
                for previous in path.elements {
                    try meter.consume(at: .bindingCandidate)
                    if try CanonicalSQLValueSemantics.equalValues(key.value, previous,
                        leftType: .sql(.composite(cycle.recordType)), rightType: .sql(.composite(cycle.recordType)),
                        environment: environment, workMeter: meter) { repeated = true; break }
                }
            }
            let path = try appending(key.value, to: parent, type: cycle.recordType,
                meter: meter, limits: limits)
            try values.append(repeated ? cycle.cycleValue : cycle.defaultValue)
            try values.append(path.value)
            if let lease = path.lease { try leases.append(lease) }
            withExtendedLifetime(key) {}
        }
        let owned = values.finish()
        try leases.append(DatabaseQueryPayloadLease(retaining: owned.reservation))
        let leaseReservation = leases.reservation
        try leases.append(DatabaseQueryPayloadLease(retaining: leaseReservation))
        let ownedLeases = leases.finish()
        return try sqlSingleRow(CanonicalSourceRow(values: owned.values, layout: schema.layout,
            payloadLeases: ownedLeases.values), schema: schema, meter: meter)
    }

    func canExpand(_ row: CanonicalSourceRow, userColumnCount: Int,
        environment: SQLTemporalEnvironment?, meter: DatabaseWorkMeter) throws -> Bool {
        guard let cycle else { return true }
        let mark = row.values[userColumnCount + (search == nil ? 0 : 1)]
        try meter.consume(at: .bindingCandidate)
        guard !mark.isNull, !cycle.cycleValue.isNull else { return false }
        return try CanonicalSQLValueSemantics.compareValues(mark, cycle.cycleValue,
            leftType: cycle.markType, rightType: cycle.markType,
            environment: environment, workMeter: meter) != .equal
    }

    private func record(slots: [Int], row: CanonicalSourceRow, type: QueryCompositeType,
        depth: Int64? = nil, meter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        var fields = try SQLArrayBuffer<QueryValue>(count: slots.count + (depth == nil ? 0 : 1), meter: meter)
        if let depth { try fields.append(.primitive(.int64(depth))) }
        for slot in slots {
            guard row.values.indices.contains(slot) else { throw mismatch("recursive key slot") }
            try fields.append(row.values[slot])
        }
        let owned = fields.finish()
        return try withExtendedLifetime(owned) {
            try CanonicalSQLCompositeEvaluator.construct(type: type, fields: owned.values,
                workMeter: meter, limits: limits)
        }
    }

    private func appending(_ key: QueryValue, to parent: QueryValue?, type: QueryCompositeType,
        meter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        let previous: QueryArrayValue?
        if let parent {
            guard case .array(let path) = parent else { throw mismatch("recursive generated path") }
            previous = path
        } else { previous = nil }
        let count = try SQLArrayStorage.checkedAdd(previous?.elements.count ?? 0, 1)
        // Each independent flat output must own its complete path; admit the copy before constructing it.
        return try SQLArrayStorage.makeArray(element: .composite(type), rank: 1, count: count,
            meter: meter, limits: limits, sourceElementType: .sql(.composite(type)),
            dimension: { _ in try SQLArrayStorage.dimension(length: count) },
            cell: { index, _ in index == count - 1 ? key : previous!.elements[index] })
    }

    private func mismatch(_ operation: String) -> DatabaseExpressionEvaluationError {
        .typeMismatch(operation: operation)
    }
}
