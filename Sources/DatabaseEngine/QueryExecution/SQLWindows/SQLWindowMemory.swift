import DatabaseKit

/// Invocation-local capacity and payload admission shared by window workspaces.
enum SQLWindowMemory {
    static func array<Element>(count: Int, of: Element.Type,
        reservation: DatabaseIntermediateReservation) throws -> [Element] {
        let layout = try DatabaseRetainedArrayLayout.forElement(Element.self)
        let growth = try layout.growth(from: 0, toFit: count)
        let bytes = try DatabaseIntermediateFootprint(bytes: layout.containerByteCount)
            .adding(.init(bytes: growth.additionalByteCount))
        try reservation.reserveAdditional(bytes: bytes.bytes, at: .expressionEvaluation)
        var result: [Element] = []
        result.reserveCapacity(growth.capacity)
        if result.capacity > growth.capacity {
            try reservation.reserveAdditional(bytes: try DatabaseIntermediateFootprint(
                bytes: layout.elementCapacitySlotByteCount)
                .multiplied(by: UInt64(result.capacity - growth.capacity)).bytes, at: .expressionEvaluation)
        }
        return result
    }

    static func retain(_ cell: DatabaseContext.ProjectedQueryValue,
        reservation: DatabaseIntermediateReservation) throws {
        let meter = reservation.workMeter
        for lease in cell.leases {
            guard lease.workMeter === meter else {
                throw DatabaseExpressionEvaluationError.typeMismatch(operation: "window payload work meter")
            }
        }
        let leaseLayout = try DatabaseRetainedArrayLayout.forElement(DatabaseQueryPayloadLease.self)
        let leaseGrowth = try leaseLayout.growth(from: 0, toFit: cell.leases.capacity)
        try reservation.reserveAdditional(bytes: try DatabaseIntermediateFootprint(bytes: leaseLayout.containerByteCount)
            .adding(.init(bytes: leaseGrowth.additionalByteCount)).bytes, at: .expressionEvaluation)
        if !cell.payloadIsRetained || cell.leases.isEmpty {
            let footprint = try CanonicalRelationalFootprintMeter.valueFootprint(of: cell.value,
                workMeter: meter, stage: .expressionEvaluation)
            try reservation.reserveAdditional(bytes: footprint.bytes, at: .expressionEvaluation)
        }
    }

    static func integer(_ value: QueryValue, function: String) throws -> Int64? {
        if value.isNull { return nil }
        switch value {
        case .primitive(.int32(let value)): return Int64(value)
        case .primitive(.int64(let value)): return value
        default: throw DatabaseExpressionEvaluationError.invalidFunctionArgument(
            function: function, reason: "selected count must be an integer")
        }
    }

    static func sort(_ indices: inout [Int],
        less: (Int, Int) throws -> Bool) throws {
        func sift(_ start: Int, _ end: Int) throws {
            var root = start
            while root < end / 2 + end % 2 {
                let child = root * 2 + 1
                guard child <= end else { return }
                var selected = child
                if child < end, try less(indices[child], indices[child + 1]) { selected += 1 }
                guard try less(indices[root], indices[selected]) else { return }
                indices.swapAt(root, selected)
                root = selected
            }
        }
        guard indices.count > 1 else { return }
        var start = indices.count / 2
        while start > 0 { start -= 1; try sift(start, indices.count - 1) }
        var end = indices.count - 1
        while end > 0 { indices.swapAt(0, end); end -= 1; try sift(0, end) }
    }
}
