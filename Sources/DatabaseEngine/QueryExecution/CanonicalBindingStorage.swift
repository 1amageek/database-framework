import DatabaseKit

/// Admission for immutable selected metadata and its temporary traversal buffers.
enum CanonicalBindingStorage {
    static func append<Element>(_ value: Element, to values: inout AdmittedArray<Element>,
        reservation: DatabaseIntermediateReservation?) throws {
        try append(value, to: &values.wrappedValue, admittedCapacity: &values.admittedCapacity,
            reservation: reservation)
    }

    /// Appends to an Array that starts empty and grows only through this
    /// function. Its admitted capacity is then the layout growth for its
    /// count, so no admission state is stored and recursive binder frames
    /// keep their size.
    static func appendGrowing<Element>(_ value: Element, to values: inout [Element],
        reservation: DatabaseIntermediateReservation?) throws {
        let layout = try DatabaseRetainedArrayLayout.forElement(Element.self)
        var admitted = values.isEmpty ? 0 : try layout.growth(from: 0, toFit: values.count).capacity
        try append(value, to: &values, admittedCapacity: &admitted, reservation: reservation)
    }

    /// The unwrapped form keeps deeply recursive binder frames small.
    static func append<Element>(_ value: Element, to values: inout [Element], admittedCapacity: inout Int,
        reservation: DatabaseIntermediateReservation?) throws {
        let next = values.count.addingReportingOverflow(1)
        guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
        let layout = try DatabaseRetainedArrayLayout.forElement(Element.self)
        let growth = try layout.growth(from: admittedCapacity, toFit: next.partialValue)
        if growth.capacity > admittedCapacity {
            try reservation?.reserveAdditional(bytes: growth.additionalByteCount, at: .bindingCandidate)
            values.reserveCapacity(growth.capacity)
            admittedCapacity = growth.capacity
        }
        values.append(value)
    }

    static func limits(_ meter: DatabaseWorkMeter?) -> QueryValueLimits {
        if let meter { return CanonicalSQLScalarCast.collectionLimits(workMeter: meter) }
        let limits = QueryStructuralLimits.default
        return QueryValueLimits(maximumRetainedBytes: limits.maximumRetainedBytes,
            maximumTotalNodes: limits.maximumTotalNodes, maximumNestingDepth: limits.maximumNestingDepth,
            maximumCollectionElements: limits.maximumCollectionElements)
    }

    static func composite(columns: [QueryColumn], workMeter: DatabaseWorkMeter?) throws
        -> (type: QueryCompositeType, lease: DatabaseQueryPayloadLease?, reservation: DatabaseIntermediateReservation?) {
        let scratch = try workMeter?.reserveIntermediate(bytes: UInt64(MemoryLayout<[QueryColumnType]>.stride),
            at: .bindingCandidate)
        defer { scratch?.release() }
        @AdmittedArray var pending: [QueryColumnType] = []
        var nodes = 1
        var count = columns.count
        var names: UInt64 = 0
        func add(_ value: Int, to total: inout Int) throws {
            let next = total.addingReportingOverflow(value)
            guard !next.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            total = next.partialValue
        }
        func column(_ column: QueryColumn) throws {
            let bytes = names.addingReportingOverflow(UInt64(column.name.utf8.count))
            guard !bytes.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            names = bytes.partialValue
            try append(column.type, to: &$pending, reservation: scratch)
        }
        for value in columns { try column(value) }
        while let type = pending.popLast() {
            try workMeter?.consume(at: .bindingCandidate)
            try add(1, to: &nodes)
            switch type {
            case .sql(.array(let element)): try append(.sql(element), to: &$pending, reservation: scratch)
            case .sql(.composite(let descriptor)):
                try add(descriptor.columns.count, to: &count)
                for value in descriptor.columns { try column(value) }
            default: break
            }
        }
        let bytes = try DatabaseIntermediateFootprint(bytes: QueryCompositeType.constructionByteCount(
            typeNodeCapacity: nodes, columnCapacity: count)).adding(.init(bytes: names)).bytes
        let reservation = try workMeter?.reserveIntermediate(bytes: bytes, at: .bindingCandidate)
        var successful = false
        defer { if !successful { reservation?.release() } }
        let result = try QueryCompositeType(columns: columns, limits: limits(workMeter),
            typeNodeCapacity: nodes, columnCapacity: count)
        let actual = try DatabaseIntermediateFootprint(bytes: result.storageFootprint.retainedStorageByteCount)
            .adding(.init(bytes: result.storageFootprint.utf8ByteCount)).bytes
        if actual > bytes { try reservation?.reserveAdditional(bytes: actual - bytes, at: .bindingCandidate) }
        else { try reservation?.releasePartial(bytes: bytes - actual) }
        successful = true
        return (result, reservation.map { DatabaseQueryPayloadLease(retaining: $0) }, reservation)
    }
}
