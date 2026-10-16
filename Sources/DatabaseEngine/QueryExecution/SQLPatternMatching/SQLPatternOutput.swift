import DatabaseKit

/// Admits complete destination storage before copying any source range.
enum SQLPatternOutput {
    static func array(_ slices: [SQLPatternSlice?], meter: DatabaseWorkMeter,
                      limits: QueryValueLimits) throws -> (value: QueryArrayValue, lease: DatabaseQueryPayloadLease) {
        guard let elementCount = Int32(exactly: slices.count) else { throw SQLPatternError.invalidCaptureIndex }
        let elements = try DatabaseRetainedArrayLayout.forElement(QueryValue.self)
        let dimensions = try DatabaseRetainedArrayLayout.forElement(QueryArrayDimension.self)
        let growth = try elements.growth(from: 0, toFit: slices.count)
        let dimensionGrowth = try dimensions.growth(from: 0, toFit: slices.isEmpty ? 0 : 1)
        var footprint = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<QueryArrayValue>.stride))
            .adding(.init(bytes: elements.containerByteCount + growth.additionalByteCount))
            .adding(.init(bytes: dimensions.containerByteCount + dimensionGrowth.additionalByteCount))
        var copiedBytes: UInt64 = 0
        for slice in slices {
            try meter.consume(at: .expressionEvaluation)
            if let slice {
                guard slice.input.meter === meter else { throw SQLPatternError.meterMismatch }
                let bytes = try slice.byteCount()
                footprint = try footprint.adding(.init(bytes: UInt64(MemoryLayout<String>.stride))).adding(.init(bytes: bytes))
                copiedBytes = try DatabaseIntermediateFootprint(bytes: copiedBytes).adding(.init(bytes: bytes)).bytes
            }
        }
        try meter.consume(copiedBytes, at: .expressionEvaluation)
        let reservation = try meter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
        var values: [QueryValue] = []; values.reserveCapacity(growth.capacity)
        for slice in slices {
            try meter.consume(at: .expressionEvaluation)
            if let slice { values.append(.primitive(.string(String(slice.input.text[slice.range])))) }
            else { values.append(.primitive(.null)) }
        }
        let shape: [QueryArrayDimension]
        if values.isEmpty { shape = [] }
        else {
            shape = [try QueryArrayDimension(lowerBound: 1, length: elementCount)]
        }
        let result = try QueryArrayValue(elementType: .text, dimensions: shape, elements: values, limits: limits)
        return (result, DatabaseQueryPayloadLease(retaining: reservation))
    }
    static func text(_ slices: [SQLPatternSlice], meter: DatabaseWorkMeter) throws -> DatabaseQueryScopedFieldValue {
        var bytes: UInt64 = 0
        for slice in slices {
            guard slice.input.meter === meter else { throw SQLPatternError.meterMismatch }
            bytes = try DatabaseIntermediateFootprint(bytes: bytes).adding(.init(bytes: slice.byteCount())).bytes
        }
        try meter.consume(bytes, at: .expressionEvaluation)
        let footprint = try DatabaseIntermediateFootprint(bytes: bytes).adding(.init(bytes: UInt64(MemoryLayout<String>.stride)))
        let reservation = try meter.reserveIntermediate(bytes: footprint.bytes, at: .expressionEvaluation)
        guard let capacity = Int(exactly: bytes) else { throw SQLPatternError.invalidReplacement(offset: 0, reason: "output capacity overflow") }
        var result = ""; result.reserveCapacity(capacity)
        for slice in slices { try meter.consume(at: .expressionEvaluation); result.append(contentsOf: slice.input.text[slice.range]) }
        return .retaining(.string(result), reservation: reservation)
    }
}
