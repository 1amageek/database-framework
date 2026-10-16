import DatabaseKit

/// Array framing over the existing selected scalar input/output policy.
enum SQLArrayText {
    static func text(meter: DatabaseWorkMeter,
        render: (_ emit: (Substring) throws -> Void) throws -> Void) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        var count: UInt64 = 0
        try render { part in
            try meter.consume(UInt64(part.utf8.count), at: .expressionEvaluation)
            count = try DatabaseIntermediateFootprint(bytes: count).adding(.init(bytes: UInt64(part.utf8.count))).bytes
        }
        let result = try CanonicalSQLScalarText.produceText(count: count, workMeter: meter) {
            // The parent initializer owns this exclusive buffer. The synchronous
            // renderer initializes each admitted byte once; its pointer never escapes.
            try CanonicalSQLScalarText.makeText(count: count, workMeter: meter) { destination in
                var offset = 0
                try render { part in
                    for byte in part.utf8 {
                        try meter.consume(at: .expressionEvaluation)
                        guard offset < destination.count else { throw SQLArrayStorage.mismatch("array output measurement") }
                        destination[offset] = byte; offset += 1
                    }
                }
                guard offset == destination.count else { throw SQLArrayStorage.mismatch("array output measurement") }
            }
        }
        return (result.withValue { .primitive(copy $0) }, result.retainedReservation.map { DatabaseQueryPayloadLease(retaining: $0) })
    }

    static func bounds(_ array: QueryArrayValue, meter: DatabaseWorkMeter,
        emit: (Substring) throws -> Void) throws {
        for dimension in array.dimensions {
            try meter.consume(at: .expressionEvaluation)
            try emit("[")
            try CanonicalSQLScalarText.withText(of: .primitive(.int32(dimension.lowerBound)), type: .sql(.integer),
                preservingCharacterPadding: true, workMeter: meter, emit)
            try emit(":")
            try CanonicalSQLScalarText.withText(of: .primitive(.int32(dimension.lowerBound + (dimension.length - 1))), type: .sql(.integer),
                preservingCharacterPadding: true, workMeter: meter, emit)
            try emit("]")
        }
    }

    static func dimensions(_ array: QueryArrayValue, meter: DatabaseWorkMeter) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try text(meter: meter) { emit in try bounds(array, meter: meter, emit: emit) }
    }

    private static func quote(_ value: borrowing Substring, meter: DatabaseWorkMeter) throws -> Bool {
        if value.isEmpty { return true }
        var null = value.utf8.count == 4
        var index = 0
        for byte in value.utf8 {
            try meter.consume(at: .expressionEvaluation)
            if byte == 34 || byte == 92 || byte == 123 || byte == 125 || byte == 44 || byte == 32 || (9...13).contains(byte) { return true }
            let expected: UInt8 = index == 0 ? 110 : index == 1 ? 117 : 108
            if (65...90).contains(byte) ? byte + 32 != expected : byte != expected { null = false }
            index += 1
        }
        return null
    }

    static func format(_ array: QueryArrayValue, environment: SQLTemporalEnvironment?, meter: DatabaseWorkMeter) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try text(meter: meter) { emit in
            if array.dimensions.contains(where: { $0.lowerBound != 1 }) { try bounds(array, meter: meter, emit: emit); try emit("=") }
            var offset = 0
            func group(_ dimension: Int) throws {
                try emit("{")
                if !array.dimensions.isEmpty {
                    for index in 0..<Int(array.dimensions[dimension].length) {
                        try meter.consume(at: .expressionEvaluation)
                        if index > 0 { try emit(",") }
                        if dimension + 1 < array.dimensions.count { try group(dimension + 1) }
                        else {
                            let value = array.elements[offset]; offset += 1
                            if value.isNull { try emit("NULL"); continue }
                            try CanonicalSQLScalarText.withText(of: value, type: .sql(array.elementType), preservingCharacterPadding: true,
                                temporalEnvironment: environment, workMeter: meter) { scalar in
                                if try quote(scalar, meter: meter) {
                                    try emit("\"")
                                    for index in scalar.unicodeScalars.indices {
                                        try meter.consume(at: .expressionEvaluation)
                                        let end = scalar.unicodeScalars.index(after: index)
                                        if scalar.unicodeScalars[index].value == 34 || scalar.unicodeScalars[index].value == 92 { try emit("\\") }
                                        try emit(scalar[index..<end])
                                    }
                                    try emit("\"")
                                } else { try emit(scalar) }
                            }
                        }
                    }
                }
                try emit("}")
            }
            try group(0)
        }
    }

    static func join(_ array: QueryArrayValue, separator: String, nullString: String?,
        environment: SQLTemporalEnvironment?, meter: DatabaseWorkMeter) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try text(meter: meter) { emit in
            var first = true
            for value in array.elements {
                try meter.consume(at: .expressionEvaluation)
                if value.isNull && nullString == nil { continue }
                if !first { try emit(separator[...]) }; first = false
                if value.isNull { if let nullString { try emit(nullString[...]) } }
                else {
                    try CanonicalSQLScalarText.withText(of: value, type: .sql(array.elementType), preservingCharacterPadding: true,
                        temporalEnvironment: environment, workMeter: meter, emit)
                }
            }
        }
    }

    static func parse(_ input: String, element: DataType, context: CanonicalQueryTypeBinder.CastContext,
        environment: SQLTemporalEnvironment?, meter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        try SQLArrayInputWorkspace.withInput(sqlLexicalForm: input, limits: limits, workMeter: meter) { parsed in
            return try SQLArrayStorage.makeArray(element: element, rank: parsed.dimensions.count, count: parsed.count,
                meter: meter, limits: limits, dimension: { parsed.dimensions[$0] }, cell: { index, destination in
                    let allowance = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: UInt64(input.utf8.count))
                    let decoded = try meter.reserveIntermediate(bytes: allowance.bytes, at: .expressionEvaluation)
                    defer { decoded.release() }
                    try meter.consume(UInt64(input.utf8.count), at: .expressionEvaluation)
                    guard let text = try parsed.elementText(at: index) else { return SQLArrayStorage.null }
                    let actual = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: UInt64(text.utf8.count))
                    if actual.bytes > allowance.bytes { try decoded.reserveAdditional(bytes: actual.bytes - allowance.bytes, at: .expressionEvaluation) }
                    else { try decoded.releasePartial(bytes: allowance.bytes - actual.bytes) }
                    if CanonicalSQLScalarCast.usesPayload(.sql(element)) {
                        let result = try CanonicalSQLScalarCast.evaluatePayload(.primitive(.string(text)), from: .sql(.text),
                            to: .sql(element), context: context, temporalEnvironment: environment, workMeter: meter)
                        if let claim = result.retainedReservation { try destination.absorbAll(from: claim) }
                        else { try destination.absorbAll(from: decoded) }
                        return result.withValue { .primitive(copy $0) }
                    }
                    return try SQLArrayStorage.scalarCast(.primitive(.string(text)), from: .sql(.text), to: .sql(element),
                    context: context, environment: environment, meter: meter, limits: limits, destination: destination)
                })
        }
    }

    static func split(_ operands: [QueryValue], meter: DatabaseWorkMeter, limits: QueryValueLimits) throws
        -> (value: QueryValue, lease: DatabaseQueryPayloadLease?) {
        if operands[0].isNull { return (SQLArrayStorage.null, nil) }
        guard case .primitive(.string(let input)) = operands[0] else { throw SQLArrayStorage.mismatch("string_to_array input") }
        let delimiter: String?
        if operands[1].isNull { delimiter = nil }
        else if case .primitive(.string(let value)) = operands[1] { delimiter = value }
        else { throw SQLArrayStorage.mismatch("string_to_array delimiter") }
        let nullString: String?
        if operands.count == 3, !operands[2].isNull {
            guard case .primitive(.string(let value)) = operands[2] else { throw SQLArrayStorage.mismatch("string_to_array nullString") }
            nullString = value
        } else { nullString = nil }
        var ranges = try SQLArrayBuffer<Range<String.Index>>(meter: meter)
        if !input.isEmpty {
            if let delimiter {
                if delimiter.isEmpty { try ranges.append(input.startIndex..<input.endIndex) }
                else {
                    var start = input.utf8.startIndex, index = start
                    while index != input.utf8.endIndex {
                        try meter.consume(at: .expressionEvaluation)
                        var cursor = index, matches = true
                        for byte in delimiter.utf8 {
                            try meter.consume(at: .expressionEvaluation)
                            if cursor == input.utf8.endIndex || input.utf8[cursor] != byte { matches = false; break }
                            input.utf8.formIndex(after: &cursor)
                        }
                        if matches { try ranges.append(start..<index); start = cursor; index = cursor }
                        else { input.utf8.formIndex(after: &index) }
                    }
                    try ranges.append(start..<input.endIndex)
                }
            } else {
                for index in input.unicodeScalars.indices {
                    try ranges.append(index..<input.unicodeScalars.index(after: index))
                }
            }
        }
        return try SQLArrayStorage.makeArray(element: .text, rank: ranges.values.isEmpty ? 0 : 1,
            count: ranges.values.count, meter: meter, limits: limits,
            dimension: { _ in try SQLArrayStorage.dimension(length: ranges.values.count) }, cell: { index, destination in
                let view = input[ranges.values[index]]
                try meter.consume(UInt64(view.utf8.count), at: .expressionEvaluation)
                if let nullString, view.utf8.elementsEqual(nullString.utf8) { return SQLArrayStorage.null }
                let footprint = try CanonicalRelationalFootprintMeter.prospectiveValueFootprint(retainedByteCount: UInt64(view.utf8.count))
                try destination.reserveAdditional(bytes: footprint.bytes, at: .expressionEvaluation)
                // String materialization is the required independently owned result cell.
                return .primitive(.string(String(view)))
            })
    }
}
