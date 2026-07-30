import DatabaseTypes
import DatabaseKit
import DatabaseEngine

private enum AggregationValueOrdering {
    case ascending
    case same
    case descending
}

/// Applies canonical aggregation semantics to query result values.
enum CanonicalAggregationReducer {
    private struct ExactIntegerAccumulator {
        var total: Int128 = 0
        var count: Int128 = 0
        var sawSigned = false
        var sawUnsigned = false

        mutating func add(
            _ value: FieldValue,
            operation: String,
            field: String
        ) throws {
            let operand: Int128
            switch value {
            case .int8(let integer):
                operand = Int128(integer)
                sawSigned = true
            case .int16(let integer):
                operand = Int128(integer)
                sawSigned = true
            case .int32(let integer):
                operand = Int128(integer)
                sawSigned = true
            case .int64(let integer):
                operand = Int128(integer)
                sawSigned = true
            case .uint8(let integer):
                operand = Int128(integer)
                sawUnsigned = true
            case .uint16(let integer):
                operand = Int128(integer)
                sawUnsigned = true
            case .uint32(let integer):
                operand = Int128(integer)
                sawUnsigned = true
            case .uint64(let integer):
                operand = Int128(integer)
                sawUnsigned = true
            default:
                throw AggregationQueryError.nonNumericValue(
                    field: field,
                    value: value
                )
            }

            let (nextTotal, totalOverflow) = total.addingReportingOverflow(operand)
            guard !totalOverflow else {
                throw AggregationQueryError.numericOverflow(
                    operation: operation,
                    field: field
                )
            }
            let (nextCount, countOverflow) = count.addingReportingOverflow(1)
            guard !countOverflow else {
                throw AggregationQueryError.numericOverflow(
                    operation: "count",
                    field: field
                )
            }
            total = nextTotal
            count = nextCount
        }

        func sum(field: String) throws -> FieldValue? {
            guard count > 0 else { return nil }
            return try integerResult(
                total,
                operation: "sum",
                field: field
            )
        }

        func average(field: String) throws -> FieldValue? {
            guard count > 0 else { return nil }
            let quotient = total / count
            let remainder = total % count
            if remainder == 0 {
                return try integerResult(
                    quotient,
                    operation: "average",
                    field: field
                )
            }

            guard let exactTotal = Double(exactly: total),
                  let exactCount = Double(exactly: count) else {
                throw AggregationQueryError.resultNotRepresentable(
                    operation: "average",
                    field: field
                )
            }
            let value = exactTotal / exactCount
            guard value.isFinite else {
                throw AggregationQueryError.numericOverflow(
                    operation: "average",
                    field: field
                )
            }
            return .float64(value)
        }

        private func integerResult(
            _ value: Int128,
            operation: String,
            field: String
        ) throws -> FieldValue {
            if sawSigned && !sawUnsigned {
                guard let integer = Int64(exactly: value) else {
                    throw AggregationQueryError.numericOverflow(
                        operation: operation,
                        field: field
                    )
                }
                return .int64(integer)
            }
            if sawUnsigned && !sawSigned {
                guard let integer = UInt64(exactly: value) else {
                    throw AggregationQueryError.numericOverflow(
                        operation: operation,
                        field: field
                    )
                }
                return .uint64(integer)
            }
            if let integer = Int64(exactly: value) {
                return .int64(integer)
            }
            guard let integer = UInt64(exactly: value) else {
                throw AggregationQueryError.numericOverflow(
                    operation: operation,
                    field: field
                )
            }
            return .uint64(integer)
        }
    }

    private struct FloatingPointAccumulator {
        var sum = 0.0
        var compensation = 0.0
        var count: Int128 = 0

        mutating func add(
            _ value: Double,
            operation: String,
            field: String
        ) throws {
            guard value.isFinite else {
                throw AggregationQueryError.nonFiniteNumericValue(field: field)
            }

            let next = sum + value
            guard next.isFinite else {
                throw AggregationQueryError.numericOverflow(
                    operation: operation,
                    field: field
                )
            }
            if abs(sum) >= abs(value) {
                compensation += (sum - next) + value
            } else {
                compensation += (value - next) + sum
            }
            guard compensation.isFinite else {
                throw AggregationQueryError.numericOverflow(
                    operation: operation,
                    field: field
                )
            }
            let (nextCount, overflow) = count.addingReportingOverflow(1)
            guard !overflow else {
                throw AggregationQueryError.numericOverflow(
                    operation: "count",
                    field: field
                )
            }
            sum = next
            count = nextCount
        }

        func total(operation: String, field: String) throws -> Double? {
            guard count > 0 else { return nil }
            let value = sum + compensation
            guard value.isFinite else {
                throw AggregationQueryError.numericOverflow(
                    operation: operation,
                    field: field
                )
            }
            return value
        }

        func average(field: String) throws -> Double? {
            guard let total = try total(operation: "average", field: field) else {
                return nil
            }
            guard let divisor = Double(exactly: count) else {
                throw AggregationQueryError.resultNotRepresentable(
                    operation: "average",
                    field: field
                )
            }
            let value = total / divisor
            guard value.isFinite else {
                throw AggregationQueryError.numericOverflow(
                    operation: "average",
                    field: field
                )
            }
            return value
        }
    }

    private enum NumericAccumulator {
        case empty
        case integer(ExactIntegerAccumulator)
        case floatingPoint(FloatingPointAccumulator)

        mutating func add(
            _ value: FieldValue,
            operation: String,
            field: String
        ) throws {
            switch value {
            case .null:
                return
            case .int8, .int16, .int32, .int64,
                 .uint8, .uint16, .uint32, .uint64:
                switch self {
                case .empty:
                    var accumulator = ExactIntegerAccumulator()
                    try accumulator.add(value, operation: operation, field: field)
                    self = .integer(accumulator)
                case .integer(var accumulator):
                    try accumulator.add(value, operation: operation, field: field)
                    self = .integer(accumulator)
                case .floatingPoint:
                    throw AggregationQueryError.incompatibleNumericKinds(field: field)
                }
            case .float32(let floatingPoint):
                try addFloatingPoint(
                    Double(floatingPoint),
                    operation: operation,
                    field: field
                )
            case .float64(let floatingPoint):
                try addFloatingPoint(
                    floatingPoint,
                    operation: operation,
                    field: field
                )
            default:
                throw AggregationQueryError.nonNumericValue(
                    field: field,
                    value: value
                )
            }
        }

        private mutating func addFloatingPoint(
            _ floatingPoint: Double,
            operation: String,
            field: String
        ) throws {
                switch self {
                case .empty:
                    var accumulator = FloatingPointAccumulator()
                    try accumulator.add(
                        floatingPoint,
                        operation: operation,
                        field: field
                    )
                    self = .floatingPoint(accumulator)
                case .integer:
                    throw AggregationQueryError.incompatibleNumericKinds(field: field)
                case .floatingPoint(var accumulator):
                    try accumulator.add(
                        floatingPoint,
                        operation: operation,
                        field: field
                    )
                    self = .floatingPoint(accumulator)
                }
        }

        func sum(field: String) throws -> FieldValue? {
            switch self {
            case .empty:
                return nil
            case .integer(let accumulator):
                return try accumulator.sum(field: field)
            case .floatingPoint(let accumulator):
                return try accumulator.total(
                    operation: "sum",
                    field: field
                ).map(FieldValue.float64)
            }
        }

        func average(field: String) throws -> FieldValue? {
            switch self {
            case .empty:
                return nil
            case .integer(let accumulator):
                return try accumulator.average(field: field)
            case .floatingPoint(let accumulator):
                return try accumulator.average(field: field).map(FieldValue.float64)
            }
        }
    }

    static func groupIdentity<T: Persistable>(
        item: T,
        fields: [FieldIdentity]
    ) throws -> [FieldValue] {
        var identity: [FieldValue] = []
        identity.reserveCapacity(fields.count)
        for field in fields {
            identity.append(
                try fieldValue(item: item, field: field)
            )
        }
        return identity
    }

    static func aggregate<T: Persistable>(
        items: [T],
        aggregation: AggregationType
    ) throws -> FieldValue? {
        switch aggregation {
        case .count:
            guard let count = Int64(exactly: items.count) else {
                throw AggregationQueryError.numericOverflow(
                    operation: "count",
                    field: "*"
                )
            }
            return .int64(count)
        case .sum(let field):
            var accumulator = NumericAccumulator.empty
            for item in items {
                try accumulator.add(
                    fieldValue(item: item, field: field),
                    operation: "sum",
                    field: field.name
                )
            }
            return try accumulator.sum(field: field.name)
        case .avg(let field):
            var accumulator = NumericAccumulator.empty
            for item in items {
                try accumulator.add(
                    fieldValue(item: item, field: field),
                    operation: "average",
                    field: field.name
                )
            }
            return try accumulator.average(field: field.name)
        case .min(let field):
            return try extremum(items: items, field: field, chooseMinimum: true)
        case .max(let field):
            return try extremum(items: items, field: field, chooseMinimum: false)
        case .distinct(let field):
            var values = Set<FieldValue>()
            for item in items {
                let value = try fieldValue(item: item, field: field)
                if !value.isNull {
                    do {
                        values.insert(
                            try DistinctValueIdentity.canonicalize(value).value
                        )
                    } catch DistinctValueIdentityError.nonFiniteNumericValue {
                        throw AggregationQueryError.nonFiniteNumericValue(
                            field: field.name
                        )
                    } catch DistinctValueIdentityError.invalidObject {
                        throw AggregationQueryError.resultNotRepresentable(
                            operation: "distinct",
                            field: field.name
                        )
                    }
                }
            }
            guard let count = Int64(exactly: values.count) else {
                throw AggregationQueryError.numericOverflow(
                    operation: "distinct",
                    field: field.name
                )
            }
            return .int64(count)
        case .percentile(let field, let percentile):
            try validate(percentile: percentile)
            var values: [FieldValue] = []
            values.reserveCapacity(items.count)
            for item in items {
                let value = try fieldValue(item: item, field: field)
                if value.isNull {
                    continue
                }
                guard value.isNumeric else {
                    throw AggregationQueryError.nonNumericValue(
                        field: field.name,
                        value: value
                    )
                }
                values.append(value)
            }
            return try Self.percentile(
                values: &values,
                percentile: percentile,
                field: field.name
            )
        }
    }

    static func sum(
        values: [FieldValue],
        field: String
    ) throws -> FieldValue? {
        var accumulator = NumericAccumulator.empty
        for value in values {
            try validate(value: value, field: field)
            try accumulator.add(value, operation: "sum", field: field)
        }
        return try accumulator.sum(field: field)
    }

    static func average(
        values: [FieldValue],
        field: String
    ) throws -> FieldValue? {
        var accumulator = NumericAccumulator.empty
        for value in values {
            try validate(value: value, field: field)
            try accumulator.add(value, operation: "average", field: field)
        }
        return try accumulator.average(field: field)
    }

    static func average(
        signedTotal: Int64,
        count: Int64,
        field: String
    ) throws -> FieldValue {
        try average(
            signedTotal: Int128(signedTotal),
            count: count,
            field: field
        )
    }

    static func average(
        signedTotal: Int128,
        count: Int64,
        field: String
    ) throws -> FieldValue {
        guard count > 0 else {
            throw AggregationQueryError.invalidIndexMetadata(
                "Average count must be positive"
            )
        }
        let wideCount = Int128(count)
        let quotient = signedTotal / wideCount
        let remainder = signedTotal % wideCount
        if remainder == 0 {
            guard let result = Int64(exactly: quotient) else {
                throw AggregationQueryError.resultNotRepresentable(
                    operation: "average",
                    field: field
                )
            }
            return .int64(result)
        }
        guard let exactTotal = Double(exactly: signedTotal),
              let exactCount = Double(exactly: wideCount) else {
            throw AggregationQueryError.resultNotRepresentable(
                operation: "average",
                field: field
            )
        }
        let result = exactTotal / exactCount
        guard result.isFinite else {
            throw AggregationQueryError.numericOverflow(
                operation: "average",
                field: field
            )
        }
        return .float64(result)
    }

    static func average(
        unsignedTotal: UInt64,
        count: Int64,
        field: String
    ) throws -> FieldValue {
        try average(
            unsignedTotal: UInt128(unsignedTotal),
            count: count,
            field: field
        )
    }

    static func average(
        unsignedTotal: UInt128,
        count: Int64,
        field: String
    ) throws -> FieldValue {
        guard count > 0, let unsignedCount = UInt128(exactly: count) else {
            throw AggregationQueryError.invalidIndexMetadata(
                "Average count must be positive"
            )
        }
        let quotient = unsignedTotal / unsignedCount
        let remainder = unsignedTotal % unsignedCount
        if remainder == 0 {
            guard let result = UInt64(exactly: quotient) else {
                throw AggregationQueryError.resultNotRepresentable(
                    operation: "average",
                    field: field
                )
            }
            return .uint64(result)
        }
        guard let exactTotal = Double(exactly: unsignedTotal),
              let exactCount = Double(exactly: unsignedCount) else {
            throw AggregationQueryError.resultNotRepresentable(
                operation: "average",
                field: field
            )
        }
        let result = exactTotal / exactCount
        guard result.isFinite else {
            throw AggregationQueryError.numericOverflow(
                operation: "average",
                field: field
            )
        }
        return .float64(result)
    }

    static func average(
        floatingPointTotal: Double,
        count: Int64,
        field: String
    ) throws -> FieldValue {
        guard floatingPointTotal.isFinite else {
            throw AggregationQueryError.nonFiniteNumericValue(field: field)
        }
        guard count > 0 else {
            throw AggregationQueryError.invalidIndexMetadata(
                "Average count must be positive"
            )
        }
        guard let exactCount = Double(exactly: count) else {
            throw AggregationQueryError.resultNotRepresentable(
                operation: "average",
                field: field
            )
        }
        let result = floatingPointTotal / exactCount
        guard result.isFinite else {
            throw AggregationQueryError.numericOverflow(
                operation: "average",
                field: field
            )
        }
        return .float64(result)
    }

    static func minimum(
        values: [FieldValue],
        field: String
    ) throws -> FieldValue? {
        try extremum(values: values, field: field, chooseMinimum: true)
    }

    static func maximum(
        values: [FieldValue],
        field: String
    ) throws -> FieldValue? {
        try extremum(values: values, field: field, chooseMinimum: false)
    }

    static func percentile(
        values: inout [FieldValue],
        percentile: Double,
        field: String
    ) throws -> FieldValue? {
        try validate(percentile: percentile)
        for value in values {
            try validate(value: value, field: field)
            guard value.isNull || value.isNumeric else {
                throw AggregationQueryError.nonNumericValue(
                    field: field,
                    value: value
                )
            }
        }
        values.removeAll { $0.isNull }
        guard !values.isEmpty else { return nil }

        try values.sort { lhs, rhs in
            try compare(lhs, rhs, field: field) == .ascending
        }

        guard let width = Double(exactly: values.count - 1) else {
            throw AggregationQueryError.resultNotRepresentable(
                operation: "percentile",
                field: field
            )
        }
        let position = percentile * width
        guard position.isFinite,
              let lowerIndex = Int(exactly: position.rounded(.down)),
              let upperIndex = Int(exactly: position.rounded(.up)) else {
            throw AggregationQueryError.resultNotRepresentable(
                operation: "percentile",
                field: field
            )
        }
        let lower = values[lowerIndex]
        let upper = values[upperIndex]
        if lowerIndex == upperIndex || lower == upper {
            return lower
        }

        let lowerDouble = try exactDouble(
            lower,
            operation: "percentile",
            field: field
        )
        let upperDouble = try exactDouble(
            upper,
            operation: "percentile",
            field: field
        )
        let fraction = position - Double(lowerIndex)
        let result = lowerDouble + ((upperDouble - lowerDouble) * fraction)
        guard result.isFinite else {
            throw AggregationQueryError.numericOverflow(
                operation: "percentile",
                field: field
            )
        }
        return .float64(result)
    }

    static func validate(percentile: Double) throws {
        guard percentile.isFinite, (0.0...1.0).contains(percentile) else {
            throw AggregationQueryError.invalidPercentile(percentile)
        }
    }

    static func fieldValue<Value: FieldValueRepresentable>(
        _ rawValue: Value?,
        field: String
    ) throws -> FieldValue {
        guard let rawValue else { return .null }
        let value = rawValue.fieldValue
        try validate(value: value, field: field)
        return value
    }

    private static func fieldValue<T: Persistable>(
        item: T,
        field: FieldIdentity
    ) throws -> FieldValue {
        guard T.fieldSchemas.contains(where: {
            $0.name == field.name && $0.fieldNumber == field.number
        }) else {
            throw AggregationQueryError.invalidField(field.name)
        }
        do {
            guard let value = try item.persistedFieldValue(for: field) else {
                throw AggregationQueryError.invalidField(field.name)
            }
            try validate(value: value, field: field.name)
            return value
        } catch let error as AggregationQueryError {
            throw error
        } catch let error as PersistableEncodingError {
            throw AggregationQueryError.persistedFieldEncodingFailed(
                field: field.name,
                reason: error
            )
        }
    }

    static func validate(value: FieldValue, field: String) throws {
        switch value {
        case .float32(let floatingPoint):
            guard floatingPoint.isFinite else {
                throw AggregationQueryError.nonFiniteNumericValue(field: field)
            }
        case .float64(let floatingPoint):
            guard floatingPoint.isFinite else {
                throw AggregationQueryError.nonFiniteNumericValue(field: field)
            }
        case .array(let values):
            for value in values {
                try validate(value: value, field: field)
            }
        default:
            break
        }
    }

    private static func extremum<T: Persistable>(
        items: [T],
        field: FieldIdentity,
        chooseMinimum: Bool
    ) throws -> FieldValue? {
        var selected: FieldValue?
        for item in items {
            let value = try fieldValue(item: item, field: field)
            if value.isNull {
                continue
            }
            if let current = selected {
                let comparison = try compare(value, current, field: field.name)
                if (chooseMinimum && comparison == .ascending)
                    || (!chooseMinimum && comparison == .descending) {
                    selected = value
                }
            } else {
                selected = value
            }
        }
        return selected
    }

    private static func extremum(
        values: [FieldValue],
        field: String,
        chooseMinimum: Bool
    ) throws -> FieldValue? {
        var selected: FieldValue?
        for value in values {
            try validate(value: value, field: field)
            if value.isNull {
                continue
            }
            if let current = selected {
                let comparison = try compare(value, current, field: field)
                if (chooseMinimum && comparison == .ascending)
                    || (!chooseMinimum && comparison == .descending) {
                    selected = value
                }
            } else {
                selected = value
            }
        }
        return selected
    }

    private static func compare(
        _ lhs: FieldValue,
        _ rhs: FieldValue,
        field: String
    ) throws -> AggregationValueOrdering {
        if lhs.isNumeric && rhs.isNumeric {
            if lhs == rhs { return .same }
            return lhs < rhs ? .ascending : .descending
        }

        switch (lhs, rhs) {
        case (.string, .string),
             (.bool, .bool),
             (.bytes, .bytes),
             (.date, .date),
             (.timestamp, .timestamp),
             (.uuid, .uuid),
             (.rdfTerm, .rdfTerm):
            if lhs == rhs { return .same }
            return lhs < rhs ? .ascending : .descending
        default:
            throw AggregationQueryError.incomparableValues(
                field: field,
                lhs: lhs,
                rhs: rhs
            )
        }
    }

    private static func exactDouble(
        _ value: FieldValue,
        operation: String,
        field: String
    ) throws -> Double {
        switch value {
        case .int8(let integer):
            return Double(integer)
        case .int16(let integer):
            return Double(integer)
        case .int32(let integer):
            return Double(integer)
        case .int64(let integer):
            guard let result = Double(exactly: integer) else {
                throw AggregationQueryError.resultNotRepresentable(
                    operation: operation,
                    field: field
                )
            }
            return result
        case .uint8(let integer):
            return Double(integer)
        case .uint16(let integer):
            return Double(integer)
        case .uint32(let integer):
            return Double(integer)
        case .uint64(let integer):
            guard let result = Double(exactly: integer) else {
                throw AggregationQueryError.resultNotRepresentable(
                    operation: operation,
                    field: field
                )
            }
            return result
        case .float32(let floatingPoint):
            guard floatingPoint.isFinite else {
                throw AggregationQueryError.nonFiniteNumericValue(field: field)
            }
            return Double(floatingPoint)
        case .float64(let floatingPoint):
            guard floatingPoint.isFinite else {
                throw AggregationQueryError.nonFiniteNumericValue(field: field)
            }
            return floatingPoint
        default:
            throw AggregationQueryError.nonNumericValue(
                field: field,
                value: value
            )
        }
    }
}
