import DatabaseTypes

enum DistinctValueIdentityError: Error, Sendable {
    case nonFiniteNumericValue
    case invalidObject
}

struct DistinctValueIdentityResult {
    let value: FieldValue
    let changedRepresentation: Bool
}

/// Defines the one value identity used by both materialized DISTINCT indexes
/// and in-memory DISTINCT reduction.
enum DistinctValueIdentity {
    static func canonicalize(
        _ value: FieldValue
    ) throws(DistinctValueIdentityError) -> DistinctValueIdentityResult {
        switch value {
        case .int8(let integer):
            return changed(.int64(Int64(integer)))
        case .int16(let integer):
            return changed(.int64(Int64(integer)))
        case .int32(let integer):
            return changed(.int64(Int64(integer)))
        case .int64:
            return unchanged(value)
        case .uint8(let integer):
            return changed(.int64(Int64(integer)))
        case .uint16(let integer):
            return changed(.int64(Int64(integer)))
        case .uint32(let integer):
            return changed(.int64(Int64(integer)))
        case .uint64(let integer):
            if integer <= UInt64(Int64.max) {
                return changed(.int64(Int64(integer)))
            }
            return unchanged(value)
        case .float32(let number):
            guard number.isFinite else {
                throw .nonFiniteNumericValue
            }
            let result = try canonicalize(.float64(Double(number)))
            return DistinctValueIdentityResult(
                value: result.value,
                changedRepresentation: result.value != value
            )
        case .float64(let number):
            guard number.isFinite else {
                throw .nonFiniteNumericValue
            }
            if let integer = Int64(exactly: number) {
                return changed(.int64(integer))
            }
            if let integer = UInt64(exactly: number) {
                return changed(
                    integer <= UInt64(Int64.max)
                        ? .int64(Int64(integer))
                        : .uint64(integer)
                )
            }
            let normalized = number == 0 ? 0 : number
            return DistinctValueIdentityResult(
                value: .float64(normalized),
                changedRepresentation:
                    normalized.bitPattern != number.bitPattern
            )
        case .decimal(let decimal):
            let canonical = integralValue(decimal) ?? .decimal(decimal)
            return DistinctValueIdentityResult(
                value: canonical,
                changedRepresentation: canonical != value
            )
        case .array(let values):
            var canonical: [FieldValue]?
            for index in values.indices {
                let result = try canonicalize(values[index])
                if result.changedRepresentation {
                    if canonical == nil {
                        canonical = values
                    }
                    canonical?[index] = result.value
                }
            }
            guard let canonical else {
                return unchanged(value)
            }
            return changed(.array(canonical))
        case .object(let object):
            let fields = object.fields
            var canonical: [(key: String, value: FieldValue)]?
            for index in fields.indices {
                let result = try canonicalize(fields[index].value)
                if result.changedRepresentation {
                    if canonical == nil {
                        canonical = fields
                    }
                    let field = fields[index]
                    canonical?[index] = (
                        key: field.key,
                        value: result.value
                    )
                }
            }
            guard let canonical else {
                return unchanged(value)
            }
            do {
                return changed(.object(try FieldObject(canonical)))
            } catch {
                throw .invalidObject
            }
        case .null, .bool, .string, .bytes, .date, .time, .dateTime,
             .timestamp, .timeSpan, .calendarPeriod, .geographicPoint,
             .geographicPosition, .vector, .uuid, .reference, .rdfTerm:
            return unchanged(value)
        }
    }

    private static func changed(
        _ value: FieldValue
    ) -> DistinctValueIdentityResult {
        DistinctValueIdentityResult(
            value: value,
            changedRepresentation: true
        )
    }

    private static func unchanged(
        _ value: FieldValue
    ) -> DistinctValueIdentityResult {
        DistinctValueIdentityResult(
            value: value,
            changedRepresentation: false
        )
    }

    private static func integralValue(
        _ decimal: ExactDecimal
    ) -> FieldValue? {
        guard decimal.scale <= 0 else { return nil }
        if decimal.coefficient == 0 {
            return .int64(0)
        }

        let exponent = -Int64(decimal.scale)
        guard exponent <= 19 else { return nil }

        var integer = decimal.coefficient
        for _ in 0..<exponent {
            let product = integer.multipliedReportingOverflow(by: 10)
            guard !product.overflow else { return nil }
            integer = product.partialValue
        }
        if let signed = Int64(exactly: integer) {
            return .int64(signed)
        }
        if integer >= 0, let unsigned = UInt64(exactly: integer) {
            return .uint64(unsigned)
        }
        return nil
    }
}
