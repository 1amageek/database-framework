import DatabaseTypes

/// Bounded, reflection-free formatting for values exposed in diagnostics.
enum DatabaseDiagnosticValueFormatter {
    private static let maximumDepth = 8
    private static let maximumCollectionElements = 16

    static func describe(_ value: FieldValue) -> String {
        describe(value, depth: 0)
    }

    private static func describe(
        _ value: FieldValue,
        depth: Int
    ) -> String {
        guard depth < maximumDepth else { return "…" }

        switch value {
        case .null:
            return "null"
        case .bool(let value):
            return value ? "true" : "false"
        case .int8(let value):
            return String(value)
        case .int16(let value):
            return String(value)
        case .int32(let value):
            return String(value)
        case .int64(let value):
            return String(value)
        case .uint8(let value):
            return String(value)
        case .uint16(let value):
            return String(value)
        case .uint32(let value):
            return String(value)
        case .uint64(let value):
            return String(value)
        case .float32(let value):
            return String(value)
        case .float64(let value):
            return String(value)
        case .decimal(let value):
            return "decimal(coefficient: \(value.coefficient), scale: \(value.scale))"
        case .string(let value):
            return value
        case .bytes(let value):
            return "0x" + DatabaseTextFormatting.lowercaseHex(value)
        case .date(let value):
            return "date(\(value.year)-\(value.month)-\(value.day))"
        case .time(let value):
            return "time(\(value.hour):\(value.minute):\(value.second).\(value.nanoseconds))"
        case .dateTime(let value):
            return "dateTime(\(value.date.year)-\(value.date.month)-\(value.date.day) "
                + "\(value.time.hour):\(value.time.minute):\(value.time.second).\(value.time.nanoseconds))"
        case .timestamp(let value):
            return "timestamp(\(value.secondsSinceUnixEpoch).\(value.nanoseconds))"
        case .timeSpan(let value):
            return "timeSpan(seconds: \(value.seconds), nanoseconds: \(value.nanoseconds))"
        case .calendarPeriod(let value):
            return "calendarPeriod(months: \(value.months), days: \(value.days))"
        case .geographicPoint(let value):
            return "point(\(value.latitude), \(value.longitude))"
        case .geographicPosition(let value):
            return "position(\(value.point.latitude), \(value.point.longitude), "
                + "\(value.ellipsoidalHeightInMeters))"
        case .vector(let value):
            return "vector(type: \(vectorElementTypeName(value.elementType)), count: \(value.count))"
        case .uuid(let value):
            return value.description
        case .array(let values):
            return collectionDescription(
                values,
                opening: "[",
                closing: "]",
                depth: depth
            )
        case .object(let object):
            let fields = object.fields
            let visible = fields.prefix(maximumCollectionElements).map { field in
                "\(field.key): \(describe(field.value, depth: depth + 1))"
            }
            let suffix = fields.count > maximumCollectionElements ? ", …" : ""
            return "{" + visible.joined(separator: ", ") + suffix + "}"
        case .reference(let reference):
            return "reference(entity: \(reference.entity), id: "
                + referenceIdentifierDescription(reference.id, depth: depth + 1)
                + ")"
        case .rdfTerm(let term):
            return term.description
        }
    }

    private static func collectionDescription(
        _ values: [FieldValue],
        opening: String,
        closing: String,
        depth: Int
    ) -> String {
        let visible = values.prefix(maximumCollectionElements).map {
            describe($0, depth: depth + 1)
        }
        let suffix = values.count > maximumCollectionElements ? ", …" : ""
        return opening + visible.joined(separator: ", ") + suffix + closing
    }

    private static func referenceIdentifierDescription(
        _ identifier: ReferenceIdentifier,
        depth: Int
    ) -> String {
        guard depth < maximumDepth else { return "…" }

        switch identifier {
        case .bool(let value): return value ? "true" : "false"
        case .int8(let value): return String(value)
        case .int16(let value): return String(value)
        case .int32(let value): return String(value)
        case .int64(let value): return String(value)
        case .uint8(let value): return String(value)
        case .uint16(let value): return String(value)
        case .uint32(let value): return String(value)
        case .uint64(let value): return String(value)
        case .string(let value): return value
        case .bytes(let value):
            return "0x" + DatabaseTextFormatting.lowercaseHex(value)
        case .uuid(let value): return value.description
        case .composite(let values):
            let visible = values.prefix(maximumCollectionElements).map {
                referenceIdentifierDescription($0, depth: depth + 1)
            }
            let suffix = values.count > maximumCollectionElements ? ", …" : ""
            return "[" + visible.joined(separator: ", ") + suffix + "]"
        }
    }

    private static func vectorElementTypeName(
        _ type: VectorElementType
    ) -> String {
        switch type {
        case .int8: return "int8"
        case .int16: return "int16"
        case .int32: return "int32"
        case .int64: return "int64"
        case .uint8: return "uint8"
        case .uint16: return "uint16"
        case .uint32: return "uint32"
        case .uint64: return "uint64"
        case .float32: return "float32"
        case .float64: return "float64"
        }
    }
}
