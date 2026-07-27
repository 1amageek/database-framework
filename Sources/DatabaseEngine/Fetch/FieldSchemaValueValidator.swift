import DatabaseKit
import DatabaseTypes

/// Validates a primitive value against type-independent field metadata.
package enum FieldSchemaValueValidator {
    package static func accepts(
        _ value: FieldValue,
        as type: FieldSchemaType
    ) -> Bool {
        switch (type, value) {
        case (.bool, .bool),
             (.int8, .int8),
             (.int16, .int16),
             (.int32, .int32),
             (.int64, .int64),
             (.uint8, .uint8),
             (.uint16, .uint16),
             (.uint32, .uint32),
             (.uint64, .uint64),
             (.float32, .float32),
             (.float64, .float64),
             (.decimal, .decimal),
             (.string, .string),
             (.bytes, .bytes),
             (.date, .date),
             (.time, .time),
             (.dateTime, .dateTime),
             (.timestamp, .timestamp),
             (.timeSpan, .timeSpan),
             (.calendarPeriod, .calendarPeriod),
             (.geographicPoint, .geographicPoint),
             (.geographicPosition, .geographicPosition),
             (.vector, .vector),
             (.uuid, .uuid),
             (.object, .object),
             (.rdfTerm, .rdfTerm),
             (.reference, .reference),
             (.nested, .object),
             (.enum, .string),
             (.enum, .int64):
            true
        default:
            false
        }
    }
}
