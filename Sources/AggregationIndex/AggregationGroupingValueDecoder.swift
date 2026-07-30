import DatabaseKit
import DatabaseTypes
import DatabaseEngine
import StorageKit

/// Decodes canonical model values from aggregation storage keys.
///
/// Grouping keys use `FieldValueTupleCodec`, whose encoded element preserves
/// the complete primitive value kind, including integer width and signedness.
/// Raw storage tuple elements are not accepted as an alternate representation.
enum AggregationGroupingValueDecoder {
    static func decode(
        _ storedElements: [any TupleElement]
    ) throws -> [FieldValue] {
        var values: [FieldValue] = []
        values.reserveCapacity(storedElements.count)
        for storedElement in storedElements {
            values.append(try FieldValue(tupleElement: storedElement))
        }
        return values
    }
}
