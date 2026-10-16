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
        _ storedElements: [any TupleElement], fieldSchemas: [FieldSchema] = []
    ) throws -> [FieldValue] {
        var values: [FieldValue] = []
        values.reserveCapacity(storedElements.count)
        if fieldSchemas.isEmpty { return try storedElements.map { try FieldValue(tupleElement: $0) } }
        guard storedElements.count <= fieldSchemas.count else {
            throw FieldValueTupleCodecError.invalidDeclaredQueryValue(.incompatibleType)
        }
        for (index, storedElement) in storedElements.enumerated() {
            values.append(try DeclaredTemporalIndexKeyCodec.decode(storedElement, schema: fieldSchemas[index]))
        }
        return values
    }
}
