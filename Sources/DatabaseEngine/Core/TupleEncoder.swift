import DatabaseKit
import DatabaseTypes
import StorageKit

/// Converts canonical database values into ordered storage tuple elements.
///
/// Model adaptation happens in `DatabaseKit`. The execution layer accepts only
/// values that already have a complete `FieldValue` representation, keeping
/// Foundation and runtime reflection outside database execution.
public enum TupleEncoder {
    public static func encode<Value: FieldValueRepresentable>(
        _ value: Value,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        try value.fieldValue.toTupleElement(limits: limits)
    }

    public static func encode(
        _ value: FieldValue,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        try value.toTupleElement(limits: limits)
    }

    public static func encode(
        _ value: RDFTerm,
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> any TupleElement {
        try FieldValue.rdfTerm(value).toTupleElement(limits: limits)
    }

    public static func encode(_ value: Tuple) -> any TupleElement {
        value
    }

    public static func encodeAll<Value: FieldValueRepresentable>(
        _ values: [Value],
        limits: FieldValueTupleCodecLimits = .default
    ) throws(FieldValueTupleCodecError) -> [any TupleElement] {
        var elements: [any TupleElement] = []
        elements.reserveCapacity(values.count)
        for value in values {
            elements.append(
                try value.fieldValue.toTupleElement(limits: limits)
            )
        }
        return elements
    }
}
