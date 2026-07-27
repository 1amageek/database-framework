import DatabaseKit
import DatabaseTypes

/// A failure while evaluating a compiled query against a persisted model.
public enum QueryEvaluationError: Error, Sendable, Equatable {
    /// The generated model adapter could not encode the selected field.
    case fieldEncoding(PersistableEncodingError)

    /// The selected schema field was not emitted by the generated model adapter.
    case missingField(entity: String, field: FieldIdentity)

    /// The selected field values do not have a defined query ordering.
    case incomparableValues(
        entity: String,
        field: FieldIdentity,
        left: FieldValue,
        right: FieldValue
    )
}
