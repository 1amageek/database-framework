// RelationshipIndexError.swift
// RelationshipIndex - Error types for relationship index operations
//
// Defines errors for invalid configurations, FK types, and field conversions.

import Foundation

/// Errors that can occur during relationship index operations
public enum RelationshipIndexError: Error, CustomStringConvertible {
    /// Configuration not found for the index
    case configurationNotFound(indexName: String, modelType: String)

    /// relatedItemLoader is required but was not provided
    case relatedItemLoaderRequired(indexName: String)

    /// FK field type is invalid (must be String or [String])
    case invalidForeignKeyType(fieldName: String, expectedType: String, actualType: String)

    /// Related field value is nil - cannot create index entry
    case relatedFieldIsNil(fieldName: String, relatedType: String)

    /// Field value cannot be converted to TupleElement
    case fieldNotConvertibleToTupleElement(fieldName: String, relatedType: String, actualType: String)

    /// Transaction is required for computing index keys (RelationshipIndex requires DB access)
    case transactionRequired(indexName: String)

    public var description: String {
        switch self {
        case .configurationNotFound(let indexName, let modelType):
            return "RelationshipIndexConfiguration not found for index '\(indexName)' on type '\(modelType)'"
        case .relatedItemLoaderRequired(let indexName):
            return "relatedItemLoader is required for RelationshipIndex '\(indexName)'"
        case .invalidForeignKeyType(let fieldName, let expectedType, let actualType):
            return "FK field '\(fieldName)' must be \(expectedType), got \(actualType)"
        case .relatedFieldIsNil(let fieldName, let relatedType):
            return "Related field '\(fieldName)' on '\(relatedType)' is nil - cannot create index entry"
        case .fieldNotConvertibleToTupleElement(let fieldName, let relatedType, let actualType):
            return "Field '\(fieldName)' on '\(relatedType)' (type: \(actualType)) cannot be converted to TupleElement"
        case .transactionRequired(let indexName):
            return "RelationshipIndex '\(indexName)' requires transaction access to compute index keys. Use computeIndexKeys(for:id:transaction:) instead."
        }
    }
}
