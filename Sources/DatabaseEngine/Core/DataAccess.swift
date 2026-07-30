import StorageKit
import DatabaseKit
import DatabaseTypes
import DatabaseWire

/// Static utility for accessing Persistable item data
///
/// DataAccess provides static functions for extracting canonical field values
/// from compiled Persistable models and for storage serialization.
///
/// **Design**: Stateless namespace with generic static functions
/// **No instantiation needed**: All methods are static
///
/// **Usage Example**:
/// ```swift
/// @Persistable
/// struct User {
///     var userID: Int64
///     var email: String
/// }
///
/// let user = User(userID: 123, email: "user@example.com")
///
/// // Extract field
/// let emailValue = try DataAccess.extractField(from: user, keyPath: "email")
///
/// // Evaluate KeyExpression
/// let values = try DataAccess.evaluate(item: user, expression: emailIndex.rootExpression)
///
/// // Serialize
/// let bytes = try DataAccess.serialize(user)
///
/// // Deserialize
/// let restored: User = try DataAccess.deserialize(bytes)
/// ```
public struct DataAccess: Sendable {
    // Private init to prevent instantiation
    private init() {}

    // MARK: - KeyExpression Evaluation

    /// Evaluate a KeyExpression to extract field values
    ///
    /// This method traverses the expression and resolves every field through
    /// macro-generated canonical field access.
    ///
    /// - Parameters:
    ///   - item: The item to evaluate
    ///   - expression: The KeyExpression to evaluate
    /// - Returns: Array of tuple elements representing the extracted values
    /// - Throws: Error if field access fails
    public static func evaluate<Item: Persistable>(
        item: Item,
        expression: KeyExpression
    ) throws -> [any TupleElement] {
        try expression.evaluate { fieldPath in
            try extractField(from: item, keyPath: fieldPath)
        }
    }

    public static func evaluate(
        model: PersistedModel,
        expression: KeyExpression
    ) throws -> [any TupleElement] {
        try expression.evaluate { fieldPath in
            try extractField(from: model, keyPath: fieldPath)
        }
    }

    /// Extract a field value through its compiled schema identity.
    ///
    /// This method is called by the KeyExpression evaluator.
    ///
    /// **Field Name Format**:
    /// - Simple field: "email", "price"
    /// - Nested field: "address.city", "user.profile.name" (dot notation)
    ///
    /// **Nested Field Support**:
    /// Nested fields traverse canonical `FieldObject` values. Runtime reflection
    /// and dynamic-member lookup are intentionally excluded from persistence.
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - keyPath: The field name or dot-notation path (e.g., "email", "address.city")
    /// - Returns: Array of tuple elements (typically single element)
    /// - Throws: Error if field not found or type conversion fails
    public static func extractField<Item: Persistable>(
        from item: Item,
        keyPath: String
    ) throws -> [any TupleElement] {
        let components = keyPath.split(separator: ".", omittingEmptySubsequences: false)
        guard let first = components.first,
              !first.isEmpty,
              let fieldNumber = Item.fieldNumber(for: String(first)),
              let firstValue = try item.persistedFieldValue(
                for: FieldIdentity(name: String(first), number: fieldNumber)
              ) else {
            throw DataAccessError.fieldNotFound(
                itemType: Item.persistableType,
                keyPath: keyPath
            )
        }
        var value = firstValue
        for component in components.dropFirst() {
            guard !component.isEmpty,
                  case .object(let object) = value,
                  let nested = object[String(component)] else {
                throw DataAccessError.fieldNotFound(
                    itemType: Item.persistableType,
                    keyPath: keyPath
                )
            }
            value = nested
        }
        if case .array(let values) = value {
            return try FieldValue.toTupleElements(values)
        }
        return [try value.toTupleElement()]
    }

    public static func extractField(
        from model: PersistedModel,
        keyPath: String
    ) throws -> [any TupleElement] {
        let components = keyPath.split(
            separator: ".",
            omittingEmptySubsequences: false
        )
        guard let first = components.first,
              !first.isEmpty,
              var value = model.value(forFieldNamed: String(first)) else {
            throw DataAccessError.fieldNotFound(
                itemType: model.entity,
                keyPath: keyPath
            )
        }
        for component in components.dropFirst() {
            guard !component.isEmpty,
                  case .object(let object) = value,
                  let nested = object[String(component)] else {
                throw DataAccessError.fieldNotFound(
                    itemType: model.entity,
                    keyPath: keyPath
                )
            }
            value = nested
        }
        if case .array(let values) = value {
            return try FieldValue.toTupleElements(values)
        }
        return [try value.toTupleElement()]
    }

    /// Resolve an item's canonical persistence identifier.
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - idExpression: Either the model's canonical `id` field or a
    ///     pre-resolved canonical tuple supplied by the persistence path.
    /// - Returns: Tuple representing the id
    /// - Throws: Error if extraction fails
    public static func extractId<Item: Persistable>(
        from item: Item,
        using idExpression: KeyExpression
    ) throws -> Tuple {
        if let resolved = idExpression.resolvedTuple {
            if resolved.count == 1 {
                _ = try PersistableIdentifierKeyCodec.value(
                    from: resolved,
                    expectedType: Item.persistableIdentifierType
                )
            } else {
                try PolymorphicIdentifierKey.validate(
                    resolved,
                    for: Item.self
                )
            }
            return resolved
        }

        if idExpression.fieldPath == "id" {
            return try item.persistableIdentifierTuple()
        }

        throw DataAccessError.invalidIdentifierExpression(
            itemType: Item.persistableType,
            actualType: "unsupported identifier expression"
        )
    }

    // MARK: - Serialization

    /// Serialize an item to canonical compiled-entity bytes.
    ///
    /// - Parameter item: The item to serialize
    /// - Returns: Serialized bytes
    /// - Throws: Error if serialization fails
    public static func serialize<Item: Persistable>(_ item: Item) throws -> ByteString {
        try PersistableStorageCodec.encode(item)
    }

    public static func serialize(_ model: PersistedModel) throws -> ByteString {
        try PersistableStorageCodec.encode(model)
    }

    /// Deserialize canonical compiled-entity bytes.
    ///
    /// - Parameter bytes: The bytes to deserialize
    /// - Returns: Deserialized item
    /// - Throws: Error if deserialization fails
    public static func deserialize<Item: Persistable>(_ bytes: ByteString) throws -> Item {
        try PersistableStorageCodec.decode(Item.self, from: bytes)
    }

    public static func deserializePersistedModel(
        _ bytes: ByteString,
        expectedEntity: String? = nil
    ) throws -> PersistedModel {
        try PersistableStorageCodec.decodePersistedModel(
            from: bytes,
            expectedEntity: expectedEntity
        )
    }

}

// MARK: - Errors

/// Errors that can occur during DataAccess operations
public enum DataAccessError: Error, CustomStringConvertible {
    case fieldNotFound(itemType: String, keyPath: String)
    case typeMismatch(itemType: String, keyPath: String, expected: String, actual: String)
    case nilValueCannotBeIndexed
    case unsupportedType(actualType: String)
    case invalidNestedExpression(actualType: String)
    case invalidIdentifierExpression(itemType: String, actualType: String)

    public var description: String {
        switch self {
        case .fieldNotFound(let itemType, let keyPath):
            return "Field '\(keyPath)' not found in \(itemType)"
        case .typeMismatch(let itemType, let keyPath, let expected, let actual):
            return "Type mismatch for field '\(keyPath)' in \(itemType): expected \(expected), got \(actual)"
        case .nilValueCannotBeIndexed:
            return "Nil values cannot be indexed. Optional fields with nil values should use sparse indexes or be excluded from indexing."
        case .unsupportedType(let actualType):
            return "Unsupported type '\(actualType)' for indexing. Supported types: String, signed and unsigned integers, Double, Float, Bool, UUID, Data, [UInt8], Tuple"
        case .invalidNestedExpression(let actualType):
            return "Nested field path cannot contain '\(actualType)'"
        case .invalidIdentifierExpression(let itemType, let actualType):
            return "Identifier for \(itemType) must use its canonical 'id' field or a pre-resolved tuple, got '\(actualType)'"
        }
    }
}
