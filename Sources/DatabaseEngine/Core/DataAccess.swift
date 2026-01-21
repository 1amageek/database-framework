import Foundation
import FoundationDB
import Core

/// Static utility for accessing Persistable item data
///
/// DataAccess provides static functions for extracting metadata and field values
/// from Persistable items. It uses the @dynamicMemberLookup subscript for field
/// access and ProtobufEncoder/Decoder for serialization.
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
    /// This method uses the Visitor pattern to traverse the KeyExpression tree
    /// and extract the corresponding values from the item using Persistable's subscript.
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
        let visitor = DataAccessEvaluator(item: item)
        return try expression.accept(visitor: visitor)
    }

    /// Extract a single field value using Persistable's subscript
    ///
    /// This method is called by the KeyExpression evaluator.
    ///
    /// **Field Name Format**:
    /// - Simple field: "email", "price"
    /// - Nested field: "address.city", "user.profile.name" (dot notation)
    ///
    /// **Nested Field Support**:
    /// Nested fields are accessed using Mirror reflection. The path is split by "."
    /// and each component is traversed to reach the final value.
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
        // Handle nested keyPaths (e.g., "user.address.city")
        if keyPath.contains(".") {
            let components = keyPath.split(separator: ".").map(String.init)
            return try extractNestedField(from: item, components: components, fullPath: keyPath)
        }

        // Use Persistable's subscript for top-level fields
        guard let value = item[dynamicMember: keyPath] else {
            throw DataAccessError.fieldNotFound(
                itemType: Item.persistableType,
                keyPath: keyPath
            )
        }

        // Convert to TupleElement
        return try convertToTupleElements(value)
    }

    /// Extract a nested field value using Mirror reflection
    ///
    /// - Parameters:
    ///   - item: The root item to extract from
    ///   - components: Path components (e.g., ["address", "city"])
    ///   - fullPath: Full dot-notation path for error messages
    /// - Returns: Array of tuple elements
    /// - Throws: Error if field not found at any level
    private static func extractNestedField<Item: Persistable>(
        from item: Item,
        components: [String],
        fullPath: String
    ) throws -> [any TupleElement] {
        guard !components.isEmpty else {
            throw DataAccessError.fieldNotFound(
                itemType: Item.persistableType,
                keyPath: fullPath
            )
        }

        var currentValue: Any = item
        var traversedPath: [String] = []

        for component in components {
            traversedPath.append(component)

            // Try Persistable subscript first (for top-level on Persistable types)
            if let persistable = currentValue as? any Persistable,
               let value = persistable[dynamicMember: component] {
                currentValue = value
                continue
            }

            // Fall back to Mirror reflection for nested structs
            let mirror = Mirror(reflecting: currentValue)
            var found = false

            for child in mirror.children {
                if child.label == component {
                    // Handle Optional values
                    if let optional = child.value as? (any _OptionalProtocol) {
                        if optional._isNil {
                            throw DataAccessError.nilValueCannotBeIndexed
                        }
                        if let unwrapped = optional._unwrappedAny {
                            currentValue = unwrapped
                        } else {
                            throw DataAccessError.nilValueCannotBeIndexed
                        }
                    } else {
                        currentValue = child.value
                    }
                    found = true
                    break
                }
            }

            if !found {
                throw DataAccessError.fieldNotFound(
                    itemType: Item.persistableType,
                    keyPath: traversedPath.joined(separator: ".")
                )
            }
        }

        // Convert final value to TupleElement
        return try convertToTupleElements(currentValue)
    }

    /// Extract id from an item using the id expression
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - idExpression: The KeyExpression defining the id
    /// - Returns: Tuple representing the id
    /// - Throws: Error if extraction fails
    public static func extractId<Item: Persistable>(
        from item: Item,
        using idExpression: KeyExpression
    ) throws -> Tuple {
        let elements = try evaluate(item: item, expression: idExpression)
        return Tuple(elements)
    }

    // MARK: - KeyPath Direct Extraction (Optimized)

    /// Extract field values using KeyPath direct subscript access
    ///
    /// This method uses direct KeyPath subscript access (`item[keyPath: kp]`)
    /// which is more efficient than string-based `@dynamicMemberLookup`.
    ///
    /// **Benefits over string-based extraction**:
    /// - Type-safe at compile time
    /// - Direct memory access without string parsing
    /// - Refactoring-friendly (IDE renames propagate)
    /// - Reduced runtime overhead
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - keyPath: The KeyPath to the field
    /// - Returns: Array of tuple elements representing the extracted value
    /// - Throws: Error if type conversion fails
    public static func extractFieldUsingKeyPath<Item: Persistable, Value>(
        from item: Item,
        keyPath: KeyPath<Item, Value>
    ) throws -> [any TupleElement] {
        let value = item[keyPath: keyPath]
        return try convertToTupleElements(value)
    }

    /// Extract multiple field values using KeyPaths (optimized batch extraction)
    ///
    /// This method extracts values from multiple KeyPaths using direct subscript access.
    /// Prefer this method when `index.keyPaths` is available.
    ///
    /// **Usage**:
    /// ```swift
    /// if let keyPaths = index.keyPaths {
    ///     let values = try DataAccess.extractFieldsUsingKeyPaths(from: user, keyPaths: keyPaths)
    /// } else {
    ///     let values = try DataAccess.evaluate(item: user, expression: index.rootExpression)
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - keyPaths: Array of KeyPaths to extract
    /// - Returns: Array of tuple elements representing all extracted values
    /// - Throws: Error if type conversion fails
    public static func extractFieldsUsingKeyPaths<Item: Persistable>(
        from item: Item,
        keyPaths: [AnyKeyPath]
    ) throws -> [any TupleElement] {
        var result: [any TupleElement] = []
        for anyKeyPath in keyPaths {
            // Try to cast to PartialKeyPath<Item> for direct access
            if let partialKeyPath = anyKeyPath as? PartialKeyPath<Item> {
                let value = item[keyPath: partialKeyPath]
                let tupleElements = try convertToTupleElements(value)
                result.append(contentsOf: tupleElements)
            } else {
                // Fallback: This shouldn't happen if keyPaths were created correctly
                // from the same Item type, but handle gracefully
                throw DataAccessError.keyPathTypeMismatch(
                    expectedType: Item.persistableType,
                    keyPath: String(describing: anyKeyPath)
                )
            }
        }
        return result
    }

    /// Evaluate index field values with KeyPath optimization
    ///
    /// This method uses direct KeyPath extraction when available, falling back
    /// to KeyExpression-based extraction for backward compatibility.
    ///
    /// **Recommended for IndexMaintainer implementations**:
    /// ```swift
    /// let fieldValues = try DataAccess.evaluateIndexFields(
    ///     from: item,
    ///     keyPaths: index.keyPaths,
    ///     expression: index.rootExpression
    /// )
    /// ```
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - keyPaths: Optional KeyPaths for direct extraction
    ///   - expression: KeyExpression fallback for string-based extraction
    /// - Returns: Array of tuple elements representing the extracted values
    /// - Throws: Error if extraction fails
    public static func evaluateIndexFields<Item: Persistable>(
        from item: Item,
        keyPaths: [AnyKeyPath]?,
        expression: KeyExpression
    ) throws -> [any TupleElement] {
        // Use KeyPath direct extraction when available (optimized path)
        if let keyPaths = keyPaths {
            return try extractFieldsUsingKeyPaths(from: item, keyPaths: keyPaths)
        }

        // Fallback to KeyExpression-based extraction (backward compatibility)
        return try evaluate(item: item, expression: expression)
    }

    /// Extract Range boundary value
    ///
    /// Extracts the lowerBound or upperBound from a Range-type field.
    ///
    /// **Supported Range types**:
    /// - Range<Bound>: Half-open range [a, b)
    /// - ClosedRange<Bound>: Closed range [a, b]
    /// - PartialRangeFrom<Bound>: [a, ∞)
    /// - PartialRangeThrough<Bound>: (-∞, b]
    /// - PartialRangeUpTo<Bound>: (-∞, b)
    ///
    /// **Default Implementation**: Throws error (not supported)
    /// Upper layers should implement Range-type field handling if needed.
    ///
    /// - Parameters:
    ///   - item: The item to extract from
    ///   - keyPath: The field name containing the Range type
    ///   - component: The boundary component to extract (lowerBound/upperBound)
    /// - Returns: Array containing the boundary value as TupleElement
    /// - Throws: Error indicating Range fields are not supported
    public static func extractRangeBoundary<Item: Persistable>(
        from item: Item,
        keyPath: String,
        component: RangeComponent
    ) throws -> [any TupleElement] {
        throw DataAccessError.rangeFieldsNotSupported(
            itemType: Item.persistableType,
            suggestion: "Range-type fields are not yet supported in this version"
        )
    }

    // MARK: - Serialization

    /// Serialize an item to bytes using ProtobufEncoder
    ///
    /// - Parameter item: The item to serialize
    /// - Returns: Serialized bytes
    /// - Throws: Error if serialization fails
    public static func serialize<Item: Persistable>(_ item: Item) throws -> FDB.Bytes {
        let encoder = ProtobufEncoder()
        let data = try encoder.encode(item)
        return Array(data)
    }

    /// Deserialize bytes to an item using ProtobufDecoder
    ///
    /// - Parameter bytes: The bytes to deserialize
    /// - Returns: Deserialized item
    /// - Throws: Error if deserialization fails
    public static func deserialize<Item: Persistable>(_ bytes: FDB.Bytes) throws -> Item {
        let decoder = ProtobufDecoder()
        return try decoder.decode(Item.self, from: Data(bytes))
    }

    /// Deserialize bytes to a type-erased Persistable using runtime type
    ///
    /// Used for polymorphic deserialization where the concrete type is known at runtime.
    ///
    /// - Parameters:
    ///   - bytes: The bytes to deserialize
    ///   - type: The concrete Persistable type to decode as
    /// - Returns: Deserialized item (type-erased)
    /// - Throws: Error if deserialization fails
    public static func deserializeAny(
        _ bytes: FDB.Bytes,
        as type: any (Persistable & Codable).Type
    ) throws -> any Persistable {
        let decoder = ProtobufDecoder()
        let decoded = try decoder.decodeAny(type, from: Data(bytes))
        guard let persistable = decoded as? any Persistable else {
            throw FDBRuntimeError.internalError("Decoded value is not Persistable")
        }
        return persistable
    }

    // MARK: - Covering Index Support (Optional)

    /// Reconstruct an item from covering index key and value
    ///
    /// This method enables covering index optimization by reconstructing
    /// items directly from index data without fetching from storage.
    ///
    /// **Default Implementation**: Throws error (not supported)
    /// Upper layers should implement reconstruction if they support covering indexes.
    ///
    /// **Index Key Structure**: `<indexSubspace><rootExpression fields><id fields>`
    ///
    /// - Parameters:
    ///   - indexKey: The index key (unpacked tuple)
    ///   - indexValue: The index value (packed covering fields)
    ///   - idExpression: ID expression for field extraction
    /// - Returns: Reconstructed item
    /// - Throws: Error indicating reconstruction is not supported
    public static func reconstruct<Item: Persistable>(
        indexKey: Tuple,
        indexValue: FDB.Bytes,
        idExpression: KeyExpression
    ) throws -> Item {
        throw DataAccessError.reconstructionNotSupported(
            itemType: Item.persistableType,
            suggestion: "Covering index reconstruction is not yet supported in this version"
        )
    }

    // MARK: - Private Type Conversion

    /// Convert any value to TupleElements
    ///
    /// Delegates to TupleElementConverter and converts errors to DataAccessError
    /// for backward compatibility with existing code.
    ///
    /// - Parameter value: The value to convert
    /// - Returns: Array of TupleElements
    /// - Throws: DataAccessError on failure
    private static func convertToTupleElements(_ value: Any) throws -> [any TupleElement] {
        do {
            return try TupleElementConverter.convert(value)
        } catch let error as TupleConversionError {
            // Convert to DataAccessError for backward compatibility
            switch error {
            case .nilValueCannotBeConverted:
                throw DataAccessError.nilValueCannotBeIndexed
            case .integerOverflow(let val, let targetType):
                throw DataAccessError.integerOverflow(value: val, targetType: targetType)
            case .unsupportedType(let actualType):
                throw DataAccessError.unsupportedType(actualType: actualType)
            case .emptyConversionResult:
                throw DataAccessError.unsupportedType(actualType: "empty result")
            case .multipleElementsNotAllowed(let count):
                throw DataAccessError.unsupportedType(actualType: "multiple elements (\(count))")
            }
        }
    }
}

// MARK: - DataAccessEvaluator

/// Visitor that evaluates KeyExpressions using DataAccess
///
/// This visitor traverses a KeyExpression tree and extracts values from an item
/// using DataAccess static methods.
private struct DataAccessEvaluator<Item: Persistable>: KeyExpressionVisitor {
    let item: Item

    typealias Result = [any TupleElement]

    func visitField(_ fieldName: String) throws -> [any TupleElement] {
        return try DataAccess.extractField(from: item, keyPath: fieldName)
    }

    func visitConcatenate(_ expressions: [KeyExpression]) throws -> [any TupleElement] {
        var result: [any TupleElement] = []
        for expression in expressions {
            let values = try expression.accept(visitor: self)
            result.append(contentsOf: values)
        }
        return result
    }

    func visitLiteral(_ value: any TupleElement) throws -> [any TupleElement] {
        return [value]
    }

    func visitEmpty() throws -> [any TupleElement] {
        return []
    }

    func visitRangeBoundary(_ fieldName: String, _ component: RangeComponent) throws -> [any TupleElement] {
        return try DataAccess.extractRangeBoundary(
            from: item,
            keyPath: fieldName,
            component: component
        )
    }

    func visitNest(_ parentField: String, _ child: KeyExpression) throws -> [any TupleElement] {
        // Build the full nested path by recursively flattening the expression
        let fullPath = buildNestedPath(parentField: parentField, child: child)
        return try DataAccess.extractField(from: item, keyPath: fullPath)
    }

    /// Build a dot-notation path from nested expressions
    private func buildNestedPath(parentField: String, child: KeyExpression) -> String {
        if let fieldExpr = child as? FieldKeyExpression {
            return "\(parentField).\(fieldExpr.fieldName)"
        }

        if let nestExpr = child as? NestExpression {
            let childPath = buildNestedPath(parentField: nestExpr.parentField, child: nestExpr.child)
            return "\(parentField).\(childPath)"
        }

        // For other expression types, just use the parent field
        // (this shouldn't happen in normal usage)
        return parentField
    }
}

// MARK: - Errors

/// Errors that can occur during DataAccess operations
public enum DataAccessError: Error, CustomStringConvertible {
    case fieldNotFound(itemType: String, keyPath: String)
    case rangeFieldsNotSupported(itemType: String, suggestion: String)
    case reconstructionNotSupported(itemType: String, suggestion: String)
    case typeMismatch(itemType: String, keyPath: String, expected: String, actual: String)
    case nilValueCannotBeIndexed
    case integerOverflow(value: UInt64, targetType: String)
    case unsupportedType(actualType: String)

    /// KeyPath type mismatch during direct extraction
    ///
    /// This occurs when a KeyPath cannot be cast to `PartialKeyPath<Item>`,
    /// indicating the KeyPath was created for a different type.
    case keyPathTypeMismatch(expectedType: String, keyPath: String)

    public var description: String {
        switch self {
        case .fieldNotFound(let itemType, let keyPath):
            return "Field '\(keyPath)' not found in \(itemType)"
        case .rangeFieldsNotSupported(let itemType, let suggestion):
            return "Range fields not supported for \(itemType). \(suggestion)"
        case .reconstructionNotSupported(let itemType, let suggestion):
            return "Reconstruction not supported for \(itemType). \(suggestion)"
        case .typeMismatch(let itemType, let keyPath, let expected, let actual):
            return "Type mismatch for field '\(keyPath)' in \(itemType): expected \(expected), got \(actual)"
        case .nilValueCannotBeIndexed:
            return "Nil values cannot be indexed. Optional fields with nil values should use sparse indexes or be excluded from indexing."
        case .integerOverflow(let value, let targetType):
            return "Integer overflow: value \(value) exceeds maximum for \(targetType) (\(Int64.max))"
        case .unsupportedType(let actualType):
            return "Unsupported type '\(actualType)' for indexing. Supported types: String, Int, Int64, UInt64 (≤ Int64.max), Double, Float, Bool, UUID, Data, [UInt8], Tuple"
        case .keyPathTypeMismatch(let expectedType, let keyPath):
            return "KeyPath '\(keyPath)' cannot be cast to PartialKeyPath<\(expectedType)>. Ensure the KeyPath was created for the correct type."
        }
    }
}
