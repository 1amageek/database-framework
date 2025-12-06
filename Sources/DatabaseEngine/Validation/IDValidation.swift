import FoundationDB
import Core

/// Error thrown when a Persistable's ID type doesn't conform to TupleElement
public struct IDTypeValidationError: Error, CustomStringConvertible {
    public let itemType: String
    public let idType: Any.Type

    public init(itemType: String, idType: Any.Type) {
        self.itemType = itemType
        self.idType = idType
    }

    public var description: String {
        """
        IDTypeValidationError: The ID type '\(idType)' of '\(itemType)' does not conform to TupleElement.

        Supported ID types:
        - String (recommended: ULID for sortable unique IDs)
        - Int64, Int32, Int16, Int8, Int
        - UInt64, UInt32, UInt16, UInt8, UInt
        - UUID
        - Double, Float
        - Bool
        - Data, [UInt8]

        To fix this, change your ID type to one of the supported types:

            @Persistable
            struct \(itemType) {
                var id: String = ULID().ulidString  // Use String instead
                // ...
            }
        """
    }
}

/// Validates that an ID value can be used with FDBRuntime
///
/// This function checks if the given ID value conforms to TupleElement,
/// which is required for FDB key encoding.
///
/// - Parameters:
///   - id: The ID value to validate
///   - itemType: The item type name (for error messages)
/// - Returns: The ID as `any TupleElement`
/// - Throws: `IDTypeValidationError` if the ID type doesn't conform to TupleElement
public func validateID<ID>(_ id: ID, for itemType: String) throws -> any TupleElement {
    guard let tupleElement = id as? (any TupleElement) else {
        throw IDTypeValidationError(itemType: itemType, idType: type(of: id))
    }
    return tupleElement
}

/// Extension to validate Persistable ID types
public extension Persistable {
    /// Validates that this instance's ID can be used with FDBRuntime
    ///
    /// Call this before attempting to save/load/delete with FDBStore or FDBContext.
    ///
    /// - Returns: The ID as `any TupleElement`
    /// - Throws: `IDTypeValidationError` if the ID type doesn't conform to TupleElement
    ///
    /// **Usage**:
    /// ```swift
    /// let user = User(email: "test@example.com", name: "Test")
    /// let validatedID = try user.validateIDForStorage()
    /// context.insert(data: data, for: "User", id: validatedID, subspace: subspace)
    /// ```
    func validateIDForStorage() throws -> any TupleElement {
        return try validateID(id, for: Self.persistableType)
    }
}
