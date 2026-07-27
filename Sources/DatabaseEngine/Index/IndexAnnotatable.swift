// IndexAnnotatable.swift
// DatabaseEngine - Abstract index metadata protocol
//
// This protocol provides the database-domain contract for declaring
// index metadata on persisted models and documents.

import DatabaseKit

/// Protocol for types that can provide index metadata.
///
/// This protocol is designed to be implemented by:
/// - `@Persistable` macro-generated code
/// - Manual implementations for custom models
///
/// **Design Goals**:
/// - Storage-engine independent
/// - Uses stable field names in descriptors
/// - Extensible (new index kinds via IndexKind enum)
/// - Macro-friendly (simple static property)
///
/// **Example**:
/// ```swift
/// @Persistable
/// struct User {
///     #Index(
///         .scalar,
///         fields: [\User.email],
///         name: "User_email"
///     )
///
///     var id: Int64
///     var email: String
/// }
///
/// // Macro generates:
/// extension User: IndexAnnotatable {
///     static var indexDescriptors: [IndexDescriptor] {
///         [
///             try IndexDescriptor(
///                 name: "User_email",
///                 definition: .scalar,
///                 fields: [User.fields.email.ascending],
///                 commonOptions: .init()
///             )
///         ]
///     }
/// }
/// ```
public protocol IndexAnnotatable {
    /// Array of index descriptors for this type.
    ///
    /// Each descriptor declares:
    /// - Index name (unique identifier)
    /// - Field names to index (string representation of KeyPaths)
    /// - Index kind (scalar, vector, spatial, etc.)
    /// - Optional configuration (commonOptions + kind-specific options)
    ///
    /// **Note**: This is declarative metadata only, no execution logic.
    static var indexDescriptors: [IndexDescriptor] { get }
}
