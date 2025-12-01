// IndexAnnotatable.swift
// FDBIndexing - Abstract index metadata protocol
//
// Part of the FDB Record Layer abstraction redesign.
// This protocol provides a minimal, FDB-independent way to declare
// index metadata for models.

import Core

/// Protocol for types that can provide index metadata.
///
/// This protocol is designed to be implemented by:
/// - `@Recordable` macro-generated code (FDBRecordCore)
/// - Manual implementations for custom models
///
/// **Design Goals**:
/// - Zero FDB dependencies (Swift stdlib + Foundation only)
/// - Codable-friendly (uses String for field names, not KeyPath)
/// - Extensible (new index kinds via IndexKind enum)
/// - Macro-friendly (simple static property)
///
/// **Example**:
/// ```swift
/// @Recordable
/// struct User {
///     #PrimaryKey<User>([\.userID])
///     #Index<User>([\.email])
///
///     var userID: Int64
///     var email: String
/// }
///
/// // Macro generates:
/// extension User: IndexAnnotatable {
///     static var indexDescriptors: [IndexDescriptor] {
///         [
///             IndexDescriptor(
///                 name: "User_email",
///                 keyPaths: ["email"],
///                 kind: .scalar,
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
