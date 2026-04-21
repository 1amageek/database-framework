// DirectoryPath.swift
// DatabaseEngine - Directory path resolution for dynamic directories

import Foundation
import Core
import StorageKit

// MARK: - Helper Function

/// Convert a value to directory path string
///
/// - Parameter value: The value to convert
/// - Returns: String representation for directory path
package func directoryPathString(from value: Any) -> String {
    switch value {
    case let str as String:
        return str
    case let uuid as UUID:
        return uuid.uuidString
    default:
        return "\(value)"
    }
}

// MARK: - DirectoryPath

/// Holds field values needed to resolve a directory path
///
/// Used to capture Field values for directory resolution when
/// the type has a dynamic directory (contains `Field(\.keyPath)` components).
///
/// **Usage**:
/// ```swift
/// // Query-side: via .partition() fluent API
/// let orders = try await context.fetch(Order.self)
///     .partition(\.tenantID, equals: "tenant_123")
///     .execute()
///
/// // From model instance
/// let path = DirectoryPath<Order>.from(order)
/// ```
public struct DirectoryPath<T: Persistable>: @unchecked Sendable {
    /// Field-value pairs for directory resolution
    internal var fieldValues: [(keyPath: PartialKeyPath<T>, value: any Sendable)] = []

    public init() {}

    /// Set a field value (overwrites if already exists)
    public mutating func set<V: Sendable>(_ keyPath: KeyPath<T, V>, to value: V) {
        // Remove existing value for this keyPath if present
        fieldValues.removeAll { $0.keyPath == keyPath }
        fieldValues.append((keyPath, value))
    }

    /// Check if a specific keyPath has a value
    public func hasValue(for keyPath: PartialKeyPath<T>) -> Bool {
        fieldValues.contains { $0.keyPath == keyPath }
    }

    /// Get value for a keyPath
    public func value<V>(for keyPath: KeyPath<T, V>) -> V? {
        fieldValues.first { $0.keyPath == keyPath }?.value as? V
    }

    /// Validate all required directory fields have values
    ///
    /// - Throws: `DirectoryPathError.missingFields` if any required field is missing
    public func validate() throws {
        let requiredKeyPaths = T.directoryFieldKeyPaths
        guard !requiredKeyPaths.isEmpty else { return }

        let providedKeyPaths = Set(fieldValues.map { $0.keyPath as AnyKeyPath })
        let requiredSet = Set(requiredKeyPaths.map { $0 as AnyKeyPath })

        let missing = requiredSet.subtracting(providedKeyPaths)
        guard missing.isEmpty else {
            let fieldNames = missing.compactMap { keyPath -> String? in
                T.fieldName(for: keyPath)
            }
            throw DirectoryPathError.missingFields(fieldNames)
        }
    }

    /// Resolve to path string components.
    ///
    /// - Precondition: `validate()` has succeeded for this path. Unresolved
    ///   `Field` components are **silently skipped** — this is intentional, so
    ///   that `resolve()` stays non-throwing and callable from pure-sync
    ///   contexts (e.g. cache-key generation). Call sites MUST NOT feed the
    ///   result into a live directory subspace or compare it to another
    ///   partition's resolved path without first calling `validate()`, because
    ///   a partial path like `["R", "Order"]` (tenant field missing) can
    ///   collide with an unrelated valid path and silently cross-contaminate
    ///   data or cache entries.
    ///
    /// The engine's `resolveDirectory` path enforces this by always calling
    /// `validate()` before using the resolved components for I/O; cache-key
    /// generation only consults caches that were themselves populated from
    /// validated paths, so a lookup with an invalid path cannot hit. If you
    /// introduce a new call site, preserve this invariant.
    internal func resolve() -> [String] {
        var path: [String] = []
        for component in T.directoryPathComponents {
            if let pathElement = component as? Path {
                path.append(pathElement.value)
            } else if let stringElement = component as? String {
                path.append(stringElement)
            } else if let fieldElement = component as? Field<T> {
                if let field = fieldValues.first(where: { $0.keyPath == fieldElement.value }) {
                    path.append(directoryPathString(from: field.value))
                }
                // else: silently skipped — see precondition above.
            }
        }
        return path
    }

    /// Create from a model instance
    ///
    /// Extracts all Field values from the instance.
    public static func from(_ model: T) -> DirectoryPath<T> {
        var path = DirectoryPath<T>()
        for component in T.directoryPathComponents {
            if let fieldElement = component as? Field<T> {
                let keyPath = fieldElement.value
                let fieldName = T.fieldName(for: keyPath)
                if let value = model[dynamicMember: fieldName] {
                    path.fieldValues.append((keyPath, value))
                }
            }
        }
        return path
    }
}

// MARK: - Persistable Extension

extension Persistable {
    /// Extract Field keyPaths from directoryPathComponents
    public static var directoryFieldKeyPaths: [PartialKeyPath<Self>] {
        directoryPathComponents.compactMap { ($0 as? Field<Self>)?.value }
    }
}

// DirectoryPathError and Persistable.directoryFieldNames are now in database-kit Core module.

// MARK: - Type-Erased DirectoryPath

/// Type-erased wrapper for DirectoryPath
///
/// Used when the generic type is not known at compile time.
public struct AnyDirectoryPath: @unchecked Sendable {
    private let _resolve: () -> [String]
    private let _validate: () throws -> Void

    /// Create from a typed DirectoryPath
    public init<T: Persistable>(_ path: DirectoryPath<T>) {
        self._resolve = { path.resolve() }
        self._validate = { try path.validate() }
    }

    /// Create for a static directory type (no Field components)
    public init(for type: any Persistable.Type) {
        let components = type.directoryPathComponents
        self._resolve = {
            var path: [String] = []
            for component in components {
                if let pathElement = component as? Path {
                    path.append(pathElement.value)
                } else if let stringElement = component as? String {
                    path.append(stringElement)
                }
            }
            return path
        }
        self._validate = {
            let hasDynamic = components.contains { $0 is any DynamicDirectoryElement }
            if hasDynamic {
                throw DirectoryPathError.dynamicFieldsRequired(
                    typeName: type.persistableType,
                    fields: type.directoryFieldNames
                )
            }
        }
    }

    /// Create from field values and type
    public init(fieldValues: [(keyPath: AnyKeyPath, value: any Sendable)], type: any Persistable.Type) {
        let components = type.directoryPathComponents
        self._resolve = {
            var path: [String] = []
            for component in components {
                if let pathElement = component as? Path {
                    path.append(pathElement.value)
                } else if let stringElement = component as? String {
                    path.append(stringElement)
                } else if let dynamicElement = component as? any DynamicDirectoryElement {
                    let keyPath = dynamicElement.anyKeyPath
                    if let field = fieldValues.first(where: { $0.keyPath == keyPath }) {
                        path.append(directoryPathString(from: field.value))
                    }
                }
            }
            return path
        }
        self._validate = {
            var requiredKeyPaths: Set<AnyKeyPath> = []
            for component in components {
                if let dynamicElement = component as? any DynamicDirectoryElement {
                    requiredKeyPaths.insert(dynamicElement.anyKeyPath)
                }
            }

            let providedKeyPaths = Set(fieldValues.map { $0.keyPath })
            let missing = requiredKeyPaths.subtracting(providedKeyPaths)

            guard missing.isEmpty else {
                let fieldNames = components.compactMap { component -> String? in
                    guard let dynamicElement = component as? any DynamicDirectoryElement,
                          missing.contains(dynamicElement.anyKeyPath) else { return nil }
                    return type.fieldName(for: dynamicElement.anyKeyPath)
                }
                throw DirectoryPathError.missingFields(fieldNames)
            }
        }
    }

    /// Resolve to path string components.
    ///
    /// Shares the precondition of `DirectoryPath<T>.resolve()`: unresolved
    /// dynamic `Field` components are silently skipped, so callers must call
    /// `validate()` first before using the result for anything other than
    /// looking up caches that were themselves populated from validated paths.
    public func resolve() -> [String] {
        _resolve()
    }

    public func validate() throws {
        try _validate()
    }
}
