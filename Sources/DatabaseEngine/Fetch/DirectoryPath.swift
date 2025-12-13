// DirectoryPath.swift
// DatabaseEngine - Directory path resolution for dynamic directories

import Foundation
import Core
import FoundationDB

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

    /// Add a field value
    public mutating func set<V: Sendable>(_ keyPath: KeyPath<T, V>, to value: V) {
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

    /// Resolve to path string components
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
    /// Returns true if directoryPathComponents contains any Field<Self>
    public static var hasDynamicDirectory: Bool {
        directoryPathComponents.contains { $0 is any DynamicDirectoryElement }
    }

    /// Extract Field keyPaths from directoryPathComponents
    public static var directoryFieldKeyPaths: [PartialKeyPath<Self>] {
        directoryPathComponents.compactMap { ($0 as? Field<Self>)?.value }
    }

    /// Get field names for directory Field components
    public static var directoryFieldNames: [String] {
        directoryPathComponents.compactMap { component -> String? in
            guard let dynamicElement = component as? any DynamicDirectoryElement else { return nil }
            return fieldName(for: dynamicElement.anyKeyPath)
        }
    }
}

// MARK: - DirectoryPathError

/// Errors related to directory path resolution
public enum DirectoryPathError: Error, CustomStringConvertible, Sendable {
    /// Required fields are missing
    case missingFields([String])

    /// Type has dynamic directory but field values not provided
    case dynamicFieldsRequired(typeName: String, fields: [String])

    public var description: String {
        switch self {
        case .missingFields(let fields):
            return "Missing directory field values: \(fields.joined(separator: ", ")). " +
                   "Use .partition() to specify values for all Field components."

        case .dynamicFieldsRequired(let typeName, let fields):
            return "Type '\(typeName)' requires field values for directory resolution: " +
                   "\(fields.joined(separator: ", ")). " +
                   "Use .partition(\\.\(fields.first ?? "field"), equals: value)."
        }
    }
}

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

    public func resolve() -> [String] {
        _resolve()
    }

    public func validate() throws {
        try _validate()
    }
}
