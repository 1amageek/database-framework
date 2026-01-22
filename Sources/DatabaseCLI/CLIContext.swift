// CLIContext.swift
// DatabaseCLI - Execution context for CLI operations
//
// Holds FDBContainer, Schema, and provides type-erased database operations.

import Foundation
import DatabaseEngine
import Core
import FoundationDB

/// CLI execution context
///
/// Provides access to the database and schema, with type-erased operations
/// for the interactive CLI.
public final class CLIContext: Sendable {

    // MARK: - Properties

    /// The FDB container
    public let container: FDBContainer

    /// The schema
    public var schema: Schema {
        container.schema
    }

    // MARK: - Initialization

    /// Create CLI context with schema
    ///
    /// - Parameters:
    ///   - schema: The schema defining all entities
    ///   - clusterFile: Optional path to FDB cluster file
    public init(schema: Schema, clusterFile: String? = nil) throws {
        let config = clusterFile.map { FDBConfiguration(url: URL(fileURLWithPath: $0)) }
        self.container = try FDBContainer(
            for: schema,
            configuration: config,
            security: .disabled
        )
    }

    /// Create CLI context with existing container
    ///
    /// - Parameter container: The FDB container
    public init(container: FDBContainer) {
        self.container = container
    }

    // MARK: - Entity Access

    /// Get entity by name
    ///
    /// - Parameter name: The entity name
    /// - Returns: The entity if found
    public func entity(named name: String) -> Schema.Entity? {
        schema.entity(named: name)
    }

    /// Get all entity names
    public var entityNames: [String] {
        schema.entities.map(\.name)
    }

    // MARK: - Type-Erased Operations

    /// Fetch an item by ID (type-erased)
    ///
    /// - Parameters:
    ///   - typeName: The entity type name
    ///   - id: The item ID as string
    /// - Returns: The item as JSON string if found
    public func fetchItem(typeName: String, id: String) async throws -> String? {
        guard let entity = schema.entity(named: typeName) else {
            throw CLIError.unknownType(typeName)
        }

        let context = container.newContext()
        let type = entity.persistableType

        // Use type-erased fetch via protocol dispatch
        guard let item = try await performFetch(type: type, id: id, context: context) else {
            return nil
        }

        return try encodeToJSON(item)
    }

    /// Fetch all items of a type (type-erased)
    ///
    /// - Parameters:
    ///   - typeName: The entity type name
    ///   - limit: Maximum number of items to return
    /// - Returns: Array of items as JSON strings
    public func fetchItems(typeName: String, limit: Int = 100) async throws -> [String] {
        guard let entity = schema.entity(named: typeName) else {
            throw CLIError.unknownType(typeName)
        }

        let context = container.newContext()
        let type = entity.persistableType
        let items = try await performFetchAll(type: type, limit: limit, context: context)

        return try items.map { try encodeToJSON($0) }
    }

    /// Count items of a type
    ///
    /// - Parameter typeName: The entity type name
    /// - Returns: The count
    public func countItems(typeName: String) async throws -> Int {
        guard let entity = schema.entity(named: typeName) else {
            throw CLIError.unknownType(typeName)
        }

        let context = container.newContext()
        let type = entity.persistableType
        return try await performCount(type: type, context: context)
    }

    /// Delete an item by ID
    ///
    /// - Parameters:
    ///   - typeName: The entity type name
    ///   - id: The item ID as string
    /// - Returns: True if deleted, false if not found
    public func deleteItem(typeName: String, id: String) async throws -> Bool {
        guard let entity = schema.entity(named: typeName) else {
            throw CLIError.unknownType(typeName)
        }

        let context = container.newContext()
        let type = entity.persistableType
        return try await performDelete(type: type, id: id, context: context)
    }

    // MARK: - Index Operations

    /// Get all index descriptors
    public var allIndexDescriptors: [IndexDescriptor] {
        schema.indexDescriptors
    }

    /// Get index descriptors for a type
    public func indexDescriptors(for typeName: String) -> [IndexDescriptor] {
        guard let entity = schema.entity(named: typeName) else {
            return []
        }
        return entity.indexDescriptors
    }

    // MARK: - Private Helpers - Type Dispatch

    private func performFetch(
        type: any Persistable.Type,
        id: String,
        context: FDBContext
    ) async throws -> (any Persistable)? {
        try await withCheckedThrowingContinuation { continuation in
            Task {
                do {
                    let result = try await self.fetchWithType(type, id: id, context: context)
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private func fetchWithType<T: Persistable>(
        _ type: T.Type,
        id: String,
        context: FDBContext
    ) async throws -> (any Persistable)? {
        return try await context.model(for: id, as: type)
    }

    private func performFetchAll(
        type: any Persistable.Type,
        limit: Int,
        context: FDBContext
    ) async throws -> [any Persistable] {
        try await withCheckedThrowingContinuation { continuation in
            Task {
                do {
                    let result = try await self.fetchAllWithType(type, limit: limit, context: context)
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private func fetchAllWithType<T: Persistable>(
        _ type: T.Type,
        limit: Int,
        context: FDBContext
    ) async throws -> [any Persistable] {
        let results = try await context.fetch(type).limit(limit).execute()
        return results.map { $0 as any Persistable }
    }

    private func performCount(
        type: any Persistable.Type,
        context: FDBContext
    ) async throws -> Int {
        try await withCheckedThrowingContinuation { continuation in
            Task {
                do {
                    let result = try await self.countWithType(type, context: context)
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private func countWithType<T: Persistable>(
        _ type: T.Type,
        context: FDBContext
    ) async throws -> Int {
        return try await context.fetch(type).count()
    }

    private func performDelete(
        type: any Persistable.Type,
        id: String,
        context: FDBContext
    ) async throws -> Bool {
        try await withCheckedThrowingContinuation { continuation in
            Task {
                do {
                    let result = try await self.deleteWithType(type, id: id, context: context)
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private func deleteWithType<T: Persistable>(
        _ type: T.Type,
        id: String,
        context: FDBContext
    ) async throws -> Bool {
        guard let item = try await context.model(for: id, as: type) else {
            return false
        }
        context.delete(item)
        try await context.save()
        return true
    }

    // MARK: - JSON Encoding

    private func encodeToJSON(_ item: any Persistable) throws -> String {
        // Build JSON from item fields using dynamicMember
        var dict: [String: Any] = [:]

        let itemType = type(of: item)
        let allFields = itemType.allFields

        for field in allFields {
            if let value = item[dynamicMember: field] {
                dict[field] = convertToJSONValue(value)
            }
        }

        let data = try JSONSerialization.data(
            withJSONObject: dict,
            options: [.prettyPrinted, .sortedKeys]
        )
        return String(data: data, encoding: .utf8) ?? "{}"
    }

    private func convertToJSONValue(_ value: any Sendable) -> Any {
        // Handle common types
        if let string = value as? String {
            return string
        }
        if let int = value as? Int {
            return int
        }
        if let int64 = value as? Int64 {
            return int64
        }
        if let double = value as? Double {
            return double
        }
        if let bool = value as? Bool {
            return bool
        }
        if let date = value as? Date {
            let formatter = ISO8601DateFormatter()
            return formatter.string(from: date)
        }
        if let array = value as? [any Sendable] {
            return array.map { convertToJSONValue($0) }
        }
        // Fallback to string description
        return "\(value)"
    }
}

// MARK: - CLI Errors

/// Errors specific to CLI operations
public enum CLIError: Error, CustomStringConvertible {
    /// Unknown entity type
    case unknownType(String)

    /// Item not found
    case itemNotFound(type: String, id: String)

    /// Invalid JSON
    case invalidJSON(String)

    /// Operation failed
    case operationFailed(String)

    /// Not connected
    case notConnected

    public var description: String {
        switch self {
        case .unknownType(let name):
            return "Unknown type: \(name). Use 'schema list' to see available types."
        case .itemNotFound(let type, let id):
            return "\(type) with id '\(id)' not found."
        case .invalidJSON(let message):
            return "Invalid JSON: \(message)"
        case .operationFailed(let message):
            return "Operation failed: \(message)"
        case .notConnected:
            return "Not connected to database. Use 'connect' to connect."
        }
    }
}
