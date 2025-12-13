// FDBContext+FieldSecurity.swift
// DatabaseEngine - FDBContext extension for field-level security

import Foundation
import Core
import FoundationDB

// MARK: - Secure Fetch

extension FDBContext {

    /// Fetch with field-level security applied (masking restricted fields)
    ///
    /// Uses the current auth context from `AuthContextKey.current` to determine
    /// which fields should be masked.
    ///
    /// **Usage**:
    /// ```swift
    /// // Set auth context
    /// try await AuthContextKey.$current.withValue(userAuth) {
    ///     // Fields user cannot read will be masked (set to default values)
    ///     let employees = try await context.fetchSecure(Employee.self).execute()
    /// }
    /// ```
    ///
    /// - Parameter type: The Persistable type to fetch
    /// - Returns: A SecureQueryExecutor for building the query
    public func fetchSecure<T: Persistable>(_ type: T.Type) -> SecureQueryExecutor<T> {
        SecureQueryExecutor(context: self, auth: AuthContextKey.current)
    }

    /// Fetch a single item by ID with field-level security applied
    ///
    /// - Parameters:
    ///   - id: The item ID
    ///   - type: The Persistable type
    /// - Returns: The masked item, or nil if not found
    public func modelSecure<T: Persistable>(
        for id: some TupleElement,
        as type: T.Type
    ) async throws -> T? {
        guard let item = try await model(for: id, as: type) else {
            return nil
        }
        return FieldSecurityEvaluator.mask(item, auth: AuthContextKey.current)
    }

    /// Fetch a single item by ID with field-level security applied (with partition)
    ///
    /// - Parameters:
    ///   - id: The item ID
    ///   - type: The Persistable type
    ///   - path: The partition path
    /// - Returns: The masked item, or nil if not found
    public func modelSecure<T: Persistable>(
        for id: some TupleElement,
        as type: T.Type,
        partition path: DirectoryPath<T>
    ) async throws -> T? {
        guard let item = try await model(for: id, as: type, partition: path) else {
            return nil
        }
        return FieldSecurityEvaluator.mask(item, auth: AuthContextKey.current)
    }
}

// MARK: - SecureQueryExecutor

/// Query executor that applies field-level security masking to results
public struct SecureQueryExecutor<T: Persistable>: Sendable {
    private let context: FDBContext
    private let auth: (any AuthContext)?
    private var queryExecutor: QueryExecutor<T>

    internal init(context: FDBContext, auth: (any AuthContext)?) {
        self.context = context
        self.auth = auth
        self.queryExecutor = context.fetch(T.self)
    }

    // MARK: - Query Building (delegate to QueryExecutor)

    /// Add a filter condition
    public func `where`(_ predicate: Predicate<T>) -> Self {
        var copy = self
        copy.queryExecutor = queryExecutor.where(predicate)
        return copy
    }

    /// Add sorting (ascending)
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>) -> Self {
        var copy = self
        copy.queryExecutor = queryExecutor.orderBy(keyPath)
        return copy
    }

    /// Add sorting with direction
    public func orderBy<V: Comparable & Sendable>(_ keyPath: KeyPath<T, V>, _ order: SortOrder) -> Self {
        var copy = self
        copy.queryExecutor = queryExecutor.orderBy(keyPath, order)
        return copy
    }

    /// Limit results
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.queryExecutor = queryExecutor.limit(count)
        return copy
    }

    /// Skip results
    public func offset(_ count: Int) -> Self {
        var copy = self
        copy.queryExecutor = queryExecutor.offset(count)
        return copy
    }

    /// Bind a partition field value for dynamic directory resolution
    public func partition<V: Sendable & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> Self {
        var copy = self
        copy.queryExecutor = queryExecutor.partition(keyPath, equals: value)
        return copy
    }

    // MARK: - Execution

    /// Execute the query with field-level security masking
    ///
    /// - Returns: Array of items with restricted fields masked
    public func execute() async throws -> [T] {
        let results = try await queryExecutor.execute()
        return FieldSecurityEvaluator.mask(results, auth: auth)
    }

    /// Execute and return count (no masking needed for count)
    public func count() async throws -> Int {
        try await queryExecutor.count()
    }

    /// Execute and return first result with masking
    public func first() async throws -> T? {
        guard let result = try await queryExecutor.first() else {
            return nil
        }
        return FieldSecurityEvaluator.mask(result, auth: auth)
    }
}

// MARK: - Write Validation

extension FDBContext {

    /// Validate write permissions for a model before saving
    ///
    /// Call this before `context.insert()` and `context.save()` to validate
    /// that the current user has write permission for all modified fields.
    ///
    /// **Usage**:
    /// ```swift
    /// try await AuthContextKey.$current.withValue(userAuth) {
    ///     var employee = existingEmployee
    ///     employee.salary = 100000  // Requires HR role
    ///
    ///     // Validate before inserting
    ///     try context.validateFieldWrite(original: existingEmployee, updated: employee)
    ///
    ///     context.insert(employee)
    ///     try await context.save()
    /// }
    /// ```
    ///
    /// - Parameters:
    ///   - original: Original value (nil for new inserts)
    ///   - updated: Updated value to be saved
    /// - Throws: FieldSecurityError.writeNotAllowed if any field modification is denied
    public func validateFieldWrite<T: Persistable>(
        original: T?,
        updated: T
    ) throws {
        try FieldSecurityEvaluator.validateWrite(
            original: original,
            updated: updated,
            auth: AuthContextKey.current
        )
    }

    /// Validate write permissions for a new model
    ///
    /// - Parameter model: The new model to be inserted
    /// - Throws: FieldSecurityError.writeNotAllowed if any restricted field has non-default value
    public func validateFieldWrite<T: Persistable>(_ model: T) throws {
        try FieldSecurityEvaluator.validateWrite(
            original: nil,
            updated: model,
            auth: AuthContextKey.current
        )
    }
}

// MARK: - Field Access Inspection

extension FDBContext {

    /// Get list of fields that cannot be read by current user for a specific item
    ///
    /// - Parameter item: The item to inspect
    /// - Returns: List of field names that will be masked
    public func unreadableFields<T: Persistable>(in item: T) -> [String] {
        FieldSecurityEvaluator.unreadableFields(in: item, auth: AuthContextKey.current)
    }

    /// Get list of fields that cannot be written by current user for a specific item
    ///
    /// - Parameter item: The item to inspect
    /// - Returns: List of field names that cannot be modified
    public func unwritableFields<T: Persistable>(in item: T) -> [String] {
        FieldSecurityEvaluator.unwritableFields(in: item, auth: AuthContextKey.current)
    }
}

// MARK: - Convenience Methods

extension FDBContext {

    /// Check if current user can read a specific field
    ///
    /// - Parameters:
    ///   - keyPath: The field to check
    ///   - item: The item containing the field
    /// - Returns: true if current user can read the field
    public func canRead<T: Persistable, V>(
        _ keyPath: KeyPath<T, V>,
        in item: T
    ) -> Bool {
        let fieldName = T.fieldName(for: keyPath)
        return FieldSecurityEvaluator.canRead(
            field: fieldName,
            in: item,
            auth: AuthContextKey.current
        )
    }

    /// Check if current user can write a specific field
    ///
    /// - Parameters:
    ///   - keyPath: The field to check
    ///   - item: The item containing the field
    /// - Returns: true if current user can write the field
    public func canWrite<T: Persistable, V>(
        _ keyPath: KeyPath<T, V>,
        in item: T
    ) -> Bool {
        let fieldName = T.fieldName(for: keyPath)
        return FieldSecurityEvaluator.canWrite(
            field: fieldName,
            in: item,
            auth: AuthContextKey.current
        )
    }
}
