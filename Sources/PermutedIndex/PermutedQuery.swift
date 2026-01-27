// PermutedQuery.swift
// PermutedIndex - Query extension for permuted compound indexes
//
// Provides FDBContext extension and query builder for permuted field queries.

import Foundation
import Core
import Permuted
import DatabaseEngine
import FoundationDB

// MARK: - Permuted Entry Point

/// Entry point for permuted index queries
///
/// **Usage**:
/// ```swift
/// import PermutedIndex
///
/// // Query by permuted field order
/// // Original index: (country, city, name) with permutation [1, 0, 2]
/// // Permuted order: (city, country, name)
///
/// // Find all entries in "Tokyo"
/// let results = try await context.permuted(Location.self)
///     .index("Location_permuted_country_city_name")
///     .prefix(["Tokyo"])
///     .execute()
///
/// // Find exact match
/// let exact = try await context.permuted(Location.self)
///     .index("Location_permuted_country_city_name")
///     .exact(["Tokyo", "Japan", "Alice"])
///     .execute()
/// ```
public struct PermutedEntryPoint<T: Persistable>: Sendable {
    private let queryContext: IndexQueryContext

    internal init(queryContext: IndexQueryContext) {
        self.queryContext = queryContext
    }

    /// Specify the permuted index by name
    ///
    /// - Parameter indexName: Name of the permuted index
    /// - Returns: Permuted query builder
    public func index(_ indexName: String) -> PermutedQueryBuilder<T> {
        PermutedQueryBuilder(
            queryContext: queryContext,
            indexName: indexName
        )
    }

    /// Specify the permuted index by fields and permutation
    ///
    /// - Parameters:
    ///   - fields: Original field names
    ///   - permutation: Permutation to apply
    /// - Returns: Permuted query builder
    public func index(fields: [String], permutation: Permutation) -> PermutedQueryBuilder<T> {
        let indexName = "\(T.persistableType)_permuted_\(fields.joined(separator: "_"))"
        return PermutedQueryBuilder(
            queryContext: queryContext,
            indexName: indexName,
            permutation: permutation
        )
    }
}

// MARK: - Permuted Query Builder

/// Builder for permuted index queries
///
/// Supports prefix and exact match queries on permuted field orderings.
public struct PermutedQueryBuilder<T: Persistable>: Sendable {
    // MARK: - Types

    /// Query type
    enum QueryType: Sendable {
        case prefix([any TupleElement & Sendable])
        case exact([any TupleElement & Sendable])
        case all
    }

    // MARK: - Properties

    private let queryContext: IndexQueryContext
    private let indexName: String
    private var permutation: Permutation?
    private var queryType: QueryType = .all
    private var limitCount: Int?

    // MARK: - Initialization

    internal init(
        queryContext: IndexQueryContext,
        indexName: String,
        permutation: Permutation? = nil
    ) {
        self.queryContext = queryContext
        self.indexName = indexName
        self.permutation = permutation
    }

    // MARK: - Query Methods

    /// Query by prefix in permuted field order
    ///
    /// - Parameter values: Prefix values in permuted order
    /// - Returns: Updated query builder
    public func prefix(_ values: [any TupleElement & Sendable]) -> Self {
        var copy = self
        copy.queryType = .prefix(values)
        return copy
    }

    /// Query by exact match in permuted field order
    ///
    /// - Parameter values: Values in permuted order (must match all fields)
    /// - Returns: Updated query builder
    public func exact(_ values: [any TupleElement & Sendable]) -> Self {
        var copy = self
        copy.queryType = .exact(values)
        return copy
    }

    /// Limit the number of results
    ///
    /// - Parameter count: Maximum number of results
    /// - Returns: Updated query builder
    public func limit(_ count: Int) -> Self {
        var copy = self
        copy.limitCount = count
        return copy
    }

    // MARK: - Execution

    /// Execute the query and return matching items
    ///
    /// - Returns: Array of matching items
    public func execute() async throws -> [T] {
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Get permutation from index descriptor if not provided
        let perm: Permutation
        if let p = permutation {
            perm = p
        } else if let descriptor = queryContext.schema.indexDescriptor(named: indexName),
                  let kind = descriptor.kind as? PermutedIndexKind<T> {
            perm = kind.permutation
        } else {
            throw PermutedQueryError.indexNotFound(indexName)
        }

        let primaryKeys: [[any TupleElement]] = try await queryContext.withTransaction { transaction in
            // Generate dummy field names based on permutation size
            let fieldNames = (0..<perm.size).map { "field\($0)" }

            let maintainer = PermutedIndexMaintainer<T>(
                index: Index(
                    name: self.indexName,
                    kind: PermutedIndexKind<T>(fieldNames: fieldNames, permutation: perm),
                    rootExpression: EmptyKeyExpression(),
                    keyPaths: []
                ),
                permutation: perm,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )

            let results: [[any TupleElement]]
            switch self.queryType {
            case .prefix(let values):
                let converted = values.map { $0 as any TupleElement }
                results = try await maintainer.scanByPrefix(
                    prefixValues: converted,
                    transaction: transaction
                )

            case .exact(let values):
                let converted = values.map { $0 as any TupleElement }
                results = try await maintainer.scanByExactMatch(
                    values: converted,
                    transaction: transaction
                )

            case .all:
                let allResults = try await maintainer.scanAll(transaction: transaction)
                results = allResults.map { $0.primaryKey }
            }

            // Apply limit
            if let limit = self.limitCount, results.count > limit {
                return Array(results.prefix(limit))
            }
            return results
        }

        // Convert to Tuples and fetch items
        let tuples = primaryKeys.map { Tuple($0) }
        return try await queryContext.fetchItems(ids: tuples, type: T.self)
    }

    /// Execute the query and return raw results with permuted fields
    ///
    /// - Returns: Array of (permutedFields, item) tuples
    public func executeWithFields() async throws -> [(permutedFields: [any TupleElement], item: T)] {
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Get permutation from index descriptor if not provided
        let perm: Permutation
        if let p = permutation {
            perm = p
        } else if let descriptor = queryContext.schema.indexDescriptor(named: indexName),
                  let kind = descriptor.kind as? PermutedIndexKind<T> {
            perm = kind.permutation
        } else {
            throw PermutedQueryError.indexNotFound(indexName)
        }

        let rawResults: [(permutedFields: [any TupleElement], primaryKey: [any TupleElement])]
        rawResults = try await queryContext.withTransaction { transaction in
            // Generate dummy field names based on permutation size
            let fieldNames = (0..<perm.size).map { "field\($0)" }

            let maintainer = PermutedIndexMaintainer<T>(
                index: Index(
                    name: self.indexName,
                    kind: PermutedIndexKind<T>(fieldNames: fieldNames, permutation: perm),
                    rootExpression: EmptyKeyExpression(),
                    keyPaths: []
                ),
                permutation: perm,
                subspace: indexSubspace,
                idExpression: FieldKeyExpression(fieldName: "id")
            )

            return try await maintainer.scanAll(transaction: transaction)
        }

        // Build mapping: packed primary key → permutedFields
        var fieldsByPackedKey: [Data: [any TupleElement]] = [:]
        for result in rawResults {
            let pkData = Data(Tuple(result.primaryKey).pack())
            fieldsByPackedKey[pkData] = result.permutedFields
        }

        // Fetch items (order may differ from rawResults; missing IDs are skipped)
        let tuples = rawResults.map { Tuple($0.primaryKey) }
        let items = try await queryContext.fetchItems(ids: tuples, type: T.self)

        // Match items with permuted fields by ID
        // Use DataAccess.extractId() to ensure consistency with the storage path
        // (PermutedIndexMaintainer.buildPermutedKey uses the same method).
        let idExpression = FieldKeyExpression(fieldName: "id")
        var finalResults: [(permutedFields: [any TupleElement], item: T)] = []
        for item in items {
            let pkTuple = try DataAccess.extractId(from: item, using: idExpression)
            let pkData = Data(pkTuple.pack())
            if let fields = fieldsByPackedKey[pkData] {
                finalResults.append((permutedFields: fields, item: item))
            }
        }

        return finalResults
    }
}

// MARK: - FDBContext Extension

extension FDBContext {
    /// Start a permuted index query
    ///
    /// This method is available when you import `PermutedIndex`.
    ///
    /// **Usage**:
    /// ```swift
    /// import PermutedIndex
    ///
    /// // Query by permuted field order
    /// let results = try await context.permuted(Location.self)
    ///     .index("Location_permuted_country_city_name")
    ///     .prefix(["Tokyo"])  // Query by city (first in permuted order)
    ///     .execute()
    /// ```
    ///
    /// - Parameter type: The Persistable type to query
    /// - Returns: Entry point for configuring the permuted query
    public func permuted<T: Persistable>(_ type: T.Type) -> PermutedEntryPoint<T> {
        PermutedEntryPoint(queryContext: indexQueryContext)
    }
}

// MARK: - Permuted Query Error

/// Errors for permuted query operations
public enum PermutedQueryError: Error, CustomStringConvertible {
    /// Index not found
    case indexNotFound(String)

    /// Field count mismatch
    case fieldCountMismatch(expected: Int, got: Int)

    public var description: String {
        switch self {
        case .indexNotFound(let name):
            return "Permuted index not found: \(name)"
        case .fieldCountMismatch(let expected, let got):
            return "Field count mismatch: expected \(expected), got \(got)"
        }
    }
}
