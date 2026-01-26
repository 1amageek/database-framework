import Foundation
import FoundationDB
import QueryAST
import DatabaseEngine

/// Storage layer for CLI schemas in FoundationDB
///
/// Key layout:
/// ```
/// /_cli/schemas/<name>                                         = JSON-encoded DynamicSchema
/// /_cli/data/<schema>/<id>                                     = JSON-encoded record values
/// /_cli/indexes/<schema>/scalar/<indexName>/<value>/<id>       = ''
/// /_cli/indexes/<schema>/vector/<indexName>/vectors/<id>       = [Float...]
/// /_cli/indexes/<schema>/vector/<indexName>/hnsw/...           = HNSW graph
/// /_cli/indexes/<schema>/fulltext/<indexName>/terms/<term>/<docId> = positions
/// /_cli/indexes/<schema>/fulltext/<indexName>/docfreq/<term>   = count
/// /_cli/indexes/<schema>/spatial/<indexName>/<cellId>/<id>     = ''
/// /_cli/indexes/<schema>/rank/<indexName>/<score>/<id>         = ''
/// /_cli/indexes/<schema>/rank/<indexName>/_count               = atomic counter
/// /_cli/indexes/<schema>/permuted/<indexName>/<values...>/<id> = ''
/// /_cli/indexes/<schema>/graph/<indexName>/out/<from>/<to>     = ''
/// /_cli/indexes/<schema>/graph/<indexName>/in/<to>/<from>      = ''
/// /_cli/indexes/<schema>/aggregation/<indexName>/<groupValues...> = aggregated value
/// /_cli/indexes/<schema>/version/<indexName>/<id>/<versionstamp> = snapshot
/// /_cli/indexes/<schema>/bitmap/<indexName>/<fieldValue>       = RoaringBitmap
/// /_cli/indexes/<schema>/leaderboard/<indexName>/<window>/<score>/<id> = ''
/// /_cli/indexes/<schema>/relationship/<indexName>/<targetId>/<sourceId> = ''
/// ```
public final class SchemaStorage: Sendable {
    nonisolated(unsafe) private let database: any DatabaseProtocol
    private let rootSubspace: Subspace

    public init(database: any DatabaseProtocol) {
        self.database = database
        // Use a dedicated subspace for CLI data
        self.rootSubspace = Subspace(prefix: Tuple(["_cli"]).pack())
    }

    // MARK: - Subspaces

    private var schemasSubspace: Subspace {
        rootSubspace.subspace(Tuple(["schemas"]))
    }

    private var dataSubspace: Subspace {
        rootSubspace.subspace(Tuple(["data"]))
    }

    private var indexesSubspace: Subspace {
        rootSubspace.subspace(Tuple(["indexes"]))
    }

    // MARK: - Index Subspaces

    /// Get the index subspace for a specific schema and index kind
    public func indexSubspace(schema: String, kind: IndexKind) -> Subspace {
        indexesSubspace.subspace(Tuple([schema, kind.rawValue]))
    }

    /// Get the index subspace for a specific named index
    public func indexSubspace(schema: String, kind: IndexKind, indexName: String) -> Subspace {
        indexSubspace(schema: schema, kind: kind).subspace(Tuple([indexName]))
    }

    // MARK: - Schema Operations

    /// Save a schema definition
    public func saveSchema(_ schema: DynamicSchema) async throws {
        let key = schemasSubspace.pack(Tuple([schema.name]))
        let data = try JSONEncoder().encode(schema)

        try await database.withTransaction { transaction in
            transaction.setValue(Array(data), for: key)
        }
    }

    /// Get a schema by name
    public func getSchema(name: String) async throws -> DynamicSchema? {
        let key = schemasSubspace.pack(Tuple([name]))

        return try await database.withTransaction { transaction in
            guard let data = try await transaction.getValue(for: key, snapshot: false) else {
                return nil
            }
            return try JSONDecoder().decode(DynamicSchema.self, from: Data(data))
        }
    }

    /// List all schema names
    public func listSchemas() async throws -> [DynamicSchema] {
        let (begin, end) = schemasSubspace.range()

        return try await database.withTransaction { transaction in
            var schemas: [DynamicSchema] = []

            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (_, value) in sequence {
                let schema = try JSONDecoder().decode(DynamicSchema.self, from: Data(value))
                schemas.append(schema)
            }

            return schemas
        }
    }

    /// Delete a schema and all its data and indexes
    public func dropSchema(name: String) async throws {
        let schemaKey = schemasSubspace.pack(Tuple([name]))
        let dataSubspace = self.dataSubspace.subspace(Tuple([name]))
        let (dataBegin, dataEnd) = dataSubspace.range()

        // Clear all index data for this schema
        let indexSchemaSubspace = indexesSubspace.subspace(Tuple([name]))
        let (indexBegin, indexEnd) = indexSchemaSubspace.range()

        try await database.withTransaction { transaction in
            // Delete schema definition
            transaction.clear(key: schemaKey)

            // Delete all data for this schema
            transaction.clearRange(beginKey: dataBegin, endKey: dataEnd)

            // Delete all indexes for this schema
            transaction.clearRange(beginKey: indexBegin, endKey: indexEnd)
        }
    }

    // MARK: - Data Operations

    /// Get the subspace for a schema's data
    public func schemaDataSubspace(for schemaName: String) -> Subspace {
        dataSubspace.subspace(Tuple([schemaName]))
    }

    /// Insert a record with index maintenance
    public func insert(
        schemaName: String,
        id: String,
        values: [String: Any],
        schema: DynamicSchema,
        indexHandlers: [any IndexHandler]
    ) async throws {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let key = dataSubspace.pack(Tuple([id]))

        // Convert to JSON-serializable format
        let jsonData = try JSONSerialization.data(withJSONObject: values, options: [])

        try await database.withTransaction { transaction in
            transaction.setValue(Array(jsonData), for: key)

            // Update indexes
            for handler in indexHandlers {
                try await handler.updateIndex(
                    oldItem: nil,
                    newItem: values,
                    id: id,
                    transaction: transaction,
                    storage: self
                )
            }
        }
    }

    /// Insert a record (simple version without index maintenance)
    public func insert(schemaName: String, id: String, values: [String: Any]) async throws {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let key = dataSubspace.pack(Tuple([id]))

        // Convert to JSON-serializable format
        let jsonData = try JSONSerialization.data(withJSONObject: values, options: [])

        try await database.withTransaction { transaction in
            transaction.setValue(Array(jsonData), for: key)
        }
    }

    /// Get a record by ID
    public func get(schemaName: String, id: String) async throws -> [String: Any]? {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let key = dataSubspace.pack(Tuple([id]))

        let bytes = try await database.withTransaction { transaction in
            try await transaction.getValue(for: key, snapshot: false)
        }

        guard let bytes = bytes else {
            return nil
        }

        guard let dict = try JSONSerialization.jsonObject(with: Data(bytes), options: []) as? [String: Any] else {
            return nil
        }

        return dict
    }

    /// Delete a record by ID with index maintenance
    public func delete(
        schemaName: String,
        id: String,
        oldValues: [String: Any],
        schema: DynamicSchema,
        indexHandlers: [any IndexHandler]
    ) async throws {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let key = dataSubspace.pack(Tuple([id]))

        try await database.withTransaction { transaction in
            transaction.clear(key: key)

            // Update indexes
            for handler in indexHandlers {
                try await handler.updateIndex(
                    oldItem: oldValues,
                    newItem: nil,
                    id: id,
                    transaction: transaction,
                    storage: self
                )
            }
        }
    }

    /// Delete a record by ID (simple version without index maintenance)
    public func delete(schemaName: String, id: String) async throws {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let key = dataSubspace.pack(Tuple([id]))

        try await database.withTransaction { transaction in
            transaction.clear(key: key)
        }
    }

    /// Update a record with index maintenance
    public func update(
        schemaName: String,
        id: String,
        oldValues: [String: Any],
        newValues: [String: Any],
        schema: DynamicSchema,
        indexHandlers: [any IndexHandler]
    ) async throws {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let key = dataSubspace.pack(Tuple([id]))
        let jsonData = try JSONSerialization.data(withJSONObject: newValues, options: [])

        try await database.withTransaction { transaction in
            transaction.setValue(Array(jsonData), for: key)

            // Update indexes
            for handler in indexHandlers {
                try await handler.updateIndex(
                    oldItem: oldValues,
                    newItem: newValues,
                    id: id,
                    transaction: transaction,
                    storage: self
                )
            }
        }
    }

    /// Update a record (simple version without index maintenance)
    public func update(schemaName: String, id: String, values: [String: Any]) async throws {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let key = dataSubspace.pack(Tuple([id]))
        let jsonData = try JSONSerialization.data(withJSONObject: values, options: [])

        // Check existence and update in the same transaction to avoid TOCTOU
        try await database.withTransaction { transaction in
            guard let _ = try await transaction.getValue(for: key, snapshot: false) else {
                throw DataStorageError.recordNotFound(schema: schemaName, id: id)
            }
            transaction.setValue(Array(jsonData), for: key)
        }
    }

    /// Query records with optional filter
    ///
    /// - Parameters:
    ///   - schemaName: The schema to query
    ///   - filter: Optional filter function (applied after fetching)
    ///   - limit: Maximum records to return (use Int.max for no limit)
    /// - Returns: Array of (id, values) tuples
    public func query(
        schemaName: String,
        filter: (@Sendable (String, [String: Any]) -> Bool)? = nil,
        limit: Int = 100
    ) async throws -> [(id: String, values: [String: Any])] {
        let dataSubspace = schemaDataSubspace(for: schemaName)
        let (begin, end) = dataSubspace.range()

        // Calculate fetch limit safely to avoid overflow
        // When limit is very large (e.g., Int.max for full scan), don't multiply
        let fetchLimit: Int
        if limit > Int.max / 2 {
            fetchLimit = Int.max  // Full scan requested
        } else {
            fetchLimit = limit * 2  // Fetch more to account for filtering
        }

        // First, fetch raw key-value pairs from FDB
        let rawResults: [(key: FDB.Bytes, value: FDB.Bytes)] = try await database.withTransaction { transaction in
            var results: [(key: FDB.Bytes, value: FDB.Bytes)] = []

            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, value) in sequence {
                results.append((key: key, value: value))
                if fetchLimit != Int.max && results.count >= fetchLimit { break }
            }

            return results
        }

        // Then parse and filter outside the transaction
        var results: [(id: String, values: [String: Any])] = []

        for (key, value) in rawResults {
            if limit != Int.max && results.count >= limit { break }

            // Extract ID from key
            guard let tuple = try? dataSubspace.unpack(key),
                  let id = tuple[0] as? String else {
                continue
            }

            guard let dict = try JSONSerialization.jsonObject(
                with: Data(value),
                options: []
            ) as? [String: Any] else {
                continue
            }

            // Apply filter if provided
            if let filter = filter {
                if filter(id, dict) {
                    results.append((id: id, values: dict))
                }
            } else {
                results.append((id: id, values: dict))
            }
        }

        return results
    }

    // MARK: - Index-backed Query

    /// Query using a scalar index
    public func queryByScalarIndex(
        schemaName: String,
        indexName: String,
        operation: ScalarOperation,
        limit: Int = 100
    ) async throws -> [(id: String, values: [String: Any])] {
        let indexSubspace = self.indexSubspace(schema: schemaName, kind: .scalar, indexName: indexName)

        // Compute the range outside the transaction closure to avoid Sendable issues
        let (begin, end): (FDB.Bytes, FDB.Bytes)
        switch operation {
        case .equals(let value):
            let valueStr = Self.toTupleElement(from: value)
            let valueSubspace = indexSubspace.subspace(Tuple([valueStr]))
            (begin, end) = valueSubspace.range()
        case .range(let lower, let upper):
            let beginKey = lower.map { indexSubspace.pack(Tuple([Self.toTupleElement(from: $0)])) } ?? indexSubspace.range().0
            let endKey = upper.map { indexSubspace.pack(Tuple([Self.toTupleElement(from: $0)])) } ?? indexSubspace.range().1
            (begin, end) = (beginKey, endKey)
        case .greaterThan(let value):
            let beginKey = indexSubspace.pack(Tuple([Self.toTupleElement(from: value)]))
            (begin, end) = (beginKey, indexSubspace.range().1)
        case .lessThan(let value):
            let endKey = indexSubspace.pack(Tuple([Self.toTupleElement(from: value)]))
            (begin, end) = (indexSubspace.range().0, endKey)
        case .fullRange:
            (begin, end) = indexSubspace.range()
        }

        // Compute dataSubspace outside the closure
        let dataSubspace = self.schemaDataSubspace(for: schemaName)

        // First fetch IDs and raw data within the transaction
        let rawData: [(id: String, bytes: FDB.Bytes)] = try await database.withTransaction { transaction in
            var ids: [String] = []

            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard ids.count < limit else { break }

                // Extract ID from key: indexSubspace/<value>/<id>
                if let tuple = try? indexSubspace.unpack(key),
                   tuple.count >= 2,
                   let id = tuple[1] as? String {
                    ids.append(id)
                }
            }

            // Fetch raw data
            var data: [(id: String, bytes: FDB.Bytes)] = []
            for id in ids {
                let dataKey = dataSubspace.pack(Tuple([id]))
                if let bytes = try await transaction.getValue(for: dataKey, snapshot: true) {
                    data.append((id: id, bytes: bytes))
                }
            }

            return data
        }

        // Parse JSON outside the transaction
        var results: [(id: String, values: [String: Any])] = []
        for (id, bytes) in rawData {
            if let dict = try JSONSerialization.jsonObject(with: Data(bytes), options: []) as? [String: Any] {
                results.append((id: id, values: dict))
            }
        }

        return results
    }

    // MARK: - Ordered Query

    /// Query using a scalar index with ordering support
    ///
    /// When the sort field matches the index field, the index's natural
    /// ordering is leveraged for efficient sorted results via reverse scan.
    ///
    /// - Parameters:
    ///   - schemaName: The schema to query
    ///   - indexName: The scalar index to use
    ///   - operation: The filter operation
    ///   - sortDescriptor: Optional sort descriptor
    ///   - limit: Maximum records to return
    /// - Returns: Array of (id, values) tuples in sorted order
    public func queryByScalarIndexOrdered(
        schemaName: String,
        indexName: String,
        operation: ScalarOperation,
        sortDescriptor: DynamicSortDescriptor?,
        limit: Int = 100
    ) async throws -> [(id: String, values: [String: Any])] {
        let indexSubspace = self.indexSubspace(schema: schemaName, kind: .scalar, indexName: indexName)

        // Determine if we should use reverse scan
        let useReverse = sortDescriptor?.direction == .descending

        // Compute range keys based on operation
        let (begin, end): (FDB.Bytes, FDB.Bytes)
        switch operation {
        case .equals(let value):
            let valueStr = Self.toTupleElement(from: value)
            let valueSubspace = indexSubspace.subspace(Tuple([valueStr]))
            (begin, end) = valueSubspace.range()
        case .range(let lower, let upper):
            let beginKey = lower.map { indexSubspace.pack(Tuple([Self.toTupleElement(from: $0)])) } ?? indexSubspace.range().0
            let endKey = upper.map { indexSubspace.pack(Tuple([Self.toTupleElement(from: $0)])) } ?? indexSubspace.range().1
            (begin, end) = (beginKey, endKey)
        case .greaterThan(let value):
            let beginKey = indexSubspace.pack(Tuple([Self.toTupleElement(from: value)]))
            (begin, end) = (beginKey, indexSubspace.range().1)
        case .lessThan(let value):
            let endKey = indexSubspace.pack(Tuple([Self.toTupleElement(from: value)]))
            (begin, end) = (indexSubspace.range().0, endKey)
        case .fullRange:
            (begin, end) = indexSubspace.range()
        }

        let dataSubspace = self.schemaDataSubspace(for: schemaName)

        // Fetch with appropriate scan direction
        let rawData: [(id: String, bytes: FDB.Bytes)] = try await database.withTransaction { transaction in
            var ids: [String] = []

            // Use KeySelector-based getRange with reverse support
            let beginSelector = FDB.KeySelector.firstGreaterOrEqual(begin)
            let endSelector = FDB.KeySelector.firstGreaterOrEqual(end)

            let sequence = transaction.getRange(
                from: beginSelector,
                to: endSelector,
                limit: limit,
                reverse: useReverse,
                snapshot: true
            )

            for try await (key, _) in sequence {
                guard ids.count < limit else { break }

                if let tuple = try? indexSubspace.unpack(key),
                   tuple.count >= 2,
                   let id = tuple[1] as? String {
                    ids.append(id)
                }
            }

            // Fetch data for IDs
            var data: [(id: String, bytes: FDB.Bytes)] = []
            for id in ids {
                let dataKey = dataSubspace.pack(Tuple([id]))
                if let bytes = try await transaction.getValue(for: dataKey, snapshot: true) {
                    data.append((id: id, bytes: bytes))
                }
            }

            return data
        }

        // Parse JSON outside transaction
        var results: [(id: String, values: [String: Any])] = []
        for (id, bytes) in rawData {
            if let dict = try JSONSerialization.jsonObject(with: Data(bytes), options: []) as? [String: Any] {
                results.append((id: id, values: dict))
            }
        }

        return results
    }

    /// Query with full scan and in-memory sorting
    ///
    /// Used when no suitable index exists for the sort field.
    ///
    /// **Important**: When sorting without an index, this method performs a full
    /// scan of the schema's data to ensure correct ordering. For large datasets,
    /// consider adding a scalar index on the sort field for better performance.
    ///
    /// - Parameters:
    ///   - schemaName: The schema to query
    ///   - filter: Optional filter function
    ///   - sortDescriptor: Optional sort descriptor
    ///   - limit: Maximum records to return
    /// - Returns: Array of (id, values) tuples in sorted order
    public func queryWithSort(
        schemaName: String,
        filter: (@Sendable (String, [String: Any]) -> Bool)? = nil,
        sortDescriptor: DynamicSortDescriptor?,
        limit: Int = 100
    ) async throws -> [(id: String, values: [String: Any])] {
        // When sorting is requested, we must scan all records to guarantee
        // correct ordering. Without this, we might miss records that should
        // appear in the final sorted result.
        let needsFullScan = sortDescriptor != nil
        let fetchLimit = needsFullScan ? Int.max : (filter != nil ? limit * 3 : limit * 2)

        var results = try await query(schemaName: schemaName, filter: filter, limit: fetchLimit)

        // Apply in-memory sort if requested
        if let sort = sortDescriptor {
            results = sortResults(results, by: sort)
        }

        // Apply limit after sorting
        if results.count > limit {
            results = Array(results.prefix(limit))
        }

        return results
    }

    /// Sort results in memory by a field
    public func sortResults(
        _ results: [(id: String, values: [String: Any])],
        by descriptor: DynamicSortDescriptor
    ) -> [(id: String, values: [String: Any])] {
        return results.sorted { a, b in
            let aValue = a.values[descriptor.field]
            let bValue = b.values[descriptor.field]

            let comparison = compareValues(aValue, bValue, nulls: descriptor.nulls)

            return descriptor.direction == .ascending ? comparison < 0 : comparison > 0
        }
    }

    /// Compare two Any values for sorting
    ///
    /// Handles NULL ordering according to SQL standard (NULLS FIRST/LAST).
    /// Default behavior: NULLs sort first in ascending, last in descending.
    public func compareValues(_ a: Any?, _ b: Any?, nulls: NullOrdering?) -> Int {
        // NULL handling with configurable ordering
        // Default: NULLS FIRST for ascending, NULLS LAST for descending (matches PostgreSQL)
        let nullsFirst = (nulls == .first)
        if a == nil && b == nil { return 0 }
        if a == nil { return nullsFirst ? -1 : 1 }
        if b == nil { return nullsFirst ? 1 : -1 }

        // Type-specific comparisons
        switch (a, b) {
        case (let aStr as String, let bStr as String):
            return aStr.compare(bStr).rawValue
        case (let aInt as Int, let bInt as Int):
            return aInt < bInt ? -1 : (aInt > bInt ? 1 : 0)
        case (let aInt64 as Int64, let bInt64 as Int64):
            return aInt64 < bInt64 ? -1 : (aInt64 > bInt64 ? 1 : 0)
        case (let aDouble as Double, let bDouble as Double):
            return aDouble < bDouble ? -1 : (aDouble > bDouble ? 1 : 0)
        case (let aBool as Bool, let bBool as Bool):
            return (!aBool && bBool) ? -1 : ((aBool && !bBool) ? 1 : 0)
        default:
            // Fall back to string comparison
            return "\(a!)".compare("\(b!)").rawValue
        }
    }

    /// Convert Any to TupleElement for index storage
    ///
    /// Uses TupleEncoder for consistent type conversion.
    /// Falls back to string representation for unsupported types.
    private static func toTupleElement(from value: Any) -> any TupleElement {
        do {
            return try TupleEncoder.encode(value)
        } catch {
            // Fallback for unsupported types in dynamic schema context
            return "\(value)"
        }
    }

    // MARK: - Index Maintenance Helpers

    /// Check if a value already exists in a unique index
    public func checkUniqueConstraint(
        schemaName: String,
        indexName: String,
        value: Any,
        excludeId: String?
    ) async throws -> Bool {
        let indexSubspace = self.indexSubspace(schema: schemaName, kind: .scalar, indexName: indexName)
        let valueStr = Self.toTupleElement(from: value)
        let valueSubspace = indexSubspace.subspace(Tuple([valueStr]))
        let (begin, end) = valueSubspace.range()

        return try await database.withTransaction { transaction in
            var count = 0
            let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)
            for try await (key, _) in sequence {
                guard count < 2 else { break }  // Only need to check first 2 entries
                count += 1

                if let tuple = try? valueSubspace.unpack(key),
                   let existingId = tuple[0] as? String {
                    if existingId != excludeId {
                        return true // Duplicate found
                    }
                }
            }
            return false
        }
    }

    /// Get the database reference
    public var databaseRef: any DatabaseProtocol {
        database
    }
}

// MARK: - Scalar Operations

public enum ScalarOperation {
    case equals(Any)
    case range(lower: Any?, upper: Any?)
    case greaterThan(Any)
    case lessThan(Any)
    case fullRange  // Scan entire index (for ORDER BY without WHERE)
}

// MARK: - Index Handler Protocol

/// Protocol for index handlers that maintain index data
public protocol IndexHandler: Sendable {
    /// The index definition this handler manages
    var indexDefinition: IndexDefinition { get }

    /// Update index when an item is inserted, updated, or deleted
    func updateIndex(
        oldItem: [String: Any]?,
        newItem: [String: Any]?,
        id: String,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws

    /// Scan the index for matching items
    func scan(
        query: Any,
        limit: Int,
        transaction: any TransactionProtocol,
        storage: SchemaStorage
    ) async throws -> [String]
}

// MARK: - Errors

public enum DataStorageError: Error, CustomStringConvertible {
    case recordNotFound(schema: String, id: String)
    case schemaNotFound(String)
    case invalidData(String)
    case uniqueConstraintViolation(field: String, value: String)
    case indexNotFound(String)
    case relationshipConstraintViolation(String)

    public var description: String {
        switch self {
        case .recordNotFound(let schema, let id):
            return "Record '\(id)' not found in schema '\(schema)'"
        case .schemaNotFound(let name):
            return "Schema '\(name)' not found"
        case .invalidData(let message):
            return "Invalid data: \(message)"
        case .uniqueConstraintViolation(let field, let value):
            return "Unique constraint violation: field '\(field)' with value '\(value)' already exists"
        case .indexNotFound(let name):
            return "Index '\(name)' not found"
        case .relationshipConstraintViolation(let message):
            return "Relationship constraint violation: \(message)"
        }
    }
}
