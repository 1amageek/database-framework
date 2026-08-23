// Bitmap.swift
// BitmapIndex - Bitmap filter query for Fusion
//
// This file is part of BitmapIndex module, not DatabaseEngine.
// DatabaseEngine remains independent of bitmap execution behavior.

import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Bitmap filter query for Fusion
///
/// Filters items using bitmap index for efficient set operations.
/// All matching items receive a score of 1.0 (pass/fail filter).
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(User.self) {
///     Bitmap(User.fields.status, equals: "active")
///     Search(\.bio).terms(["developer"])
/// }
/// .execute()
///
/// // OR query
/// let results = try await context.fuse(User.self) {
///     Bitmap(User.fields.status, in: ["active", "pending"])
///     Similar(\.embedding, dimensions: 384).nearest(to: vector, k: 100)
/// }
/// .execute()
/// ```
public struct Bitmap<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var predicate: BitmapPredicate

    private enum BitmapPredicate: Sendable {
        case equals(FieldValue)
        case `in`([FieldValue])
    }

    // MARK: - Initialization (FusionContext - Equals)

    /// Create a Bitmap query for equality comparison
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(User.self) {
    ///     Bitmap(User.fields.status, equals: "active")
    /// }
    /// ```
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Bitmap must be used within context.fuse { } block")
        }
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Bitmap query for optional field equality
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V?>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Bitmap must be used within context.fuse { } block")
        }
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - In)

    /// Create a Bitmap query for set membership (OR)
    ///
    /// Returns items matching ANY of the provided values.
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        in values: [V]
    ) {
        guard let context = FusionContext.current else {
            fatalError("Bitmap must be used within context.fuse { } block")
        }
        self.fieldName = field.name
        self.predicate = .in(values.map { $0.fieldValue })
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Equals)

    /// Create a Bitmap query for equality comparison with explicit context
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    /// Create a Bitmap query for optional field equality with explicit context
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V?>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = field.name
        self.predicate = .equals(value.fieldValue)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - In)

    /// Create a Bitmap query for set membership with explicit context
    public init<V: FieldValueRepresentable & Hashable & Equatable>(
        _ field: Field<T, V>,
        in values: [V],
        context: IndexQueryContext
    ) {
        self.fieldName = field.name
        self.predicate = .in(values.map { $0.fieldValue })
        self.queryContext = context
    }

    // MARK: - Index Discovery

    /// Finds the bitmap index descriptor for the requested field.
    private func findIndexDescriptor() throws -> IndexDescriptor? {
        guard let descriptor = queryContext.indexDescriptors(
            for: T.self
        ).first(where: {
                $0.type == .bitmap
                    && $0.fieldNames.contains(fieldName)
        }) else {
            return nil
        }
        return descriptor
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<T.ID>?) async throws -> [ScoredResult<T>] {
        guard let descriptor = try findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                entity: T.persistableType,
                field: fieldName,
                indexType: .bitmap
            )
        }

        let indexName = descriptor.name

        // Execute bitmap query within transaction
        let primaryKeys: [Tuple] = try await queryContext.withReadableIndex(
            named: indexName,
            indexType: .bitmap,
            for: T.self
        ) { readableIndex, transaction in
            guard let readableIndex else {
                return []
            }
            switch self.predicate {
            case .equals(let value):
                let fieldValues = [try TupleEncoder.encode(value)]
                return try await self.readBitmapPrimaryKeys(
                    fieldValues: fieldValues,
                    indexSubspace: readableIndex.subspace,
                    transaction: transaction
                )

            case .in(let values):
                // OR query across multiple values
                var allPks: [Tuple] = []
                var seen: Set<ByteString> = []

                for value in values {
                    let fieldValues = [try TupleEncoder.encode(value)]
                    let pks = try await self.readBitmapPrimaryKeys(
                        fieldValues: fieldValues,
                        indexSubspace: readableIndex.subspace,
                        transaction: transaction
                    )
                    for pk in pks {
                        let packedPrimaryKey = pk.pack()
                        if !seen.contains(packedPrimaryKey) {
                            seen.insert(packedPrimaryKey)
                            allPks.append(pk)
                        }
                    }
                }
                return allPks
            }
        }

        // Fetch items by primary keys
        var results = try await queryContext.fetchItems(ids: primaryKeys, type: T.self)

        // Filter to candidates if provided
        if let candidateIDs = candidates {
            results = results.filter { candidateIDs.contains($0.id) }
        }

        // All matching items get score 1.0 (pass/fail filter)
        return results.map { ScoredResult(item: $0, score: 1.0) }
    }

    // MARK: - Bitmap Index Reading

    /// Read primary keys from bitmap index
    ///
    /// Index structure:
    /// - `[indexSubspace]["data"][fieldValue]` -> RoaringBitmap of sequential IDs
    /// - `[indexSubspace]["ids"][seqId]` -> primary key bytes
    private func readBitmapPrimaryKeys(
        fieldValues: [any TupleElement],
        indexSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [Tuple] {
        let dataSubspace = indexSubspace.subspace("data")
        let idsSubspace = indexSubspace.subspace("ids")

        // Get bitmap for field values
        let bitmapKey = dataSubspace.pack(Tuple(fieldValues))
        guard let bitmapBytes = try await transaction.getValue(for: bitmapKey) else {
            return []
        }

        let bitmap = try RoaringBitmap(serializedBytes: bitmapBytes)

        // Convert sequential IDs to primary keys
        var primaryKeys: [Tuple] = []
        primaryKeys.reserveCapacity(bitmap.cardinality)
        for seqId in bitmap {
            let idKey = idsSubspace.pack(Tuple(Int(seqId)))
            if let pkBytes = try await transaction.getValue(for: idKey) {
                let pkElements = try Tuple.unpack(from: pkBytes)
                primaryKeys.append(Tuple(pkElements))
            }
        }

        return primaryKeys
    }

}
