// Bitmap.swift
// BitmapIndex - Bitmap filter query for Fusion
//
// This file is part of BitmapIndex module, not DatabaseEngine.
// DatabaseEngine does not know about BitmapIndexKind.

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Bitmap filter query for Fusion
///
/// Filters items using bitmap index for efficient set operations.
/// All matching items receive a score of 1.0 (pass/fail filter).
///
/// **Usage**:
/// ```swift
/// let results = try await context.fuse(User.self) {
///     Bitmap(\.status, equals: "active")
///     Search(\.bio).terms(["developer"])
/// }
/// .execute()
///
/// // OR query
/// let results = try await context.fuse(User.self) {
///     Bitmap(\.status, in: ["active", "pending"])
///     Similar(\.embedding, dimensions: 384).nearest(to: vector, k: 100)
/// }
/// .execute()
/// ```
public struct Bitmap<T: Persistable>: FusionQuery, Sendable {
    public typealias Item = T

    private let queryContext: IndexQueryContext
    private let fieldName: String
    private var predicate: BitmapPredicate

    private enum BitmapPredicate: @unchecked Sendable {
        case equals(any Sendable & Hashable)
        case `in`([any Sendable & Hashable])
    }

    // MARK: - Initialization (FusionContext - Equals)

    /// Create a Bitmap query for equality comparison
    ///
    /// Uses FusionContext.current for context (automatically set by `context.fuse { }`).
    ///
    /// **Usage**:
    /// ```swift
    /// context.fuse(User.self) {
    ///     Bitmap(\.status, equals: "active")
    /// }
    /// ```
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Bitmap must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    /// Create a Bitmap query for optional field equality
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V?>,
        equals value: V
    ) {
        guard let context = FusionContext.current else {
            fatalError("Bitmap must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    // MARK: - Initialization (FusionContext - In)

    /// Create a Bitmap query for set membership (OR)
    ///
    /// Returns items matching ANY of the provided values.
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        in values: [V]
    ) {
        guard let context = FusionContext.current else {
            fatalError("Bitmap must be used within context.fuse { } block")
        }
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .in(values)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - Equals)

    /// Create a Bitmap query for equality comparison with explicit context
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    /// Create a Bitmap query for optional field equality with explicit context
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V?>,
        equals value: V,
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .equals(value)
        self.queryContext = context
    }

    // MARK: - Initialization (Explicit Context - In)

    /// Create a Bitmap query for set membership with explicit context
    public init<V: Sendable & Hashable & Equatable>(
        _ keyPath: KeyPath<T, V>,
        in values: [V],
        context: IndexQueryContext
    ) {
        self.fieldName = T.fieldName(for: keyPath)
        self.predicate = .in(values)
        self.queryContext = context
    }

    // MARK: - Index Discovery

    /// Find the index descriptor using kindIdentifier and fieldName
    private func findIndexDescriptor() -> IndexDescriptor? {
        T.indexDescriptors.first { descriptor in
            guard descriptor.kindIdentifier == Core.BitmapIndexKind<T>.identifier else {
                return false
            }
            guard let kind = descriptor.kind as? Core.BitmapIndexKind<T> else {
                return false
            }
            return kind.fieldNames.contains(fieldName)
        }
    }

    // MARK: - FusionQuery

    public func execute(candidates: Set<String>?) async throws -> [ScoredResult<T>] {
        guard let descriptor = findIndexDescriptor() else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldName,
                kind: "bitmap"
            )
        }

        let indexName = descriptor.name

        // Get index subspace using public API
        let typeSubspace = try await queryContext.indexSubspace(for: T.self)
        let indexSubspace = typeSubspace.subspace(indexName)

        // Execute bitmap query within transaction
        let primaryKeys: [Tuple] = try await queryContext.withTransaction { transaction in
            switch self.predicate {
            case .equals(let value):
                let fieldValues = [self.convertToTupleElement(value)]
                return try await self.readBitmapPrimaryKeys(
                    fieldValues: fieldValues,
                    indexSubspace: indexSubspace,
                    transaction: transaction
                )

            case .in(let values):
                // OR query across multiple values
                var allPks: [Tuple] = []
                var seen: Set<Data> = []

                for value in values {
                    let fieldValues = [self.convertToTupleElement(value)]
                    let pks = try await self.readBitmapPrimaryKeys(
                        fieldValues: fieldValues,
                        indexSubspace: indexSubspace,
                        transaction: transaction
                    )
                    for pk in pks {
                        let pkData = Data(pk.pack())
                        if !seen.contains(pkData) {
                            seen.insert(pkData)
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
        if let candidateIds = candidates {
            results = results.filter { candidateIds.contains("\($0.id)") }
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
        transaction: any TransactionProtocol
    ) async throws -> [Tuple] {
        let dataSubspace = indexSubspace.subspace("data")
        let idsSubspace = indexSubspace.subspace("ids")

        // Get bitmap for field values
        let bitmapKey = dataSubspace.pack(Tuple(fieldValues))
        guard let bitmapBytes = try await transaction.getValue(for: bitmapKey) else {
            return []
        }

        // Deserialize bitmap
        let bitmap = try RoaringBitmap.deserialize(Data(bitmapBytes))

        // Convert sequential IDs to primary keys
        var primaryKeys: [Tuple] = []
        for seqId in bitmap.toArray() {
            let idKey = idsSubspace.pack(Tuple(Int(seqId)))
            if let pkBytes = try await transaction.getValue(for: idKey) {
                let pkElements = try Tuple.unpack(from: pkBytes)
                primaryKeys.append(Tuple(pkElements))
            }
        }

        return primaryKeys
    }

    /// Convert value to TupleElement for bitmap lookup
    private func convertToTupleElement(_ value: any Sendable & Hashable) -> any TupleElement {
        switch value {
        case let v as String: return v
        case let v as Int: return v
        case let v as Int64: return v
        case let v as Int32: return Int(v)
        case let v as Int16: return Int(v)
        case let v as Int8: return Int(v)
        case let v as UInt: return Int(v)
        case let v as UInt64: return Int64(v)
        case let v as UInt32: return Int(v)
        case let v as Double: return v
        case let v as Float: return Double(v)
        case let v as Bool: return v
        default:
            return String(describing: value)
        }
    }
}
