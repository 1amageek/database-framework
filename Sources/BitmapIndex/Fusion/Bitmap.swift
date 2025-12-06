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
///     Similar(\.embedding, dimensions: 384).query(vector, k: 100)
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
            guard descriptor.kindIdentifier == BitmapIndexKind<T>.identifier else {
                return false
            }
            guard let kind = descriptor.kind as? BitmapIndexKind<T> else {
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

        // Execute bitmap query
        var results: [T]

        switch predicate {
        case .equals(let value):
            results = try await queryContext.executeBitmapSearch(
                type: T.self,
                indexName: indexName,
                fieldValues: [convertToTupleElement(value)]
            )

        case .in(let values):
            // OR query across multiple values
            var allResults: [T] = []
            var seen: Set<String> = []

            for value in values {
                let matches = try await queryContext.executeBitmapSearch(
                    type: T.self,
                    indexName: indexName,
                    fieldValues: [convertToTupleElement(value)]
                )
                for item in matches {
                    let id = "\(item.id)"
                    if !seen.contains(id) {
                        seen.insert(id)
                        allResults.append(item)
                    }
                }
            }
            results = allResults
        }

        // Filter to candidates if provided
        if let candidateIds = candidates {
            results = results.filter { candidateIds.contains("\($0.id)") }
        }

        // All matching items get score 1.0 (pass/fail filter)
        return results.map { ScoredResult(item: $0, score: 1.0) }
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
