// IndexQueryContext+Bitmap.swift
// BitmapIndex - IndexQueryContext extension for Bitmap queries

import Foundation
import Core
import DatabaseEngine
import FoundationDB

extension IndexQueryContext {
    /// Create a Bitmap query for set membership filtering
    ///
    /// **Usage**:
    /// ```swift
    /// let results = try await context.fuse(User.self) {
    ///     context.indexQueryContext.bitmap(User.self, \.status, equals: "active")
    /// }
    /// .execute()
    /// ```
    public func bitmap<T: Persistable, V: Sendable & Hashable & Equatable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        equals value: V
    ) -> Bitmap<T> {
        Bitmap(keyPath, equals: value, context: self)
    }

    /// Create a Bitmap query for set membership (OR)
    public func bitmap<T: Persistable, V: Sendable & Hashable & Equatable>(
        _ type: T.Type,
        _ keyPath: KeyPath<T, V>,
        in values: [V]
    ) -> Bitmap<T> {
        Bitmap(keyPath, in: values, context: self)
    }

    /// Execute bitmap search using the index
    ///
    /// - Parameters:
    ///   - type: The Persistable type
    ///   - indexName: Name of the bitmap index
    ///   - fieldValues: Field values to match
    /// - Returns: Array of matching items
    public func executeBitmapSearch<T: Persistable>(
        type: T.Type,
        indexName: String,
        fieldValues: [any TupleElement]
    ) async throws -> [T] {
        guard let index = schema.index(named: indexName) else {
            throw FusionQueryError.indexNotFound(
                type: T.persistableType,
                field: fieldValues.map { "\($0)" }.joined(separator: ", "),
                kind: "bitmap"
            )
        }

        // Get the maintainer
        guard let maintainer = try await indexMaintainerProvider.maintainer(
            for: index,
            type: T.self
        ) as? BitmapIndexMaintainer<T> else {
            throw FusionQueryError.invalidConfiguration(
                "Could not create BitmapIndexMaintainer for \(indexName)"
            )
        }

        // Execute bitmap query and get primary keys
        return try await database.withTransaction { transaction in
            let bitmap = try await maintainer.getBitmap(
                for: fieldValues,
                transaction: transaction
            )

            let primaryKeys = try await maintainer.getPrimaryKeys(
                from: bitmap,
                transaction: transaction
            )

            // Fetch items by primary keys
            var items: [T] = []
            for pk in primaryKeys {
                if let item: T = try await self.fetchItem(id: pk, transaction: transaction) {
                    items.append(item)
                }
            }

            return items
        }
    }

    /// Fetch a single item by primary key
    private func fetchItem<T: Persistable>(
        id: Tuple,
        transaction: any TransactionProtocol
    ) async throws -> T? {
        let recordKey = try recordSubspace(for: T.self).pack(id)
        guard let data = try await transaction.getValue(for: recordKey) else {
            return nil
        }
        return try serializer.deserialize(data, as: T.self)
    }

    /// Get record subspace for a type
    private func recordSubspace<T: Persistable>(for type: T.Type) throws -> Subspace {
        subspace.subspace(SubspaceKey.items.rawValue).subspace(T.persistableType)
    }
}
