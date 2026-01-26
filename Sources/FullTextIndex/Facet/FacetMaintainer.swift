// FacetMaintainer.swift
// FullTextIndex - Faceted search support
//
// Reference: Faceted search patterns from Elasticsearch and Solr

import Foundation
import Core
import DatabaseEngine
import FoundationDB

/// Maintainer for faceted search indexes
///
/// **Purpose**: Maintains term counts for facet fields alongside full-text search.
/// Facets allow users to filter search results by category, brand, price range, etc.
///
/// **Storage Layout**:
/// ```
/// [subspace]/facets/[fieldName]/[value] = Int64 (document count)
/// [subspace]/doc_facets/[primaryKey]/[fieldName] = Tuple([values...])
/// ```
///
/// **Usage**:
/// ```swift
/// let facetMaintainer = FacetMaintainer<Product>(
///     subspace: indexSubspace,
///     idExpression: FieldKeyExpression(fieldName: "id"),
///     facetFields: ["category", "brand", "color"]
/// )
///
/// // Update facets when document changes
/// try await facetMaintainer.updateFacets(
///     oldItem: oldProduct,
///     newItem: newProduct,
///     transaction: transaction
/// )
///
/// // Get facet counts for search results
/// let facets = try await facetMaintainer.getFacetCounts(
///     fields: ["category", "brand"],
///     limit: 10,
///     transaction: transaction
/// )
/// ```
public struct FacetMaintainer<Item: Persistable>: Sendable {
    private let subspace: Subspace
    private let idExpression: KeyExpression
    private let facetFields: [String]

    // Subspaces
    private let facetsSubspace: Subspace
    private let docFacetsSubspace: Subspace

    /// Create a facet maintainer
    ///
    /// - Parameters:
    ///   - subspace: FDB subspace for facet data
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - facetFields: Field names to maintain facets for
    public init(
        subspace: Subspace,
        idExpression: KeyExpression,
        facetFields: [String]
    ) {
        self.subspace = subspace
        self.idExpression = idExpression
        self.facetFields = facetFields
        self.facetsSubspace = subspace.subspace("facets")
        self.docFacetsSubspace = subspace.subspace("doc_facets")
    }

    /// Update facet counts when a document changes
    ///
    /// - Parameters:
    ///   - oldItem: Previous item state (nil for new items)
    ///   - newItem: New item state (nil for deletions)
    ///   - transaction: FDB transaction
    public func updateFacets(
        oldItem: Item?,
        newItem: Item?,
        transaction: any TransactionProtocol
    ) async throws {
        // Remove old facet values
        if let oldItem = oldItem {
            let oldId = try DataAccess.extractId(from: oldItem, using: idExpression)
            try await removeFacets(for: oldId, item: oldItem, transaction: transaction)
        }

        // Add new facet values
        if let newItem = newItem {
            let newId = try DataAccess.extractId(from: newItem, using: idExpression)
            try await addFacets(for: newId, item: newItem, transaction: transaction)
        }
    }

    /// Get facet counts for specified fields
    ///
    /// - Parameters:
    ///   - fields: Fields to get facets for (nil = all configured fields)
    ///   - limit: Maximum number of values per field (default: 10)
    ///   - transaction: FDB transaction
    /// - Returns: Dictionary of field -> [(value, count)] sorted by count descending
    public func getFacetCounts(
        fields: [String]? = nil,
        limit: Int = 10,
        transaction: any TransactionProtocol
    ) async throws -> [String: [(value: String, count: Int64)]] {
        let fieldsToFetch = fields ?? facetFields
        var result: [String: [(value: String, count: Int64)]] = [:]

        for field in fieldsToFetch {
            let fieldFacets = try await getFacetsForField(
                field: field,
                limit: limit,
                transaction: transaction
            )
            result[field] = fieldFacets
        }

        return result
    }

    /// Get facet counts filtered by matching document IDs
    ///
    /// This is used when you want facet counts only for documents matching a search query.
    ///
    /// - Parameters:
    ///   - fields: Fields to get facets for
    ///   - matchingIds: Document IDs that matched the search
    ///   - limit: Maximum values per field
    ///   - transaction: FDB transaction
    /// - Returns: Filtered facet counts
    public func getFacetCountsFiltered(
        fields: [String],
        matchingIds: [Tuple],
        limit: Int = 10,
        transaction: any TransactionProtocol
    ) async throws -> [String: [(value: String, count: Int64)]] {
        var fieldCounts: [String: [String: Int64]] = [:]

        // Initialize counts for each field
        for field in fields {
            fieldCounts[field] = [:]
        }

        // Count facet values for matching documents
        for docId in matchingIds {
            for field in fields {
                let values = try await getDocumentFacetValues(
                    docId: docId,
                    field: field,
                    transaction: transaction
                )
                for value in values {
                    fieldCounts[field]![value, default: 0] += 1
                }
            }
        }

        // Sort and limit results
        var result: [String: [(value: String, count: Int64)]] = [:]
        for (field, counts) in fieldCounts {
            let sorted = counts.sorted { $0.value > $1.value }
            result[field] = Array(sorted.prefix(limit).map { (value: $0.key, count: $0.value) })
        }

        return result
    }

    // MARK: - Private Methods

    /// Add facet values for a document
    private func addFacets(
        for id: Tuple,
        item: Item,
        transaction: any TransactionProtocol
    ) async throws {
        for field in facetFields {
            let values = extractFieldValues(from: item, field: field)

            // Store document's facet values for reverse lookup
            let docFacetKey = docFacetsSubspace.subspace(field).pack(id)
            let valuesElements: [any TupleElement] = values.map { $0 as any TupleElement }
            transaction.setValue(Tuple(valuesElements).pack(), for: docFacetKey)

            // Increment global facet counts
            for value in values {
                let facetKey = facetsSubspace.subspace(field).pack(Tuple(value))
                transaction.atomicOp(key: facetKey, param: int64ToBytes(1), mutationType: .add)
            }
        }
    }

    /// Remove facet values for a document
    private func removeFacets(
        for id: Tuple,
        item: Item,
        transaction: any TransactionProtocol
    ) async throws {
        for field in facetFields {
            let values = extractFieldValues(from: item, field: field)

            // Remove document's facet values
            let docFacetKey = docFacetsSubspace.subspace(field).pack(id)
            transaction.clear(key: docFacetKey)

            // Decrement global facet counts
            for value in values {
                let facetKey = facetsSubspace.subspace(field).pack(Tuple(value))
                transaction.atomicOp(key: facetKey, param: int64ToBytes(-1), mutationType: .add)
            }
        }
    }

    /// Get all facet values for a field
    private func getFacetsForField(
        field: String,
        limit: Int,
        transaction: any TransactionProtocol
    ) async throws -> [(value: String, count: Int64)] {
        let fieldSubspace = facetsSubspace.subspace(field)
        let (begin, end) = fieldSubspace.range()
        let sequence = transaction.getRange(begin: begin, end: end, snapshot: true)

        var facets: [(value: String, count: Int64)] = []

        for try await (key, value) in sequence {
            guard let keyTuple = try? fieldSubspace.unpack(key),
                  let facetValue = keyTuple[0] as? String else {
                continue
            }

            let count = bytesToInt64(value)
            if count > 0 {  // Only include non-zero counts
                facets.append((value: facetValue, count: count))
            }
        }

        // Sort by count descending and limit
        facets.sort { $0.count > $1.count }
        return Array(facets.prefix(limit))
    }

    /// Get facet values for a specific document and field
    private func getDocumentFacetValues(
        docId: Tuple,
        field: String,
        transaction: any TransactionProtocol
    ) async throws -> [String] {
        let docFacetKey = docFacetsSubspace.subspace(field).pack(docId)
        guard let value = try await transaction.getValue(for: docFacetKey, snapshot: true),
              let valuesTuple = try? Tuple.unpack(from: value) else {
            return []
        }

        var values: [String] = []
        for i in 0..<valuesTuple.count {
            if let v = valuesTuple[i] as? String {
                values.append(v)
            }
        }
        return values
    }

    /// Extract field values from an item
    private func extractFieldValues(from item: Item, field: String) -> [String] {
        guard let value = item[dynamicMember: field] else {
            return []
        }

        // Handle arrays
        if let array = value as? [String] {
            return array
        }

        // Handle single values
        if let string = value as? String {
            return [string]
        }

        // Handle other types by converting to string
        if let convertible = value as? CustomStringConvertible {
            return [convertible.description]
        }

        return []
    }

    /// Convert Int64 to little-endian bytes
    private func int64ToBytes(_ value: Int64) -> [UInt8] {
        ByteConversion.int64ToBytes(value)
    }

    /// Convert little-endian bytes to Int64
    private func bytesToInt64(_ bytes: [UInt8]) -> Int64 {
        ByteConversion.bytesToInt64(bytes)
    }
}

// MARK: - Facet Result

/// Result of a faceted search
public struct FacetedSearchResult<T: Persistable>: Sendable {
    /// Matching items from the search
    public let items: [T]

    /// Facet counts for each field
    /// Key: field name, Value: array of (value, count) sorted by count descending
    public let facets: [String: [(value: String, count: Int64)]]

    /// Total number of matching documents (before any limit)
    public let totalCount: Int

    public init(items: [T], facets: [String: [(value: String, count: Int64)]], totalCount: Int) {
        self.items = items
        self.facets = facets
        self.totalCount = totalCount
    }
}
