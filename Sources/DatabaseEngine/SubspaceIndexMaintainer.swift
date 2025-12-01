// SubspaceIndexMaintainer.swift
// DatabaseEngine - Protocol for subspace-based IndexMaintainers
//
// Provides default implementations for IndexMaintainers that store data in FDB subspaces.

import Foundation
import Core
import FoundationDB

/// Protocol for IndexMaintainers that store index data in an FDB subspace
///
/// This protocol defines the common structure for IndexMaintainers that use
/// FDB subspaces for storage, enabling shared default implementations.
///
/// **Purpose**:
/// - Reduce code duplication across subspace-based IndexMaintainer implementations
/// - Ensure consistent key validation and ID resolution
/// - Provide optimized, inlinable default implementations
///
/// **Required Properties**:
/// - `index`: The index definition containing field expressions and keyPaths
/// - `subspace`: The FDB subspace for this index
/// - `idExpression`: Expression for extracting item's unique identifier
///
/// **Provided Methods** (via extension):
/// - `resolveItemId(for:providedId:)`: Extract or use provided ID
/// - `packAndValidate(_:in:)`: Pack tuple and validate key size
/// - `evaluateIndexFields(from:)`: Extract index field values from item
///
/// **Usage**:
/// ```swift
/// struct MyIndexMaintainer<Item: Persistable>: SubspaceIndexMaintainer {
///     public let index: Index
///     public let subspace: Subspace
///     public let idExpression: KeyExpression
///
///     func buildKey(for item: Item, id: Tuple?) throws -> FDB.Bytes {
///         let itemId = try resolveItemId(for: item, providedId: id)
///         // ... build key elements ...
///         return try packAndValidate(Tuple(elements))
///     }
/// }
/// ```
public protocol SubspaceIndexMaintainer: IndexMaintainer {
    /// The index definition
    var index: Index { get }

    /// The FDB subspace for this index
    var subspace: Subspace { get }

    /// Expression for extracting item's unique identifier
    var idExpression: KeyExpression { get }
}

// MARK: - Default Implementations

extension SubspaceIndexMaintainer {
    /// Resolve item ID from provided tuple or extract from item
    ///
    /// This is a common pattern across all IndexMaintainers:
    /// - If `providedId` is given, use it directly
    /// - Otherwise, extract ID from item using `idExpression`
    ///
    /// - Parameters:
    ///   - item: The item to extract ID from (if providedId is nil)
    ///   - providedId: Optional pre-extracted ID tuple
    /// - Returns: The item's ID as a Tuple
    /// - Throws: Error if ID extraction fails
    @inlinable
    public func resolveItemId(for item: Item, providedId: Tuple?) throws -> Tuple {
        if let providedId = providedId {
            return providedId
        }
        return try DataAccess.extractId(from: item, using: idExpression)
    }

    /// Extract ID elements as array from a Tuple
    ///
    /// Efficiently extracts all non-nil elements from a Tuple into an array.
    /// Useful when building composite keys that include the ID.
    ///
    /// - Parameter id: The ID tuple
    /// - Returns: Array of tuple elements
    @inlinable
    public func extractIdElements(from id: Tuple) -> [any TupleElement] {
        var elements: [any TupleElement] = []
        elements.reserveCapacity(id.count)
        for i in 0..<id.count {
            if let element = id[i] {
                elements.append(element)
            }
        }
        return elements
    }

    /// Pack tuple into key and validate size
    ///
    /// Combines packing and validation into a single operation.
    /// Uses the provided subspace or falls back to `self.subspace`.
    ///
    /// - Parameters:
    ///   - tuple: The tuple to pack
    ///   - targetSubspace: Optional subspace to use (defaults to self.subspace)
    /// - Returns: Packed and validated key bytes
    /// - Throws: `FDBLimitError.keyTooLarge` if key exceeds 10KB
    @inlinable
    public func packAndValidate(_ tuple: Tuple, in targetSubspace: Subspace? = nil) throws -> FDB.Bytes {
        let key = (targetSubspace ?? subspace).pack(tuple)
        try validateKeySize(key)
        return key
    }

    /// Evaluate index fields from item using optimized DataAccess
    ///
    /// Uses KeyPath extraction when available, falls back to KeyExpression.
    ///
    /// - Parameter item: The item to extract fields from
    /// - Returns: Array of field values as TupleElements
    /// - Throws: Error if field extraction fails
    @inlinable
    public func evaluateIndexFields(from item: Item) throws -> [any TupleElement] {
        return try DataAccess.evaluateIndexFields(
            from: item,
            keyPaths: index.keyPaths,
            expression: index.rootExpression
        )
    }
}
