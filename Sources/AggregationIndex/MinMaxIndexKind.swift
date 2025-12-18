// MinMaxIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extensions for MinIndexKind and MaxIndexKind
//
// This file provides the bridge between MinIndexKind/MaxIndexKind (defined in FDBModel)
// and MinIndexMaintainer/MaxIndexMaintainer (defined in this package).

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - MinIndexKind IndexKindMaintainable Extension

/// Extends MinIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension MinIndexKind: IndexKindMaintainable {
    /// Create a MinIndexMaintainer for this index kind
    ///
    /// This bridges `MinIndexKind<Root, Value>` (metadata) with `MinIndexMaintainer<Item, Value>` (runtime).
    /// The `Value` type parameter is preserved at compile time for type-safe result.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: MinIndexMaintainer instance with type-safe Value parameter
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Value type is preserved from MinIndexKind<Root, Value>
        return MinIndexMaintainer<Item, Value>(
            index: index,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression
        )
    }
}

// MARK: - MaxIndexKind IndexKindMaintainable Extension

/// Extends MaxIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension MaxIndexKind: IndexKindMaintainable {
    /// Create a MaxIndexMaintainer for this index kind
    ///
    /// This bridges `MaxIndexKind<Root, Value>` (metadata) with `MaxIndexMaintainer<Item, Value>` (runtime).
    /// The `Value` type parameter is preserved at compile time for type-safe result.
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: MaxIndexMaintainer instance with type-safe Value parameter
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Value type is preserved from MaxIndexKind<Root, Value>
        return MaxIndexMaintainer<Item, Value>(
            index: index,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression
        )
    }
}
