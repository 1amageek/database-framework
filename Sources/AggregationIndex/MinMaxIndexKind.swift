// MinMaxIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extensions for MinIndexKind and MaxIndexKind
//
// This file provides the bridge between MinIndexKind/MaxIndexKind (defined in FDBModel)
// and MinIndexMaintainer/MaxIndexMaintainer (defined in this package).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB

// MARK: - MinIndexKind IndexKindMaintainable Extension

/// Extends MinIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension MinIndexKind: IndexKindMaintainable {
    /// Create a MinIndexMaintainer for this index kind
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: MinIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return MinIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}

// MARK: - MaxIndexKind IndexKindMaintainable Extension

/// Extends MaxIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension MaxIndexKind: IndexKindMaintainable {
    /// Create a MaxIndexMaintainer for this index kind
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: MaxIndexMaintainer instance
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return MaxIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
