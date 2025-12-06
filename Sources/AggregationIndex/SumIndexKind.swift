// SumIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extension for SumIndexKind
//
// This file provides the bridge between SumIndexKind (defined in FDBModel)
// and SumIndexMaintainer (defined in this package).

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends SumIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension SumIndexKind: IndexKindMaintainable {
    /// Create a SumIndexMaintainer for this index kind
    ///
    /// This bridges `SumIndexKind<Root, Value>` (metadata) with `SumIndexMaintainer<Item, Value>` (runtime).
    /// The `Value` type parameter is preserved at compile time, enabling type-safe storage.
    ///
    /// **Type-Safe Storage**:
    /// - Integer types (Int, Int64, Int32): Stored as Int64 bytes (precision preserved)
    /// - Floating-point types (Float, Double): Stored as scaled fixed-point Int64
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: SumIndexMaintainer instance with type-safe Value parameter
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Value type is preserved from SumIndexKind<Root, Value>
        return SumIndexMaintainer<Item, Value>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
