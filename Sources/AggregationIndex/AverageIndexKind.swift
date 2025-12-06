// AverageIndexKind+Maintainable.swift
// AggregationIndexLayer - IndexKindMaintainable extension for AverageIndexKind
//
// This file provides the bridge between AverageIndexKind (defined in FDBModel)
// and AverageIndexMaintainer (defined in this package).

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends AverageIndexKind (from FDBModel) with IndexKindMaintainable conformance
extension AverageIndexKind: IndexKindMaintainable {
    /// Create an AverageIndexMaintainer for this index kind
    ///
    /// This bridges `AverageIndexKind<Root, Value>` (metadata) with `AverageIndexMaintainer<Item, Value>` (runtime).
    /// The `Value` type parameter is preserved at compile time, enabling type-safe storage.
    ///
    /// **Type-Safe Storage**:
    /// - Integer types (Int, Int64, Int32): Sum stored as Int64 bytes
    /// - Floating-point types (Float, Double): Sum stored as scaled fixed-point Int64
    ///
    /// **Note**: Result type is always Double (average = sum / count)
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    /// - Returns: AverageIndexMaintainer instance with type-safe Value parameter
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Value type is preserved from AverageIndexKind<Root, Value>
        return AverageIndexMaintainer<Item, Value>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
