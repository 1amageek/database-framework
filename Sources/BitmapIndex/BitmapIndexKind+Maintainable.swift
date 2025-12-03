// BitmapIndexKind+Maintainable.swift
// BitmapIndexLayer - IndexKindMaintainable extension for BitmapIndexKind
//
// Provides efficient set operations on low-cardinality fields using Roaring Bitmaps.
// Reference: Lemire et al., "Roaring Bitmaps", 2016

import Foundation
import Core
import DatabaseEngine
import FoundationDB

// MARK: - IndexKindMaintainable Extension

/// Extends BitmapIndexKind with IndexKindMaintainable conformance
extension Core.BitmapIndexKind: IndexKindMaintainable {
    /// Create a BitmapIndexMaintainer for this index kind
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return BitmapIndexMaintainer<Item>(
            index: index,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
