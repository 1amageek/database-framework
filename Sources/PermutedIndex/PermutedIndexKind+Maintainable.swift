// PermutedIndexKind+Maintainable.swift
// PermutedIndexLayer - PERMUTED indexes for alternative field orderings (FDB-dependent)
//
// Provides IndexKindMaintainable conformance for PermutedIndexKind.
// PermutedIndexKind, Permutation, and errors are defined in PermutedIndexModel (FDB-independent).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import Permuted

// Re-export PermutedIndexModel types for convenience
@_exported import Permuted

// MARK: - IndexKindMaintainable Conformance

extension PermutedIndexKind: IndexKindMaintainable {
    /// Create index maintainer for permuted indexes
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return PermutedIndexMaintainer<Item>(
            index: index,
            permutation: permutation,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression
        )
    }
}
