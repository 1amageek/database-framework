// FullTextIndexKind+Maintainable.swift
// FullTextIndexLayer - Full-text search with inverted index (FDB-dependent)
//
// Provides IndexKindMaintainable conformance for FullTextIndexKind.
// FullTextIndexKind and TokenizationStrategy are defined in FullTextIndexModel (FDB-independent).

import Foundation
import Core
import Core
import DatabaseEngine
import FoundationDB
import FullText

// Re-export FullTextIndexModel types for convenience
@_exported import FullText

// MARK: - IndexKindMaintainable Conformance

extension FullTextIndexKind: IndexKindMaintainable {
    /// Create index maintainer for full-text indexes
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        return FullTextIndexMaintainer<Item>(
            index: index,
            tokenizer: tokenizer,
            storePositions: storePositions,
            ngramSize: ngramSize,
            minTermLength: minTermLength,
            subspace: subspace,  // Already index-specific from caller
            idExpression: idExpression
        )
    }
}
