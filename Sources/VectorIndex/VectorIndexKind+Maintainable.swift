// VectorIndexKind+Maintainable.swift
// VectorIndexLayer - Vector similarity search indexes (FDB-dependent)
//
// Provides IndexKindMaintainable conformance for VectorIndexKind.
// VectorIndexKind and VectorMetric are defined in VectorIndexModel (FDB-independent).

import Foundation
import Core
import DatabaseEngine
import FoundationDB
import Vector

// Re-export VectorIndexModel types for convenience
@_exported import Vector

// MARK: - IndexKindMaintainable Conformance

extension VectorIndexKind: IndexKindMaintainable {
    /// Create index maintainer for vector indexes
    ///
    /// **Algorithm Selection**:
    /// 1. Search `configurations` for `VectorIndexConfiguration` matching this index
    /// 2. If found with `.hnsw` algorithm: use `HNSWIndexMaintainer`
    /// 3. Otherwise: use `FlatVectorIndexMaintainer` (safe default)
    ///
    /// **Performance Characteristics**:
    /// - **Flat**: O(n) search, 100% recall, no setup required
    /// - **HNSW**: O(log n) search, ~95-99% recall, limited to ~500 nodes inline
    ///
    /// - Parameters:
    ///   - index: Index definition
    ///   - subspace: FDB subspace for this index
    ///   - idExpression: Expression for extracting item's unique identifier
    ///   - configurations: Index configurations (may contain VectorIndexConfiguration)
    /// - Returns: Appropriate vector index maintainer
    public func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration]
    ) -> any IndexMaintainer<Item> {
        // Search for VectorIndexConfiguration matching this index
        // Use type-safe protocol cast instead of Mirror reflection
        let matchingConfig = configurations.first { config in
            type(of: config).kindIdentifier == VectorIndexKind.identifier &&
            config.indexName == index.name
        } as? _VectorIndexConfiguration

        // Check if HNSW algorithm is requested
        if let vectorConfig = matchingConfig {
            switch vectorConfig.algorithm {
            case .flat:
                // Explicit flat selection - use flat maintainer
                break

            case .hnsw(let hnswParams):
                // HNSW requested - convert parameters and create HNSW maintainer
                let params = HNSWParameters(
                    m: hnswParams.m,
                    efConstruction: hnswParams.efConstruction,
                    efSearch: hnswParams.efSearch
                )
                return HNSWIndexMaintainer<Item>(
                    index: index,
                    kind: self,
                    subspace: subspace.subspace(index.name),
                    idExpression: idExpression,
                    parameters: params
                )
            }
        }

        // Default: flat scan (safe, exact, no memory requirements)
        return FlatVectorIndexMaintainer<Item>(
            index: index,
            kind: self,
            subspace: subspace.subspace(index.name),
            idExpression: idExpression
        )
    }
}
