// VectorIndexKind+Maintainable.swift
// VectorIndexLayer - Vector similarity search indexes (FDB-dependent)
//
// Provides IndexMaintainerFactory conformance for VectorIndexKind.
// VectorIndexKind and VectorMetric are defined in VectorIndexModel (FDB-independent).

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import Core
import DatabaseEngine
import StorageKit
import Vector

// Re-export VectorIndexModel types for convenience
@_exported import Vector

extension VectorIndexKind {
    internal func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexConfiguration],
        graphCache: HNSWGraphCache
    ) -> any IndexMaintainer<Item> {
        // Search for VectorIndexConfiguration matching this index
        // Use type-safe protocol cast instead of Mirror reflection
        let matchingConfig = configurations.first { config in
            type(of: config).kindIdentifier == VectorIndexKind.identifier &&
            config.indexName == index.name
        } as? _VectorIndexConfiguration

        // Build subspace with optional subspaceKey
        // Note: subspace is already index-specific (caller passes indexSubspace.subspace(indexName))
        let indexSubspace: Subspace
        if let subspaceKey = matchingConfig?.subspaceKey {
            indexSubspace = subspace.subspace(subspaceKey)
        } else {
            indexSubspace = subspace
        }

        // Check algorithm selection
        if let vectorConfig = matchingConfig {
            switch vectorConfig.algorithm {
            case .auto:
                // Auto mode: Use flat maintainer for storage (universal format)
                // Query layer handles algorithm selection based on dataset size
                // This ensures data can be queried with any algorithm
                break

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
                    dimensions: dimensions,
                    metric: metric,
                    subspace: indexSubspace,
                    idExpression: idExpression,
                    parameters: params,
                    graphCache: graphCache
                )

            case .ivf(let ivfParams):
                // IVF requested - create IVF maintainer
                let params = IVFParameters(
                    nlist: ivfParams.nlist,
                    nprobe: ivfParams.nprobe,
                    kmeansIterations: ivfParams.kmeansIterations
                )
                return IVFIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: indexSubspace,
                    idExpression: idExpression,
                    parameters: params
                )

            case .pq(let pqParams):
                // PQ requested - create PQ maintainer
                let params = PQParameters(
                    m: pqParams.m,
                    ksub: 256,
                    niter: pqParams.niter
                )
                return PQIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: indexSubspace,
                    idExpression: idExpression,
                    parameters: params
                )
            }
        }

        // Default: flat scan (safe, exact, no memory requirements)
        return FlatVectorIndexMaintainer<Item>(
            index: index,
            dimensions: dimensions,
            metric: metric,
            subspace: indexSubspace,
            idExpression: idExpression
        )
    }
}
