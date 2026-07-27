#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif
import DatabaseKit
import DatabaseEngine
import StorageKit

extension VectorIndexSpecification {
    func makeIndexMaintainer<Item: Persistable>(
        index: Index,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        graphCache: HNSWGraphCache
    ) -> any IndexMaintainer<Item> {
        let matchingConfiguration = configurations.first { configuration in
            type(of: configuration).kindIdentifier == Self.identifier
                && configuration.indexName == index.name
        } as? VectorIndexRuntimeConfiguration

        let indexSubspace: Subspace
        if let subspaceKey = matchingConfiguration?.subspaceKey {
            indexSubspace = subspace.subspace(subspaceKey)
        } else {
            indexSubspace = subspace
        }

        if let matchingConfiguration {
            switch matchingConfiguration.algorithm {
            case .auto, .flat:
                break
            case .hnsw(let parameters):
                return HNSWIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: indexSubspace,
                    idExpression: idExpression,
                    parameters: HNSWParameters(
                        m: parameters.m,
                        efConstruction: parameters.efConstruction,
                        efSearch: parameters.efSearch
                    ),
                    graphCache: graphCache
                )
            case .ivf(let parameters):
                return IVFIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: indexSubspace,
                    idExpression: idExpression,
                    parameters: IVFParameters(
                        nlist: parameters.nlist,
                        nprobe: parameters.nprobe,
                        kmeansIterations: parameters.kmeansIterations
                    )
                )
            case .pq(let parameters):
                return PQIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: indexSubspace,
                    idExpression: idExpression,
                    parameters: PQParameters(
                        m: parameters.m,
                        ksub: 256,
                        niter: parameters.niter
                    )
                )
            }
        }

        return FlatVectorIndexMaintainer<Item>(
            index: index,
            dimensions: dimensions,
            metric: metric,
            subspace: indexSubspace,
            idExpression: idExpression
        )
    }
}
