import DatabaseEngine
import DatabaseKit
import StorageKit

extension VectorIndexSpecification {
    func makeIndexMaintainer<Item: PersistedEntityValue>(
        index: ResolvedIndex,
        subspace: Subspace,
        idExpression: KeyExpression,
        configurations: [any IndexRuntimeConfiguration],
        graphCache: HNSWGraphCache,
        graphResourceLimits: HNSWGraphResourceLimits,
        trainingResourceLimits: VectorTrainingResourceLimits
    ) throws -> any IndexMaintainer<Item> {
        let matchingConfigurations = configurations.filter { configuration in
            configuration.indexType == .vector
                && configuration.indexName == index.name
        }
        let runtimePolicy = try VectorRuntimePolicy.resolve(
            in: matchingConfigurations
        )

        if let runtimePolicy {
            switch runtimePolicy.algorithm {
            case .flat:
                break
            case .hnsw(let parameters):
                return HNSWIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: subspace,
                    idExpression: idExpression,
                    parameters: HNSWParameters(
                        m: parameters.m,
                        efConstruction: parameters.efConstruction,
                        efSearch: parameters.efSearch
                    ),
                    graphCache: graphCache,
                    resourceLimits: graphResourceLimits
                )
            case .ivf(let parameters):
                return IVFIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: subspace,
                    idExpression: idExpression,
                    parameters: IVFParameters(
                        nlist: parameters.nlist,
                        nprobe: parameters.nprobe,
                        kmeansIterations: parameters.kmeansIterations
                    ),
                    trainingResourceLimits: trainingResourceLimits
                )
            case .pq(let parameters):
                return try PQIndexMaintainer<Item>(
                    index: index,
                    dimensions: dimensions,
                    metric: metric,
                    subspace: subspace,
                    idExpression: idExpression,
                    parameters: PQParameters(
                        m: parameters.m,
                        ksub: 256,
                        niter: parameters.niter
                    ),
                    trainingResourceLimits: trainingResourceLimits
                )
            }
        }

        return FlatVectorIndexMaintainer<Item>(
            index: index,
            dimensions: dimensions,
            metric: metric,
            subspace: subspace,
            idExpression: idExpression
        )
    }
}
