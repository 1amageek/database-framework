import DatabaseEngine
import DatabaseKit
import StorageKit

extension IndexQueryContext {
    /// Trains one schema-declared IVF or PQ index from its retained vectors.
    ///
    /// Training and the corresponding list or code replacement commit in one
    /// transaction, so readers never observe a partially trained generation.
    public func trainVectorIndex<Model: Persistable>(
        named indexName: String,
        for type: Model.Type,
        configuration: TransactionConfiguration = .batch,
        resourceLimits: VectorTrainingResourceLimits = .default
    ) async throws {
        try await withWritableIndex(
            named: indexName,
            indexType: .vector,
            for: type,
            requiredAccess: .administer,
            configuration: configuration
        ) { readableIndex, transaction in
            let descriptor = readableIndex.descriptor
            let specification = try VectorIndexSpecification(
                descriptor.declaration.definition
            )
            guard descriptor.fieldNames.count == 1,
                  let fieldName = descriptor.fieldNames.first else {
                throw VectorIndexError.invalidStructure(
                    "Vector training requires exactly one indexed field"
                )
            }
            let configurations = context.container.runtimeConfiguration
                .indexConfigurations(named: indexName)
            guard
                let runtimePolicy = try VectorRuntimePolicy.resolve(
                    in: configurations
                )
            else {
                throw VectorIndexError.invalidArgument(
                    "Vector training requires an explicit IVF or PQ runtime configuration"
                )
            }
            let index = ResolvedIndex(
                descriptor: descriptor,
                rootExpression: FieldKeyExpression(fieldName: fieldName),
                itemTypes: Set([Model.persistableType]),
            )
            let identifier = FieldKeyExpression(fieldName: "id")

            switch runtimePolicy.algorithm {
            case .ivf(let parameters):
                let maintainer = IVFIndexMaintainer<Model>(
                    index: index,
                    dimensions: specification.dimensions,
                    metric: specification.metric,
                    subspace: readableIndex.subspace,
                    idExpression: identifier,
                    parameters: IVFParameters(
                        nlist: parameters.nlist,
                        nprobe: parameters.nprobe,
                        kmeansIterations: parameters.kmeansIterations
                    ),
                    trainingResourceLimits: resourceLimits
                )
                try await maintainer.trainStoredVectors(
                    transaction: transaction
                )
            case .pq(let parameters):
                let maintainer = try PQIndexMaintainer<Model>(
                    index: index,
                    dimensions: specification.dimensions,
                    metric: specification.metric,
                    subspace: readableIndex.subspace,
                    idExpression: identifier,
                    parameters: PQParameters(
                        m: parameters.m,
                        ksub: 256,
                        niter: parameters.niter
                    ),
                    trainingResourceLimits: resourceLimits
                )
                try await maintainer.train(transaction: transaction)
            case .flat, .hnsw:
                throw VectorIndexError.invalidArgument(
                    "Only IVF and PQ vector indexes require training"
                )
            }
        }
    }
}
