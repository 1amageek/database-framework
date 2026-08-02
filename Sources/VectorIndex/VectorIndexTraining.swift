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
        let descriptor = try indexDescriptor(
            named: indexName,
            kindIdentifier: VectorIndexSpecification.identifier,
            for: type
        )
        let specification = try VectorIndexSpecification(descriptor.kind)
        guard specification.metadata.fieldNames.count == 1,
              let fieldName = specification.metadata.fieldNames.first else {
            throw VectorIndexError.invalidStructure(
                "Vector training requires exactly one indexed field"
            )
        }

        let configurations = context.container.indexConfigurations[indexName] ?? []
        guard let runtimePolicy = try VectorRuntimePolicy.resolve(
            in: configurations
        ) else {
            throw VectorIndexError.invalidArgument(
                "Vector training requires an explicit IVF or PQ runtime configuration"
            )
        }

        try await withReadableIndex(
            named: indexName,
            kindIdentifier: VectorIndexSpecification.identifier,
            for: type,
            configuration: configuration
        ) { readableIndex, transaction in
            guard let readableIndex else {
                throw VectorIndexError.invalidStructure(
                    "Vector index is not readable; complete migration before training"
                )
            }
            let indexSubspace: Subspace
            if let subspaceKey = runtimePolicy.subspaceKey {
                indexSubspace = readableIndex.subspace.subspace(subspaceKey)
            } else {
                indexSubspace = readableIndex.subspace
            }
            let index = Index(
                name: descriptor.name,
                kind: descriptor.kind,
                rootExpression: FieldKeyExpression(fieldName: fieldName),
                subspaceKey: descriptor.name,
                itemTypes: Set([Model.persistableType]),
                isUnique: descriptor.isUnique,
                storedFieldNames: descriptor.storedFieldNames
            )
            let identifier = FieldKeyExpression(fieldName: "id")

            switch runtimePolicy.algorithm {
            case .ivf(let parameters):
                let maintainer = IVFIndexMaintainer<Model>(
                    index: index,
                    dimensions: specification.dimensions,
                    metric: specification.metric,
                    subspace: indexSubspace,
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
                    subspace: indexSubspace,
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
