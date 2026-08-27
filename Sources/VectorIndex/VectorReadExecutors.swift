import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

enum VectorReadParameter {
    static let fieldName = "fieldName"
    static let dimensions = "dimensions"
    static let queryVector = "queryVector"
    static let k = "k"
    static let metric = "metric"
}
public enum VectorReadExecutors {
    public static func polymorphicIndexExecutor(
        graphResourceLimits: HNSWGraphResourceLimits = .default,
        maximumGraphCacheCost: Int = 24 * 1_024 * 1_024
    ) -> any PolymorphicIndexReadExecutor {
        PolymorphicVectorReadExecutor(
            graphResourceLimits: graphResourceLimits,
            maximumGraphCacheCost: maximumGraphCacheCost
        )
    }

    public static func register(
        with definition: inout EntityRuntimeDefinition,
        graphResourceLimits: HNSWGraphResourceLimits = .default,
        maximumGraphCacheCost: Int = 24 * 1_024 * 1_024
    ) throws(DatabaseRuntimeConfigurationError) {
        try definition.register(
            VectorReadExecutor(
                graphResourceLimits: graphResourceLimits,
                maximumGraphCacheCost: maximumGraphCacheCost
            )
        )
    }
}

private enum VectorReadError: Error, Sendable {
    case missingParameter(String)
    case invalidParameter(String)
    case indexNotFound(String)
    case fetchedItemCountMismatch(expected: Int, actual: Int)
    case missingFetchedEntity(position: Int)
}

private struct VectorReadExecutor: IndexReadExecutor {
    let indexType: IndexType = .vector
    private let graphCache: HNSWGraphCache
    private let graphResourceLimits: HNSWGraphResourceLimits

    init(
        graphResourceLimits: HNSWGraphResourceLimits,
        maximumGraphCacheCost: Int
    ) {
        self.graphResourceLimits = graphResourceLimits
        self.graphCache = HNSWGraphCache(maximumCost: maximumGraphCacheCost)
    }

    func additionalRequiredFieldNames(
        indexScan: IndexScanSource
    ) throws -> Set<String> {
        []
    }

    func executeRows(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        index: IndexDescriptor,
        indexScan: IndexScanSource,
        entity: Schema.Entity,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let transaction = session.transaction
        let fieldName = try requireString(VectorReadParameter.fieldName, from: indexScan.parameters)
        let dimensions = try requireInt(VectorReadParameter.dimensions, from: indexScan.parameters)
        let queryVector = try requireFloat32Vector(
            VectorReadParameter.queryVector,
            from: indexScan.parameters
        )
        let k = try requireInt(VectorReadParameter.k, from: indexScan.parameters)
        let metricRawValue = try requireString(VectorReadParameter.metric, from: indexScan.parameters)

        try session.requireCanonicalIndexReadAuthorization(
            entity: entity,
            index: index,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )

        let specification = try VectorIndexSpecification(
            index.declaration.definition
        )
        guard index.type == indexType,
            index.fieldNames == [fieldName],
              specification.dimensions == dimensions,
              specification.metric.rawValue == metricRawValue,
              queryVector.count == dimensions,
              k > 0 else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.fieldName
            )
        }
        let boundedK = min(
            k,
            try options.workMeter.storageReadLimitWithSentinel()
        )

        let search = PolymorphicVectorReadExecutor(
            graphCache: graphCache,
            graphResourceLimits: graphResourceLimits
        )
        let algorithm = try search.resolveAlgorithm(
            indexName: index.name,
            session: session
        )
        guard let readableIndex = try await session.readableIndex(
            named: index.name,
            indexType: indexType,
            forEntityName: entity.name,
            partitions: partitions
        ) else {
            return .empty
        }
        let matches = try await search.executeSearch(
                specification: specification,
                indexSubspace: readableIndex.subspace,
                queryVector: queryVector,
                k: boundedK,
                transaction: transaction,
                snapshot: options.consistency == .snapshot,
                workMeter: options.workMeter,
                algorithm: algorithm
        )
        let fetched = try await session
            .fetchRetainedPersistedModelsPreservingOrder(
                entity: entity,
                primaryKeys: matches,
                partitions: partitions,
                snapshot: options.consistency == .snapshot
        )
        guard fetched.count == matches.count else {
            throw VectorReadError.fetchedItemCountMismatch(
                expected: matches.count,
                actual: fetched.count
            )
        }
        let validator = VectorCanonicalStateValidator(
            indexSubspace: readableIndex.subspace,
            fieldName: fieldName,
            dimensions: specification.dimensions,
            algorithm: algorithm
        )
        for index in 0..<matches.count {
            var isPresent = false
            try await matches.withRetainedPrimaryKey(at: index) {
                primaryKey in
                isPresent = try await validator.validate(
                    primaryKey: primaryKey,
                    entities: fetched,
                    position: index,
                    transaction: transaction,
                    snapshot: options.consistency == .snapshot,
                    workMeter: options.workMeter
                )
            }
            guard isPresent else {
                throw VectorReadError.missingFetchedEntity(position: index)
            }
        }
        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: matches.count
        ) { rows in
            for index in 0..<matches.count {
                let appended = try fetched.appendIndexRow(
                    at: index,
                    to: &rows,
                    additionalAnnotation: (
                        name: "distance",
                        value: .float64(matches.distance(at: index))
                    )
                )
                guard appended else {
                    throw VectorReadError.missingFetchedEntity(
                        position: index
                    )
                }
            }
        }
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw VectorReadError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw VectorReadError.missingParameter(key)
        }
        guard let result = Int(exactly: value) else {
            throw VectorReadError.invalidParameter(key)
        }
        return result
    }

    private func requireFloat32Vector(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Vector {
        guard let vector = parameters[key]?.vectorValue,
              vector.elementType == .float32 else {
            throw VectorReadError.missingParameter(key)
        }
        return vector
    }
}

private struct PolymorphicVectorReadExecutor: PolymorphicIndexReadExecutor {
    let indexType: IndexType = .vector
    private let graphCache: HNSWGraphCache
    private let graphResourceLimits: HNSWGraphResourceLimits

    init(
        graphResourceLimits: HNSWGraphResourceLimits,
        maximumGraphCacheCost: Int
    ) {
        self.graphResourceLimits = graphResourceLimits
        self.graphCache = HNSWGraphCache(maximumCost: maximumGraphCacheCost)
    }

    fileprivate init(
        graphCache: HNSWGraphCache,
        graphResourceLimits: HNSWGraphResourceLimits
    ) {
        self.graphCache = graphCache
        self.graphResourceLimits = graphResourceLimits
    }

    func additionalRequiredFieldNames(
        indexScan: IndexScanSource
    ) throws -> Set<String> {
        []
    }

    func executeRows(
        session: DatabaseReadSession,
        selectQuery: SelectQuery,
        index: IndexDeclaration<String>,
        indexScan: IndexScanSource,
        group: PolymorphicGroup,
        options: ReadExecutionContext,
        partitions: FieldObject
    ) async throws -> IndexReadResult {
        let transaction = session.transaction
        let fieldName = try requireString(
            VectorReadParameter.fieldName,
            from: indexScan.parameters
        )
        let dimensions = try requireInt(
            VectorReadParameter.dimensions,
            from: indexScan.parameters
        )
        let queryVector = try requireFloat32Vector(
            VectorReadParameter.queryVector,
            from: indexScan.parameters
        )
        let k = try requireInt(
            VectorReadParameter.k,
            from: indexScan.parameters
        )
        let metricRawValue = try requireString(
            VectorReadParameter.metric,
            from: indexScan.parameters
        )
        guard VectorMetric(rawValue: metricRawValue) != nil, k > 0 else {
            throw VectorReadError.invalidParameter(VectorReadParameter.metric)
        }

        try session.requireCanonicalPolymorphicIndexReadAuthorization(
            index: index,
            group: group,
            selectQuery: selectQuery,
            additionalFieldNames: []
        )
        let concreteDescriptor = try resolveConcreteDescriptor(
            schema: session.schema,
            group: group,
            indexName: index.name
        )
        let specification = try resolveSpecification(
            fieldName: fieldName,
            dimensions: dimensions,
            metricRawValue: metricRawValue,
            groupDescriptor: index,
            concreteDescriptor: concreteDescriptor
        )
        let boundedK = min(
            k,
            try options.workMeter.storageReadLimitWithSentinel()
        )
        let algorithm = try resolveAlgorithm(
            indexName: index.name,
            session: session
        )
        guard let readableIndex = try await session.readablePolymorphicIndex(
            index,
            in: group
        ) else {
            return .empty
        }

        let matches = try await executeSearch(
            specification: specification,
            indexSubspace: readableIndex.subspace,
            queryVector: queryVector,
            k: boundedK,
            transaction: transaction,
            snapshot: options.consistency == .snapshot,
            workMeter: options.workMeter,
            algorithm: algorithm
        )
        let entities = try await session
            .fetchRetainedPolymorphicItemsPreservingOrder(
                group: group,
                ids: matches,
                snapshot: options.consistency == .snapshot
            )
        guard entities.count == matches.count else {
            throw VectorReadError.fetchedItemCountMismatch(
                expected: matches.count,
                actual: entities.count
            )
        }

        let validator = VectorCanonicalStateValidator(
            indexSubspace: readableIndex.subspace,
            fieldName: fieldName,
            dimensions: specification.dimensions,
            algorithm: algorithm
        )
        for index in 0..<matches.count {
            var isPresent = false
            try await matches.withRetainedPrimaryKey(
                at: index
            ) { primaryKey in
                isPresent = try await validator.validate(
                    primaryKey: primaryKey,
                    entities: entities,
                    position: index,
                    transaction: transaction,
                    snapshot: options.consistency == .snapshot,
                    workMeter: options.workMeter
                )
            }
            guard isPresent else {
                throw VectorReadError.missingFetchedEntity(position: index)
            }
        }

        return try IndexReadResult.build(
            workMeter: options.workMeter,
            expectedCount: matches.count
        ) { rows in
            for index in 0..<matches.count {
                let appended = try entities.appendIndexRow(
                    at: index,
                    to: &rows,
                    additionalAnnotation: (
                        name: "distance",
                        value: .float64(matches.distance(at: index))
                    )
                )
                guard appended else {
                    throw VectorReadError.missingFetchedEntity(
                        position: index
                    )
                }
            }
        }
    }

    fileprivate func executeSearch(
        specification: VectorIndexSpecification,
        indexSubspace: Subspace,
        queryVector: Vector,
        k: Int,
        transaction: any TransactionReadAccess,
        snapshot: Bool,
        workMeter: DatabaseWorkMeter,
        algorithm: VectorAlgorithm
    ) async throws -> VectorRetainedMatches {
        switch algorithm {
        case .flat:
            return try await FlatVectorIndexReader(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric
            ).search(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )

        case .hnsw(let hnswParams):
            try workMeter.checkpoint(at: .indexScan)
            let parameters = HNSWParameters(
                m: hnswParams.m,
                efConstruction: hnswParams.efConstruction,
                efSearch: hnswParams.efSearch
            )
            let storage = HNSWIndexStorage(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric,
                parameters: parameters,
                graphCache: graphCache,
                resourceLimits: graphResourceLimits
            )
            let results = try await HNSWIndexReader(storage: storage).search(
                queryVector: queryVector,
                k: k,
                parameters: HNSWSearchParameters(
                    ef: max(k, hnswParams.efSearch)
                ),
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
            return results

        case .ivf(let ivfParams):
            return try await IVFIndexReader(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric,
                parameters: IVFParameters(
                    nlist: ivfParams.nlist,
                    nprobe: ivfParams.nprobe,
                    kmeansIterations: ivfParams.kmeansIterations
                )
            ).search(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )

        case .pq(let pqParams):
            return try await PQIndexReader(
                subspace: indexSubspace,
                dimensions: specification.dimensions,
                metric: specification.metric,
                parameters: PQParameters(
                    m: pqParams.m,
                    ksub: 256,
                    niter: pqParams.niter
                )
            ).search(
                queryVector: queryVector,
                k: k,
                transaction: transaction,
                snapshot: snapshot,
                workMeter: workMeter
            )
        }
    }

    fileprivate func resolveAlgorithm(
        indexName: String,
        session: DatabaseReadSession
    ) throws -> VectorAlgorithm {
        let configurations = try session.indexConfigurations(named: indexName)
        return try VectorRuntimePolicy.resolve(in: configurations)?.algorithm
            ?? .flat
    }

    private func resolveSpecification(
        fieldName: String,
        dimensions: Int,
        metricRawValue: String,
        groupDescriptor: IndexDeclaration<String>,
        concreteDescriptor: IndexDescriptor
    ) throws -> VectorIndexSpecification {
        guard
            case .vector(
                let groupField,
                let groupDimensions,
                let groupMetric
            ) = groupDescriptor.definition,
            groupField == fieldName
        else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.fieldName
            )
        }
        let specification = try VectorIndexSpecification(
            concreteDescriptor.declaration.definition
        )
        guard specification.dimensions == groupDimensions,
            specification.metric == groupMetric,
            specification.dimensions == dimensions else {
            throw VectorReadError.invalidParameter(
                VectorReadParameter.dimensions
            )
        }
        guard specification.metric.rawValue == metricRawValue else {
            throw VectorReadError.invalidParameter(VectorReadParameter.metric)
        }
        return specification
    }

    private func resolveConcreteDescriptor(
        schema: Schema,
        group: PolymorphicGroup,
        indexName: String
    ) throws -> IndexDescriptor {
        for memberTypeName in group.memberTypeNames {
            if let descriptor = schema.polymorphicIndexDescriptors(
                identifier: group.identifier,
                memberTypeName: memberTypeName
            ).first(
                where: { $0.name == indexName }
            ) {
                return descriptor
            }
        }
        throw VectorReadError.indexNotFound(indexName)
    }

    private func requireString(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> String {
        guard let value = parameters[key]?.stringValue else {
            throw VectorReadError.missingParameter(key)
        }
        return value
    }

    private func requireInt(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Int {
        guard let value = parameters[key]?.int64Value else {
            throw VectorReadError.missingParameter(key)
        }
        guard let result = Int(exactly: value) else {
            throw VectorReadError.invalidParameter(key)
        }
        return result
    }

    private func requireFloat32Vector(
        _ key: String,
        from parameters: [String: FieldValue]
    ) throws -> Vector {
        guard let vector = parameters[key]?.vectorValue,
              vector.elementType == .float32 else {
            throw VectorReadError.missingParameter(key)
        }
        return vector
    }

    private func authorizationValue(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else {
            return nil
        }
        guard let result = Int(exactly: value) else {
            throw VectorReadError.invalidParameter(parameter)
        }
        return result
    }
}
