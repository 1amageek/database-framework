import DatabaseKit
import DatabaseEngine
import DatabaseTypes

/// Executes a model query and typed relationship joins in one transaction.
public struct RelationshipQueryExecutor<Model: Persistable>: Sendable {
    private let context: DatabaseContext
    package var query: Query<Model>
    private var joins: [RelationshipJoin<Model>]

    public init(
        context: DatabaseContext,
        query: Query<Model>
    ) {
        self.context = context
        self.query = query
        self.joins = []
    }

    public func `where`(
        _ predicate: DatabaseEngine.Predicate<Model>
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.where(predicate)
        return copy
    }

    public func orderBy<Value: Comparable & Sendable>(
        _ field: Field<Model, Value>
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(field)
        return copy
    }

    public func orderBy<Value: Comparable & Sendable>(
        _ field: Field<Model, Value>,
        _ order: DatabaseEngine.SortOrder
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(field, order)
        return copy
    }

    public func limit(_ count: Int) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.limit(count)
        return copy
    }

    public func offset(_ count: Int) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.offset(count)
        return copy
    }

    public func cachePolicy(
        _ policy: CachePolicy
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.cachePolicy(policy)
        return copy
    }

    public func partition<Value: Sendable & Equatable & FieldValueRepresentable>(
        _ field: Field<Model, Value>,
        equals value: Value
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.partition(field, equals: value)
        return copy
    }

    public func joining<Related: Persistable>(
        _ field: Field<Model, PersistableReference<Related>?>
    ) throws -> RelationshipQueryExecutor<Model> {
        try appendingJoin(
            fieldName: field.name,
            relatedType: Related.self,
            cardinality: .optionalToOne
        ) { models in
            try castToOne(models, as: Related.self)
                .map(LoadedRelationship.toOne) ?? .absentToOne
        }
    }

    public func joining<Related: Persistable>(
        _ field: Field<Model, PersistableReference<Related>>
    ) throws -> RelationshipQueryExecutor<Model> {
        try appendingJoin(
            fieldName: field.name,
            relatedType: Related.self,
            cardinality: .requiredToOne
        ) { models in
            try castToOne(models, as: Related.self)
                .map(LoadedRelationship.toOne) ?? .absentToOne
        }
    }

    public func joining<Related: Persistable>(
        _ field: Field<Model, [PersistableReference<Related>]>
    ) throws -> RelationshipQueryExecutor<Model> {
        try appendingJoin(
            fieldName: field.name,
            relatedType: Related.self,
            cardinality: .toMany
        ) { models in
            for model in models {
                guard model.entity == Related.persistableType else {
                    throw RelationshipReferenceError.loadedTypeMismatch(
                        expected: Related.persistableType,
                        actual: model.entity
                    )
                }
            }
            return .toMany(models)
        }
    }

    public func execute() async throws -> [RelationshipSnapshot<Model>] {
        let joins = self.joins
        let container = context.container
        return try await context.withFetchedModelsInTransaction(query) {
            models,
            transaction in
            guard !joins.isEmpty else {
                return models.map { RelationshipSnapshot(item: $0) }
            }

            let resolver = RelationshipReferenceResolver(schema: container.schema)
            var referencesByModel: [[[EntityReference]]] = []
            referencesByModel.reserveCapacity(models.count)
            var orderedIdentities: [EntityReference] = []
            var seenIdentities = Set<EntityReference>()

            for model in models {
                let persistedModel = try PersistedModel(model)
                var modelReferences: [[EntityReference]] = []
                modelReferences.reserveCapacity(joins.count)
                for join in joins {
                    let references = try resolver.orderedReferences(
                        from: persistedModel,
                        descriptor: join.descriptor
                    )
                    modelReferences.append(references)
                    for identity in references where seenIdentities.insert(identity).inserted {
                        orderedIdentities.append(identity)
                    }
                }
                referencesByModel.append(modelReferences)
            }

            var loadedByIdentity: [EntityReference: PersistedModel] = [:]
            loadedByIdentity.reserveCapacity(orderedIdentities.count)
            for identity in orderedIdentities {
                if let loaded = try await transaction.fetchPersistedModel(
                    identifiedBy: identity
                ) {
                    loadedByIdentity[identity] = loaded
                }
            }

            var snapshots: [RelationshipSnapshot<Model>] = []
            snapshots.reserveCapacity(models.count)
            for (modelOffset, model) in models.enumerated() {
                var loadedRelationships: [String: LoadedRelationship] = [:]
                for (joinOffset, join) in joins.enumerated() {
                    let related = referencesByModel[modelOffset][joinOffset].compactMap {
                        loadedByIdentity[$0]
                    }
                    loadedRelationships[join.descriptor.propertyName] =
                        try join.assemble(related)
                }
                snapshots.append(
                    RelationshipSnapshot(
                        item: model,
                        loadedRelationships: loadedRelationships
                    )
                )
            }
            return snapshots
        }
    }

    public func count() async throws -> Int {
        try await QueryExecutor(context: context, query: query).count()
    }

    public func first() async throws -> RelationshipSnapshot<Model>? {
        try await limit(1).execute().first
    }

    private func appendingJoin<Related: Persistable>(
        fieldName: String,
        relatedType: Related.Type,
        cardinality: RelationshipCardinality,
        assemble: @escaping @Sendable ([PersistedModel]) throws -> LoadedRelationship
    ) throws -> RelationshipQueryExecutor<Model> {
        let matching = Model.relationshipDescriptors.filter {
            $0.propertyName == fieldName
        }
        guard let descriptor = matching.first, matching.count == 1 else {
            throw RelationshipReferenceError.missingDescriptor(
                owner: Model.persistableType,
                field: fieldName
            )
        }
        guard descriptor.ownerTypeName == Model.persistableType,
              descriptor.relatedTypeName == Related.persistableType,
              descriptor.cardinality == cardinality else {
            throw RelationshipReferenceError.descriptorMismatch(
                owner: Model.persistableType,
                field: fieldName
            )
        }
        var copy = self
        copy.joins.append(
            RelationshipJoin(
                descriptor: descriptor,
                assemble: assemble
            )
        )
        return copy
    }
}

private struct RelationshipJoin<Model: Persistable>: Sendable {
    let descriptor: RelationshipDescriptor
    let assemble: @Sendable ([PersistedModel]) throws -> LoadedRelationship
}

private func castToOne<Related: Persistable>(
    _ models: [PersistedModel],
    as relatedType: Related.Type
) throws -> PersistedModel? {
    guard let first = models.first else {
        return nil
    }
    guard models.count == 1, first.entity == Related.persistableType else {
        throw RelationshipReferenceError.loadedTypeMismatch(
            expected: Related.persistableType,
            actual: first.entity
        )
    }
    return first
}

extension QueryExecutor {
    public func joining<Related: Persistable>(
        _ field: Field<T, PersistableReference<Related>?>
    ) throws -> RelationshipQueryExecutor<T> {
        try RelationshipQueryExecutor(context: context, query: query)
            .joining(field)
    }

    public func joining<Related: Persistable>(
        _ field: Field<T, PersistableReference<Related>>
    ) throws -> RelationshipQueryExecutor<T> {
        try RelationshipQueryExecutor(context: context, query: query)
            .joining(field)
    }

    public func joining<Related: Persistable>(
        _ field: Field<T, [PersistableReference<Related>]>
    ) throws -> RelationshipQueryExecutor<T> {
        try RelationshipQueryExecutor(context: context, query: query)
            .joining(field)
    }
}
