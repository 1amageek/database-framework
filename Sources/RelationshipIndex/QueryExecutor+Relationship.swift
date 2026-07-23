import Core
import DatabaseEngine
import DatabaseValue
import Relationship

/// Executes a model query and typed relationship joins in one transaction.
public struct RelationshipQueryExecutor<Model: Persistable>: Sendable {
    private let context: FDBContext
    package var query: Query<Model>
    private var joins: [RelationshipJoin<Model>]

    public init(
        context: FDBContext,
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
        _ keyPath: KeyPath<Model, Value>
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(keyPath)
        return copy
    }

    public func orderBy<Value: Comparable & Sendable>(
        _ keyPath: KeyPath<Model, Value>,
        _ order: DatabaseEngine.SortOrder
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.orderBy(keyPath, order)
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

    public func partition<Value: Sendable & Equatable & FieldValueConvertible>(
        _ keyPath: KeyPath<Model, Value>,
        equals value: Value
    ) -> RelationshipQueryExecutor<Model> {
        var copy = self
        copy.query = query.partition(keyPath, equals: value)
        return copy
    }

    public func joining<Related: Persistable>(
        _ keyPath: KeyPath<Model, DatabaseReference<Related>?>
    ) throws -> RelationshipQueryExecutor<Model> {
        try appendingJoin(
            fieldName: Model.fieldName(for: keyPath),
            relatedType: Related.self,
            cardinality: .optionalToOne
        ) { models in
            try castToOne(models, as: Related.self)
        }
    }

    public func joining<Related: Persistable>(
        _ keyPath: KeyPath<Model, DatabaseReference<Related>>
    ) throws -> RelationshipQueryExecutor<Model> {
        try appendingJoin(
            fieldName: Model.fieldName(for: keyPath),
            relatedType: Related.self,
            cardinality: .requiredToOne
        ) { models in
            try castToOne(models, as: Related.self)
        }
    }

    public func joining<Related: Persistable>(
        _ keyPath: KeyPath<Model, [DatabaseReference<Related>]>
    ) throws -> RelationshipQueryExecutor<Model> {
        try appendingJoin(
            fieldName: Model.fieldName(for: keyPath),
            relatedType: Related.self,
            cardinality: .toMany
        ) { models in
            var typed: [Related] = []
            typed.reserveCapacity(models.count)
            for model in models {
                guard let related = model as? Related else {
                    throw RelationshipReferenceError.loadedTypeMismatch(
                        expected: Related.persistableType,
                        actual: type(of: model).persistableType
                    )
                }
                typed.append(related)
            }
            return typed
        }
    }

    public func execute() async throws -> [Snapshot<Model>] {
        let joins = self.joins
        let container = context.container
        return try await context.withFetchedModelsInTransaction(query) {
            models,
            transaction in
            guard !joins.isEmpty else {
                return models.map { Snapshot(item: $0) }
            }

            let resolver = RelationshipReferenceResolver(schema: container.schema)
            var referencesByModel: [[[RecordIdentity]]] = []
            referencesByModel.reserveCapacity(models.count)
            var orderedIdentities: [RecordIdentity] = []
            var seenIdentities = Set<RecordIdentity>()

            for model in models {
                var modelReferences: [[RecordIdentity]] = []
                modelReferences.reserveCapacity(joins.count)
                for join in joins {
                    let references = try resolver.orderedReferences(
                        from: model,
                        descriptor: join.descriptor
                    )
                    modelReferences.append(references)
                    for identity in references where seenIdentities.insert(identity).inserted {
                        orderedIdentities.append(identity)
                    }
                }
                referencesByModel.append(modelReferences)
            }

            var loadedByIdentity: [RecordIdentity: any Persistable] = [:]
            loadedByIdentity.reserveCapacity(orderedIdentities.count)
            for identity in orderedIdentities {
                if let loaded = try await transaction.fetchPersistedModel(
                    identifiedBy: identity
                ) {
                    loadedByIdentity[identity] = loaded
                }
            }

            var snapshots: [Snapshot<Model>] = []
            snapshots.reserveCapacity(models.count)
            for (modelOffset, model) in models.enumerated() {
                var relations: [String: any Sendable] = [:]
                for (joinOffset, join) in joins.enumerated() {
                    let related = referencesByModel[modelOffset][joinOffset].compactMap {
                        loadedByIdentity[$0]
                    }
                    if let value = try join.assemble(related) {
                        relations[join.descriptor.propertyName] = value
                    }
                }
                snapshots.append(Snapshot(item: model, relations: relations))
            }
            return snapshots
        }
    }

    public func count() async throws -> Int {
        try await QueryExecutor(context: context, query: query).count()
    }

    public func first() async throws -> Snapshot<Model>? {
        try await limit(1).execute().first
    }

    private func appendingJoin<Related: Persistable>(
        fieldName: String,
        relatedType: Related.Type,
        cardinality: RelationshipCardinality,
        assemble: @escaping @Sendable ([any Persistable]) throws -> (any Sendable)?
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
    let assemble: @Sendable ([any Persistable]) throws -> (any Sendable)?
}

private func castToOne<Related: Persistable>(
    _ models: [any Persistable],
    as relatedType: Related.Type
) throws -> (any Sendable)? {
    guard let first = models.first else {
        return nil
    }
    guard models.count == 1, let related = first as? Related else {
        throw RelationshipReferenceError.loadedTypeMismatch(
            expected: Related.persistableType,
            actual: type(of: first).persistableType
        )
    }
    return related
}

extension QueryExecutor {
    public func joining<Related: Persistable>(
        _ keyPath: KeyPath<T, DatabaseReference<Related>?>
    ) throws -> RelationshipQueryExecutor<T> {
        try RelationshipQueryExecutor(context: context, query: query)
            .joining(keyPath)
    }

    public func joining<Related: Persistable>(
        _ keyPath: KeyPath<T, DatabaseReference<Related>>
    ) throws -> RelationshipQueryExecutor<T> {
        try RelationshipQueryExecutor(context: context, query: query)
            .joining(keyPath)
    }

    public func joining<Related: Persistable>(
        _ keyPath: KeyPath<T, [DatabaseReference<Related>]>
    ) throws -> RelationshipQueryExecutor<T> {
        try RelationshipQueryExecutor(context: context, query: query)
            .joining(keyPath)
    }
}
