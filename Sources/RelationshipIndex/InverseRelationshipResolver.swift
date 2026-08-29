import DatabaseKit
import DatabaseEngine
import StorageKit
import DatabaseTypes

/// Performs bounded inverse lookups through the canonical relationship catalog.
public struct InverseRelationshipResolver: Sendable {
    private let context: DatabaseContext

    public init(context: DatabaseContext) {
        self.context = context
    }

    public func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: PersistableReference<Target>,
        from ownerType: Owner.Type,
        via field: Field<Owner, PersistableReference<Target>?>,
        limit: Int,
        continuation: ByteString? = nil
    ) async throws -> RelationshipPage<Owner> {
        try await referencedBy(
            target,
            from: ownerType,
            fieldName: field.name,
            cardinality: .optionalToOne,
            limit: limit,
            continuation: continuation
        )
    }

    public func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: PersistableReference<Target>,
        from ownerType: Owner.Type,
        via field: Field<Owner, PersistableReference<Target>>,
        limit: Int,
        continuation: ByteString? = nil
    ) async throws -> RelationshipPage<Owner> {
        try await referencedBy(
            target,
            from: ownerType,
            fieldName: field.name,
            cardinality: .requiredToOne,
            limit: limit,
            continuation: continuation
        )
    }

    public func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: PersistableReference<Target>,
        from ownerType: Owner.Type,
        via field: Field<Owner, [PersistableReference<Target>]>,
        limit: Int,
        continuation: ByteString? = nil
    ) async throws -> RelationshipPage<Owner> {
        try await referencedBy(
            target,
            from: ownerType,
            fieldName: field.name,
            cardinality: .toMany,
            limit: limit,
            continuation: continuation
        )
    }

    private func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: PersistableReference<Target>,
        from ownerType: Owner.Type,
        fieldName: String,
        cardinality: RelationshipCardinality,
        limit: Int,
        continuation: ByteString?
    ) async throws -> RelationshipPage<Owner> {
        let matching = Owner.relationshipDescriptors.filter {
            $0.propertyName == fieldName
        }
        guard let descriptor = matching.first, matching.count == 1 else {
            throw RelationshipReferenceError.missingDescriptor(
                owner: Owner.persistableType,
                field: fieldName
            )
        }
        guard descriptor.ownerTypeName == Owner.persistableType,
              descriptor.relatedTypeName == Target.persistableType,
              descriptor.cardinality == cardinality else {
            throw RelationshipReferenceError.descriptorMismatch(
                owner: Owner.persistableType,
                field: fieldName
            )
        }

        return try await context.withTransaction(
            requiredAccess: .read
        ) { transaction in
            let dataRoot = try context.operationDataRoot()
            let page = try await RelationshipReferenceCatalog.referrerPage(
                of: target.persistableIdentity,
                descriptor: descriptor,
                continuation: continuation,
                limit: limit,
                dataRoot: dataRoot,
                transaction: transaction.storageAccess
            )
            var entities: [Owner] = []
            entities.reserveCapacity(page.identities.count)
            for identity in page.identities {
                guard let model = try await transaction.fetchPersistedModel(
                    identifiedBy: identity
                ) else {
                    throw RelationshipError.catalogOwnerMissing(identity)
                }
                guard model.entity == Owner.persistableType else {
                    throw RelationshipReferenceError.loadedTypeMismatch(
                        expected: Owner.persistableType,
                        actual: model.entity
                    )
                }
                entities.append(try model.decode(as: Owner.self))
            }
            return RelationshipPage(
                entities: entities,
                continuation: page.continuation
            )
        }
    }
}

extension DatabaseContext {
    public func inverseRelationshipResolver() -> InverseRelationshipResolver {
        InverseRelationshipResolver(context: self)
    }
}
