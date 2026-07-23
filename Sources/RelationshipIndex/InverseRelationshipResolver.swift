import Core
import DatabaseEngine
import Relationship
import StorageKit

/// Performs bounded inverse lookups through the canonical relationship catalog.
public struct InverseRelationshipResolver: Sendable {
    private let container: DBContainer

    public init(container: DBContainer) {
        self.container = container
    }

    public func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: DatabaseReference<Target>,
        from ownerType: Owner.Type,
        via keyPath: KeyPath<Owner, DatabaseReference<Target>?>,
        limit: Int,
        continuation: Bytes? = nil
    ) async throws -> RelationshipPage<Owner> {
        try await referencedBy(
            target,
            from: ownerType,
            fieldName: Owner.fieldName(for: keyPath),
            cardinality: .optionalToOne,
            limit: limit,
            continuation: continuation
        )
    }

    public func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: DatabaseReference<Target>,
        from ownerType: Owner.Type,
        via keyPath: KeyPath<Owner, DatabaseReference<Target>>,
        limit: Int,
        continuation: Bytes? = nil
    ) async throws -> RelationshipPage<Owner> {
        try await referencedBy(
            target,
            from: ownerType,
            fieldName: Owner.fieldName(for: keyPath),
            cardinality: .requiredToOne,
            limit: limit,
            continuation: continuation
        )
    }

    public func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: DatabaseReference<Target>,
        from ownerType: Owner.Type,
        via keyPath: KeyPath<Owner, [DatabaseReference<Target>]>,
        limit: Int,
        continuation: Bytes? = nil
    ) async throws -> RelationshipPage<Owner> {
        try await referencedBy(
            target,
            from: ownerType,
            fieldName: Owner.fieldName(for: keyPath),
            cardinality: .toMany,
            limit: limit,
            continuation: continuation
        )
    }

    private func referencedBy<Target: Persistable, Owner: Persistable>(
        _ target: DatabaseReference<Target>,
        from ownerType: Owner.Type,
        fieldName: String,
        cardinality: RelationshipCardinality,
        limit: Int,
        continuation: Bytes?
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

        let context = container.newContext()
        return try await context.withTransaction { transaction in
            let page = try await RelationshipReferenceCatalog.referrerPage(
                of: target.identity,
                descriptor: descriptor,
                continuation: continuation,
                limit: limit,
                transaction: transaction.storageAccess
            )
            var records: [Owner] = []
            records.reserveCapacity(page.identities.count)
            for identity in page.identities {
                guard let model = try await transaction.fetchPersistedModel(
                    identifiedBy: identity
                ) else {
                    throw RelationshipError.catalogOwnerMissing(identity)
                }
                guard let owner = model as? Owner else {
                    throw RelationshipReferenceError.loadedTypeMismatch(
                        expected: Owner.persistableType,
                        actual: type(of: model).persistableType
                    )
                }
                records.append(owner)
            }
            return RelationshipPage(
                records: records,
                continuation: page.continuation
            )
        }
    }
}

extension DatabaseContext {
    public func inverseRelationshipResolver() -> InverseRelationshipResolver {
        InverseRelationshipResolver(container: container)
    }
}
