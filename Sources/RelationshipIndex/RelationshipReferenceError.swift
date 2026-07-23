import DatabaseValue

public enum RelationshipReferenceError: Error, Sendable, Equatable {
    case unknownRelatedEntity(String)
    case relatedEntityHasNoCompiledType(String)
    case missingRelationshipField(entity: String, field: String)
    case invalidRelationshipValue(entity: String, field: String)
    case invalidReferenceEntity(expected: String, actual: String)
    case invalidTargetPartition(entity: String, reason: String)
    case invalidTargetIdentifier(
        entity: String,
        reason: PersistableIdentifierValidationError
    )
    case invalidOwnerIdentity(entity: String)
    case missingDescriptor(owner: String, field: String)
    case descriptorMismatch(owner: String, field: String)
    case loadedTypeMismatch(expected: String, actual: String)
    case targetEntityMissing(PersistableIdentity)
    case corruptedCatalogEntry
    case invalidScanLimit(Int)
    case nullifyRequiresOptionalField(entity: String, field: String)
    case entityDecodingFailed(entity: String, reason: String)
}
