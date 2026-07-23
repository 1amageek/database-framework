public enum RelationshipSchemaError: Error, Sendable, Equatable {
    case duplicateDescriptorName(String)
    case duplicateRelationshipField(owner: String, field: String)
    case ownerMismatch(expected: String, actual: String)
    case missingField(owner: String, field: String)
    case fieldNumberMismatch(owner: String, field: String)
    case fieldTypeMismatch(owner: String, field: String)
    case referenceTargetMismatch(
        owner: String,
        field: String,
        expected: String,
        actual: String?
    )
    case unknownTarget(String)
    case targetHasNoCompiledType(String)
    case cardinalityMismatch(owner: String, field: String)
    case nullifyRequiresNullableCardinality(owner: String, field: String)
}
