import DatabaseValue

public enum DatabaseRuntimeConfigurationError: Error, Sendable, Equatable {
    case duplicateIndexMaintainerProvider(String)
    case duplicateIndexReadExecutor(String)
    case duplicatePolymorphicIndexReadExecutor(String)
    case duplicateFusionReadExecutor(String)
    case duplicateRecordMutationMaintainer(identifier: String)
    case missingCompiledEntityType(entityName: String)
    case invalidRecordIdentifierType(
        entityName: String,
        reason: RecordIdentifierValidationError
    )
    case missingCompiledPolymorphicMemberType(
        groupIdentifier: String,
        memberTypeName: String
    )
    case missingIndexMaintainerProvider(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        kindIdentifier: String
    )
    case missingIndexReadExecutor(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        kindIdentifier: String
    )
    case missingPolymorphicIndexReadExecutor(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        kindIdentifier: String
    )
    case missingGraphTableSourceExecutor
    case missingSPARQLSourceExecutor
    case missingRecordMutationMaintainer(
        entityName: String,
        descriptorName: String,
        identifier: String
    )
    case invalidRecordMutationMaintainerSchema(
        identifier: String,
        reason: String
    )
}
