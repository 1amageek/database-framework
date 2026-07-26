import DatabaseKit
import DatabaseTypes

public enum DatabaseRuntimeConfigurationError: Error, Sendable, Equatable {
    case duplicateIndexMaintainerProvider(String)
    case duplicateIndexReadExecutor(String)
    case duplicatePolymorphicIndexReadExecutor(String)
    case duplicateFusionReadExecutor(String)
    case duplicatePersistableMutationMaintainer(identifier: String)
    case duplicatePersistableType(entityName: String)
    case duplicateAuthorizationPolicy(entityName: String)
    case missingCompiledEntityType(entityName: String)
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
    case missingPersistableMutationMaintainer(
        entityName: String,
        descriptorName: String,
        identifier: String
    )
    case invalidPersistableMutationMaintainerSchema(
        identifier: String,
        reason: String
    )
}
