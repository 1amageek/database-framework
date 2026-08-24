import DatabaseKit
import DatabaseTypes

public enum DatabaseRuntimeConfigurationError: Error, Sendable, Equatable {
    case invalidExecutionIdentityIdentifier
    case invalidExecutionIdentityRevision
    case duplicateIndexMaintainerProvider(IndexType)
    case duplicateIndexReadExecutor(IndexType)
    case duplicatePolymorphicIndexReadExecutor(IndexType)
    case duplicateFusionReadExecutor(IndexType)
    case duplicatePersistableMutationMaintainer(identifier: String)
    case duplicatePersistableType(entityName: String)
    case duplicateAuthorizationPolicy(entityName: String)
    case missingCompiledEntityType(entityName: String)
    case entitySchemaMismatch(
        entityName: String,
        schemaEntity: Schema.Entity,
        runtimeEntity: Schema.Entity
    )
    case missingCompiledPolymorphicMemberType(
        groupIdentifier: String,
        memberTypeName: String
    )
    case missingIndexMaintainerProvider(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        indexType: IndexType
    )
    case missingIndexUniquenessSupport(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        indexType: IndexType
    )
    case missingIndexReadExecutor(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        indexType: IndexType
    )
    case missingPolymorphicIndexReadExecutor(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        indexType: IndexType
    )
    case missingGraphTableSourceExecutor
    case missingSPARQLSourceExecutor
    case unsupportedStorageCapability(
        source: DatabaseRuntimeIndexRequirementSource,
        indexName: String,
        indexType: IndexType,
        capability: DatabaseRuntimeStorageCapability
    )
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
