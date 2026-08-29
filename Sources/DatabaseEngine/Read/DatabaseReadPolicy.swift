import DatabaseKit

/// One list-read decision collected during bounded query analysis.
package struct DatabaseListReadAuthorizationRequirement:
    Sendable,
    Hashable
{
    package let entityName: String
    package let limit: Int?
    package let offset: Int?
    package let orderBy: [String]?
}

/// Immutable evidence that one captured policy admitted a complete read plan.
///
/// Only `DatabaseReadPolicy` can construct this value. Retaining the exact
/// schema lease prevents a numerically reused generation or a different
/// runtime from being combined with the admitted field set.
package struct DatabaseReadAuthorization: Sendable {
    package let fields: DatabaseFieldReadAuthorizationPlan

    private let schemaLease: DatabaseSchemaLease
    private let authorization: AuthorizationContext
    private let listRequirements: Set<
        DatabaseListReadAuthorizationRequirement
    >

    fileprivate var schemaGeneration: UInt64 { schemaLease.generation }

    fileprivate init(
        fields: DatabaseFieldReadAuthorizationPlan,
        schemaLease: DatabaseSchemaLease,
        authorization: AuthorizationContext,
        listRequirements: Set<DatabaseListReadAuthorizationRequirement>
    ) {
        self.fields = fields
        self.schemaLease = schemaLease
        self.authorization = authorization
        self.listRequirements = listRequirements
    }

    fileprivate func matches(
        schemaLease: DatabaseSchemaLease,
        authorization: AuthorizationContext
    ) -> Bool {
        self.schemaLease === schemaLease
            && self.authorization == authorization
    }

    package func covers(
        listRequirements: some Sequence<
            DatabaseListReadAuthorizationRequirement
        >,
        fields: DatabaseFieldReadAuthorizationPlan
    ) -> Bool {
        fields.isCovered(by: self.fields.fieldsByEntity)
            && listRequirements.allSatisfy {
                self.listRequirements.contains($0)
            }
    }

    package func covers(
        listRequirement: DatabaseListReadAuthorizationRequirement
    ) -> Bool {
        listRequirements.contains(listRequirement)
    }

    package func covers(
        listRequirements: some Sequence<DatabaseListReadAuthorizationRequirement>
    ) -> Bool {
        listRequirements.allSatisfy {
            self.listRequirements.contains($0)
        }
    }

    package func admittedFieldNames(
        forEntityName entityName: String
    ) -> Set<String>? {
        fields.fieldsByEntity[entityName]
    }

}

/// Immutable read policy captured from one schema generation.
///
/// This value is the single owner of read authorization and runtime lookup
/// decisions shared by DatabaseContext and revocable read sessions. Callers
/// provide domain values; the policy never exposes its delegate or registry.
package struct DatabaseReadPolicy: Sendable {
    private let schemaLease: DatabaseSchemaLease
    private let authorization: AuthorizationContext

    package var schemaGeneration: UInt64 { schemaLease.generation }

    package var schema: Schema { schemaLease.schema }

    /// Layer map derived from the same generation as `schema`.
    ///
    /// The layer of a Directory position is a property of the whole schema that
    /// declares it, so binding a path declared by this policy's schema must use
    /// the map of that generation rather than the container's current one.
    package var directoryLayers: DirectoryLayerTagMap {
        schemaLease.directoryLayers
    }

    private var runtimeConfiguration: DatabaseRuntimeConfiguration {
        schemaLease.runtimeConfiguration
    }

    private var securityDelegate: (any DataStoreSecurityDelegate)? {
        schemaLease.securityDelegate
    }

    package init(
        schemaLease: DatabaseSchemaLease,
        authorization: AuthorizationContext
    ) {
        self.schemaLease = schemaLease
        self.authorization = authorization
    }

    package func authorizeCanonicalListAccess(
        entityName: String,
        selectQuery: SelectQuery
    ) throws {
        try authorizeList(
            Self.listRequirement(
                entityName: entityName,
                selectQuery: selectQuery
            )
        )
    }

    package func authorizeList(
        entityName: String,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        try authorizeList(
            DatabaseListReadAuthorizationRequirement(
                entityName: entityName,
                limit: limit,
                offset: offset,
                orderBy: orderBy
            )
        )
    }

    private func authorizeList(
        _ requirement: DatabaseListReadAuthorizationRequirement
    ) throws {
        try withAuthorization {
            try securityDelegate?.evaluateList(
                entity: requirement.entityName,
                limit: requirement.limit,
                offset: requirement.offset,
                orderBy: requirement.orderBy
            )
        }
    }

    package func authorizePolymorphicListAccess(
        group: PolymorphicGroup,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        try withAuthorization {
            for typeName in group.memberTypeNames {
                let runtime = try requiredRuntime(
                    named: typeName,
                    in: group
                )
                try securityDelegate?.evaluateList(
                    entity: runtime.entity.name,
                    limit: limit,
                    offset: offset,
                    orderBy: orderBy
                )
            }
        }
    }

    /// Authorizes a public polymorphic scan that returns complete models.
    /// Both list policy and every returned field are sealed before a storage
    /// transaction or snapshot is created.
    package func authorizePolymorphicModelScan(
        group: PolymorphicGroup,
        selectQuery: SelectQuery
    ) throws -> DatabaseReadAuthorization {
        var fieldsByEntity: [String: Set<String>] = [:]
        var listRequirements: [DatabaseListReadAuthorizationRequirement] = []
        fieldsByEntity.reserveCapacity(group.memberTypeNames.count)
        listRequirements.reserveCapacity(group.memberTypeNames.count)
        for typeName in group.memberTypeNames {
            let runtime = try requiredRuntime(named: typeName, in: group)
            fieldsByEntity[typeName] = Set(runtime.entity.allFields)
            listRequirements.append(
                try Self.listRequirement(
                    entityName: typeName,
                    selectQuery: selectQuery
                )
            )
        }
        return try authorizeRead(
            listRequirements: listRequirements,
            fields: DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: fieldsByEntity
            )
        )
    }

    /// Authorizes complete-model reads for already resolved polymorphic IDs.
    /// Exact-ID reads do not require list authority, but every field that can
    /// leave the public or index output boundary is sealed before storage.
    package func authorizePolymorphicModelRead(
        group: PolymorphicGroup
    ) throws -> DatabaseReadAuthorization {
        var fieldsByEntity: [String: Set<String>] = [:]
        fieldsByEntity.reserveCapacity(group.memberTypeNames.count)
        for typeName in group.memberTypeNames {
            let runtime = try requiredRuntime(named: typeName, in: group)
            fieldsByEntity[typeName] = Set(runtime.entity.allFields)
        }
        return try authorizeRead(
            listRequirements: [],
            fields: DatabaseFieldReadAuthorizationPlan(
                fieldsByEntity: fieldsByEntity
            )
        )
    }

    package func authorizeFields(
        _ plan: DatabaseFieldReadAuthorizationPlan
    ) throws {
        try withAuthorization {
            for entityName in plan.fieldsByEntity.keys.sorted() {
                guard let fields = plan.fieldsByEntity[entityName] else {
                    continue
                }
                try securityDelegate?.evaluateFieldRead(
                    entity: entityName,
                    fields: fields
                )
            }
        }
    }

    /// Performs one query-level authorization transition and returns evidence
    /// bound to this exact policy generation and principal.
    package func authorizeRead(
        listRequirements: some Sequence<
            DatabaseListReadAuthorizationRequirement
        >,
        fields: DatabaseFieldReadAuthorizationPlan
    ) throws -> DatabaseReadAuthorization {
        var uniqueListRequirements: [
            DatabaseListReadAuthorizationRequirement
        ] = []
        var seenListRequirements = Set<
            DatabaseListReadAuthorizationRequirement
        >()
        for requirement in listRequirements
            where seenListRequirements.insert(requirement).inserted {
            uniqueListRequirements.append(requirement)
        }
        try withAuthorization {
            for requirement in uniqueListRequirements {
                try securityDelegate?.evaluateList(
                    entity: requirement.entityName,
                    limit: requirement.limit,
                    offset: requirement.offset,
                    orderBy: requirement.orderBy
                )
            }
            for entityName in fields.fieldsByEntity.keys.sorted() {
                guard let fieldNames = fields.fieldsByEntity[entityName]
                else { continue }
                try securityDelegate?.evaluateFieldRead(
                    entity: entityName,
                    fields: fieldNames
                )
            }
        }
        return DatabaseReadAuthorization(
            fields: fields,
            schemaLease: schemaLease,
            authorization: authorization,
            listRequirements: seenListRequirements
        )
    }

    package func validate(
        _ admitted: DatabaseReadAuthorization
    ) throws {
        guard admitted.schemaGeneration == schemaGeneration else {
            throw DatabaseReadSessionError.schemaGenerationMismatch
        }
        guard admitted.matches(
            schemaLease: schemaLease,
            authorization: authorization
        ) else {
            throw DatabaseReadSessionError.authorizationMismatch
        }
    }

    package func authorizeGet(
        _ model: PersistedModel,
        fields: Set<String>? = nil
    ) throws {
        try withAuthorization {
            try securityDelegate?.evaluateGet(model, fields: fields)
        }
    }

    package var requiresPerEntityReadAuthorization: Bool {
        securityDelegate != nil
    }

    package func polymorphicTypeMap(
        for group: PolymorphicGroup
    ) throws -> [Int64: EntityRuntimeRegistration] {
        var result: [Int64: EntityRuntimeRegistration] = [:]
        for typeName in group.memberTypeNames {
            let runtime = try requiredRuntime(named: typeName, in: group)
            guard runtime.entity.polymorphicMembership?.identifier
                    == group.identifier else {
                throw PolymorphicRuntimeError.nonPolymorphableMember(
                    groupIdentifier: group.identifier,
                    memberTypeName: typeName
                )
            }
            result[PolymorphicTypeCode.value(for: runtime.entity.name)] = runtime
        }
        return result
    }

    package func entityRuntime(
        named entityName: String
    ) -> EntityRuntimeRegistration? {
        runtimeConfiguration.entityRuntimes.registration(named: entityName)
    }

    package func indexConfigurations(
        named indexName: String
    ) -> [any IndexRuntimeConfiguration] {
        runtimeConfiguration.indexConfigurations(named: indexName)
    }

    package var indexConfigurations: [any IndexRuntimeConfiguration] {
        runtimeConfiguration.indexConfigurations
    }

    package func polymorphicIndexExecutor(
        for indexType: IndexType
    ) -> (any PolymorphicIndexReadExecutor)? {
        runtimeConfiguration.readExecutors.polymorphicIndexExecutor(
            for: indexType
        )
    }

    package func additionalPolymorphicIndexRequiredFieldNames(
        for indexScan: IndexScanSource
    ) throws -> Set<String>? {
        try runtimeConfiguration.readExecutors
            .additionalRequiredFieldNames(for: indexScan)
    }

    package var graphTableSourceExecutor: (any GraphTableSourceExecutor)? {
        runtimeConfiguration.logicalSourceExecutors.graphTableExecutor
    }

    package var sparqlSourceExecutor: (any SPARQLSourceExecutor)? {
        runtimeConfiguration.logicalSourceExecutors.sparqlExecutor
    }

    package func fusionIndexExecutor(
        for indexType: IndexType
    ) -> (any FusionIndexReadExecutor)? {
        runtimeConfiguration.fusionReadExecutors.indexExecutor(for: indexType)
    }

    package func fusionConnectedExecutor(
        for indexType: IndexType
    ) -> (any FusionConnectedReadExecutor)? {
        runtimeConfiguration.fusionReadExecutors.connectedExecutor(
            for: indexType
        )
    }

    package func fieldSchema(
        entityName: String,
        fieldName: String
    ) -> FieldSchema? {
        entityRuntime(named: entityName)?.entity.fieldMapByName[fieldName]
    }

    private func requiredRuntime(
        named typeName: String,
        in group: PolymorphicGroup
    ) throws -> EntityRuntimeRegistration {
        guard let runtime = entityRuntime(named: typeName) else {
            throw DatabaseRuntimeConfigurationError
                .missingCompiledPolymorphicMemberType(
                    groupIdentifier: group.identifier,
                    memberTypeName: typeName
                )
        }
        return runtime
    }

    package func withAuthorization<Result>(
        _ operation: () throws -> Result
    ) rethrows -> Result {
        try RequestAuthorization.$context.withValue(
            authorization,
            operation: operation
        )
    }

    package func withAuthorization<Result: Sendable>(
        _ operation: nonisolated(nonsending) () async throws -> Result
    ) async rethrows -> Result {
        try await RequestAuthorization.$context.withValue(
            authorization,
            operation: operation
        )
    }

    private static func windowValue(
        _ value: UInt64?,
        parameter: String
    ) throws -> Int? {
        guard let value else { return nil }
        guard let result = Int(exactly: value) else {
            throw CanonicalReadError.paginationValueExceedsRuntimeRange(
                name: parameter,
                value: value
            )
        }
        return result
    }

    package static func listRequirement(
        entityName: String,
        selectQuery: SelectQuery
    ) throws -> DatabaseListReadAuthorizationRequirement {
        DatabaseListReadAuthorizationRequirement(
            entityName: entityName,
            limit: try windowValue(
                selectQuery.limit,
                parameter: "limit"
            ),
            offset: try windowValue(
                selectQuery.offset,
                parameter: "offset"
            ),
            orderBy: try selectQuery.requiredOrderByColumnNames()
        )
    }
}

extension DatabaseContext {
    package func readPolicy() throws -> DatabaseReadPolicy {
        if let binding = ActiveDatabaseTransactionContext.binding {
            try binding.validate(for: self)
            return DatabaseReadPolicy(
                schemaLease: binding.schemaLease,
                authorization: authorization
            )
        }
        return DatabaseReadPolicy(
            schemaLease: container.acquireActiveSchemaLease(),
            authorization: authorization
        )
    }
}

extension DBContainer {
    /// Captures the schema and authorization already admitted by an enclosing
    /// context operation. A raw ambient authorization is consulted only when
    /// the container is used without an admitted context binding.
    package func readPolicyForCurrentOperation() throws -> DatabaseReadPolicy {
        if let binding = ActiveDatabaseTransactionContext.binding {
            guard binding.container === self else {
                throw DatabaseTransactionError.invalidOperationContext
            }
            return DatabaseReadPolicy(
                schemaLease: binding.schemaLease,
                authorization: binding.authorization
            )
        }
        return DatabaseReadPolicy(
            schemaLease: acquireActiveSchemaLease(),
            authorization: RequestAuthorization.context
        )
    }
}
