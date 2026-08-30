import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit

extension DBContainer {
    package func requireNoActiveSchemaTransition(
        transaction: any TransactionAccess
    ) async throws {
        let activeKey = metadataSubspace
            .subspace("schema")
            .subspace("applications")
            .pack(Tuple("active"))
        if try await transaction.getValue(
            for: activeKey,
            snapshot: false
        ) != nil {
            throw DatabaseSchemaPublicationError.transitionInProgress
        }
    }

    /// Atomically publishes the complete initial schema catalog for a compiled
    /// application. Existing complete state is validated, never overwritten.
    package func initializeSchemaCatalogIfNeeded(
        _ schema: Schema,
        fingerprint targetFingerprint: SchemaFingerprint,
        indexPhysicalFingerprint: ByteString,
        executionRuntimeFingerprint: ByteString,
        indexPhysicalLayouts: [String: IndexPhysicalLayout]
    ) async throws -> UInt64 {
        #if DATABASE_MULTI_BASE
        let schemaEngine = storageTopology.controlDomain.engine
        let schemaRoot = storageTopology.controlDomain.systemRoot
        let schemaTransactionExecutor = controlTransactionExecutor
        #else
        let schemaEngine = engine
        let schemaRoot = defaultTenant.systemRoot
        let schemaTransactionExecutor = transactionExecutor
        #endif
        let registry = SchemaRegistry(
            database: schemaEngine,
            root: schemaRoot,
            clock: monotonicClock
        )
        return try await schemaTransactionExecutor.withTransaction(
            configuration: .default,
            clock: monotonicClock
        ) { transaction in
            let entities = try await registry.loadAll(transaction: transaction)
            let version = try await Self.loadSchemaVersion(
                metadataSubspace: self.metadataSubspace,
                transaction: transaction
            )
            let fingerprint = try await Self.loadActiveSchemaFingerprint(
                metadataSubspace: self.metadataSubspace,
                transaction: transaction
            )
            let generation = try await Self.loadPersistedSchemaGeneration(
                metadataSubspace: self.metadataSubspace,
                transaction: transaction
            )
            let persistedExecutionRuntimeFingerprint =
                try await Self
                .loadActiveExecutionRuntimeFingerprint(
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )
            let persistedIndexPhysicalFingerprint =
                try await Self
                .loadActiveIndexPhysicalFingerprint(
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )
            let persistedLayouts =
                try await Self
                .loadActiveIndexLayoutFingerprints(
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )

            if entities.isEmpty, fingerprint == nil, generation == nil {
                guard version == nil || version == schema.version else {
                    throw DatabaseSchemaPublicationError.corruptedState(
                        "bootstrap schema version does not match the compiled schema"
                    )
                }
                guard
                    persistedExecutionRuntimeFingerprint == nil
                        || persistedExecutionRuntimeFingerprint
                            == executionRuntimeFingerprint
                else {
                    throw DatabaseSchemaRestorationError
                        .executionRuntimeFingerprintMismatch
                }
                guard
                    persistedIndexPhysicalFingerprint == nil
                        || persistedIndexPhysicalFingerprint
                            == indexPhysicalFingerprint
                else {
                    throw DatabaseSchemaRestorationError
                        .indexPhysicalFingerprintMismatch
                }
                guard
                    persistedLayouts.isEmpty
                        || persistedLayouts
                            == indexPhysicalLayouts.mapValues({
                                $0.fingerprint
                            })
                else {
                    throw DatabaseSchemaRestorationError
                        .indexPhysicalFingerprintMismatch
                }
                #if !DATABASE_MULTI_BASE
                // Bootstrapping asserts that no unversioned store already
                // holds rows. The Directory is opened in the publication
                // transaction: one no write ever created holds no row, so it
                // is skipped rather than created by the check that declares
                // it empty.
                for entity in schema.entities where !entity.hasDynamicDirectory {
                    guard
                        let subspace = try await self.openDirectory(
                            for: entity,
                            transaction: transaction
                        )
                    else { continue }
                    let range =
                        subspace
                        .subspace(SubspaceKey.items)
                        .subspace(entity.name)
                        .range()
                    let rows = try await TransactionRangeCollection.collect(
                        using: transaction,
                        from: .firstGreaterOrEqual(range.begin),
                        to: .firstGreaterOrEqual(range.end),
                        limit: 1,
                        snapshot: false,
                        streamingMode: .small
                    )
                    guard rows.isEmpty else {
                        throw
                            MigrationPlanError
                            .unversionedStoreContainsEntities(
                                entity: entity.name
                            )
                    }
                }
                #endif
                try await registry.persistInitialSchema(
                    schema,
                    transaction: transaction
                )
                try Self.setCurrentSchemaSnapshot(
                    schema,
                    indexPhysicalFingerprint: indexPhysicalFingerprint,
                    executionRuntimeFingerprint: executionRuntimeFingerprint,
                    indexPhysicalLayouts: indexPhysicalLayouts,
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )
                try transaction.setValue(
                    targetFingerprint.bytes,
                    for: Self.activeSchemaFingerprintKey(
                        metadataSubspace: self.metadataSubspace
                    )
                )
                try transaction.setValue(
                    Tuple(UInt64(0)).pack(),
                    for: Self.schemaGenerationKey(
                        metadataSubspace: self.metadataSubspace
                    )
                )
                return UInt64(0)
            }

            guard let version else {
                throw DatabaseSchemaRestorationError.missingVersion
            }
            guard let fingerprint else {
                throw DatabaseSchemaRestorationError.missingFingerprint
            }
            guard let generation else {
                throw DatabaseSchemaRestorationError.invalidGeneration
            }
            guard let persistedIndexPhysicalFingerprint else {
                throw DatabaseSchemaRestorationError
                    .missingIndexPhysicalFingerprint
            }
            guard let persistedExecutionRuntimeFingerprint else {
                throw DatabaseSchemaRestorationError
                    .missingExecutionRuntimeFingerprint
            }
            let persistedSchema = try Schema(
                entities: entities,
                version: version
            )
            let computedFingerprint = try SchemaManifest(
                schema: persistedSchema
            ).fingerprint()
            guard computedFingerprint == fingerprint else {
                throw DatabaseSchemaRestorationError.fingerprintMismatch
            }
            guard fingerprint == targetFingerprint else {
                throw DatabaseSchemaPublicationError.fingerprintConflict(
                    expected: targetFingerprint,
                    actual: fingerprint
                )
            }
            guard
                persistedIndexPhysicalFingerprint
                    == indexPhysicalFingerprint
            else {
                throw DatabaseSchemaRestorationError
                    .indexPhysicalFingerprintMismatch
            }
            guard
                persistedLayouts
                    == indexPhysicalLayouts.mapValues({ $0.fingerprint })
            else {
                throw DatabaseSchemaRestorationError
                    .indexPhysicalFingerprintMismatch
            }
            guard
                persistedExecutionRuntimeFingerprint
                    != executionRuntimeFingerprint
            else {
                return generation
            }
            let incremented = generation.addingReportingOverflow(1)
            guard !incremented.overflow else {
                throw DatabaseSchemaPublicationError.generationOverflow
            }
            try Self.setCurrentSchemaSnapshot(
                schema,
                indexPhysicalFingerprint: indexPhysicalFingerprint,
                executionRuntimeFingerprint: executionRuntimeFingerprint,
                indexPhysicalLayouts: indexPhysicalLayouts,
                metadataSubspace: self.metadataSubspace,
                transaction: transaction
            )
            try transaction.setValue(
                Tuple(incremented.partialValue).pack(),
                for: Self.schemaGenerationKey(
                    metadataSubspace: self.metadataSubspace
                )
            )
            return incremented.partialValue
        }
    }

    @_spi(DatabaseExecution)
    public func publishSchema(
        _ schema: Schema,
        fingerprint targetFingerprint: SchemaFingerprint,
        expectedFingerprint: SchemaFingerprint,
        idempotencyKey: String,
        authorization: AuthorizationContext,
        runtimeConfiguration targetRuntimeConfiguration: DatabaseRuntimeConfiguration
    ) async throws -> DatabaseSchemaPublicationResult {
        guard !idempotencyKey.isEmpty else {
            throw DatabaseSchemaPublicationError.invalidIdempotencyKey
        }

        let preparedGeneration = try prepareSchemaGeneration(
            schema,
            runtimeConfiguration: targetRuntimeConfiguration
        )
        let indexPhysicalLayouts = preparedGeneration.indexPhysicalLayouts

        let publishedLease = acquirePublishedSchemaLease()
        let leasedFingerprint = publishedLease.fingerprint.detached()
        let leasedGeneration = publishedLease.generation
        #if DATABASE_MULTI_BASE
        let schemaEngine = storageTopology.controlDomain.engine
        let schemaRoot = storageTopology.controlDomain.systemRoot
        let schemaTransactionExecutor = controlTransactionExecutor
        #else
        let schemaEngine = engine
        let schemaRoot = defaultTenant.systemRoot
        let schemaTransactionExecutor = transactionExecutor
        _ = authorization
        #endif
        let registry = SchemaRegistry(
            database: schemaEngine,
            root: schemaRoot,
            clock: monotonicClock
        )
        let outcome = try await schemaTransactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock,
            executionDeadline: nil
        ) { transaction in
            #if DATABASE_MULTI_BASE
            try await self.databaseGrantStore.require(
                .administer,
                authorization: authorization,
                transaction: transaction
            )
            #endif
            let databaseTransaction = DatabaseTransaction(
                storageAccess: transaction,
                container: self,
                readPolicy: DatabaseReadPolicy(
                    schemaLease: publishedLease,
                    authorization: authorization
                )
            )
            do {
                if let stored = try await Self.loadSchemaPublication(
                    idempotencyKey: idempotencyKey,
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                ) {
                    guard stored.fingerprint == targetFingerprint,
                        stored.indexPhysicalFingerprint
                            == preparedGeneration.indexPhysicalFingerprint,
                        stored.executionRuntimeFingerprint
                            == preparedGeneration.executionRuntimeFingerprint
                    else {
                        throw DatabaseSchemaPublicationError.idempotencyKeyReused(
                            idempotencyKey
                        )
                    }
                    let activeFingerprint =
                        try await Self
                        .loadActiveSchemaFingerprint(
                            metadataSubspace: self.metadataSubspace,
                            transaction: transaction
                        )
                    let activeGeneration =
                        try await Self
                        .loadPersistedSchemaGeneration(
                            metadataSubspace: self.metadataSubspace,
                            transaction: transaction
                        )
                    let activeExecutionRuntimeFingerprint =
                        try await Self
                        .loadActiveExecutionRuntimeFingerprint(
                            metadataSubspace: self.metadataSubspace,
                            transaction: transaction
                        )
                    let activeIndexPhysicalFingerprint =
                        try await Self
                        .loadActiveIndexPhysicalFingerprint(
                            metadataSubspace: self.metadataSubspace,
                            transaction: transaction
                        )
                    let activeIndexLayoutFingerprints =
                        try await Self
                        .loadActiveIndexLayoutFingerprints(
                            metadataSubspace: self.metadataSubspace,
                            transaction: transaction
                        )
                    guard activeFingerprint != nil, activeGeneration != nil,
                        activeIndexPhysicalFingerprint != nil,
                        activeExecutionRuntimeFingerprint != nil
                    else {
                        throw DatabaseSchemaPublicationError.corruptedState(
                            "idempotent publication exists without active schema state"
                        )
                    }
                    let matchesActivePublication =
                        activeFingerprint == stored.fingerprint
                        && activeGeneration == stored.generation
                        && activeIndexPhysicalFingerprint
                            == stored.indexPhysicalFingerprint
                        && activeExecutionRuntimeFingerprint
                            == stored.executionRuntimeFingerprint
                    if matchesActivePublication,
                        activeIndexLayoutFingerprints
                            != indexPhysicalLayouts.mapValues({
                                $0.fingerprint
                            })
                    {
                        throw DatabaseSchemaPublicationError.corruptedState(
                            "idempotent publication index layouts do not match the active generation"
                        )
                    }
                    let outcome = SchemaPublicationTransactionOutcome(
                        publication: stored,
                        shouldPublishGeneration: matchesActivePublication
                            && stored.executionRuntimeFingerprint
                                == preparedGeneration.executionRuntimeFingerprint
                    )
                    try await databaseTransaction.prepareForCommit()
                    return outcome
                }

                let actualFingerprint =
                    try await Self.loadActiveSchemaFingerprint(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    ) ?? leasedFingerprint
                guard actualFingerprint == expectedFingerprint else {
                    throw DatabaseSchemaPublicationError.fingerprintConflict(
                        expected: expectedFingerprint,
                        actual: actualFingerprint
                    )
                }

                let storedGeneration = try await Self.loadPersistedSchemaGeneration(
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )
                let currentGeneration = storedGeneration ?? leasedGeneration
                guard currentGeneration == leasedGeneration else {
                    throw DatabaseSchemaPublicationError.generationConflict(
                        expected: leasedGeneration,
                        actual: currentGeneration
                    )
                }
                let incremented = currentGeneration.addingReportingOverflow(1)
                guard !incremented.overflow else {
                    throw DatabaseSchemaPublicationError.generationOverflow
                }
                let nextGeneration = incremented.partialValue

                try await registry.persist(
                    schema,
                    mode: .strict,
                    transaction: transaction
                )
                try Self.setCurrentSchemaSnapshot(
                    schema,
                    indexPhysicalFingerprint:
                        preparedGeneration.indexPhysicalFingerprint,
                    executionRuntimeFingerprint:
                        preparedGeneration.executionRuntimeFingerprint,
                    indexPhysicalLayouts: indexPhysicalLayouts,
                    metadataSubspace: self.metadataSubspace,
                    transaction: transaction
                )
                try transaction.setValue(
                    targetFingerprint.bytes,
                    for: Self.activeSchemaFingerprintKey(
                        metadataSubspace: self.metadataSubspace
                    )
                )
                try transaction.setValue(
                    Tuple(nextGeneration).pack(),
                    for: Self.schemaGenerationKey(
                        metadataSubspace: self.metadataSubspace
                    )
                )

                let result = DatabaseSchemaPublicationResult(
                    previousFingerprint: actualFingerprint,
                    fingerprint: targetFingerprint,
                    schemaVersion: schema.version,
                    generation: nextGeneration,
                    indexPhysicalFingerprint:
                        preparedGeneration.indexPhysicalFingerprint,
                    executionRuntimeFingerprint:
                        preparedGeneration.executionRuntimeFingerprint
                )
                try transaction.setValue(
                    Self.encodeSchemaPublication(result),
                    for: Self.schemaPublicationKey(
                        idempotencyKey: idempotencyKey,
                        metadataSubspace: self.metadataSubspace
                    )
                )
                let outcome = SchemaPublicationTransactionOutcome(
                    publication: result,
                    shouldPublishGeneration: true
                )
                try await databaseTransaction.prepareForCommit()
                return outcome
            } catch {
                await databaseTransaction.invalidate()
                throw error
            }
        }

        if outcome.shouldPublishGeneration {
            try publishSchemaGeneration(
                schema,
                fingerprint: outcome.publication.fingerprint,
                indexPhysicalFingerprint:
                    outcome.publication.indexPhysicalFingerprint,
                executionRuntimeFingerprint:
                    outcome.publication.executionRuntimeFingerprint,
                runtimeConfiguration: targetRuntimeConfiguration,
                indexPhysicalLayouts: indexPhysicalLayouts,
                generation: outcome.publication.generation
            )
        }
        return outcome.publication
    }

    @_spi(DatabaseExecution)
    public func prepareSchemaGeneration(
        _ schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) throws -> DatabasePreparedSchemaGeneration {
        try runtimeConfiguration.validate(schema: schema)
        try runtimeConfiguration.validateStorageRequirements(
            schema: schema,
            transactionCapabilities: transactionCapabilities
        )
        let layouts = try IndexRuntimeConfigurationValidator.validate(
            schema: schema,
            runtimeConfiguration: runtimeConfiguration
        )
        return try DatabasePreparedSchemaGeneration(
            schemaFingerprint: SchemaManifest(schema: schema).fingerprint(),
            runtimeConfiguration: runtimeConfiguration,
            securityConfiguration: securityConfiguration,
            indexPhysicalLayouts: layouts
        )
    }

    /// Resolves one migration stage against the installed runtime providers.
    /// Runtime policies for later stages are ignored until their index names
    /// become part of the candidate schema.
    package func prepareMigrationSchemaGeneration(
        _ schema: Schema
    ) throws -> DatabasePreparedSchemaGeneration {
        let layouts = try IndexRuntimeConfigurationValidator.validate(
            schema: schema,
            runtimeConfiguration: runtimeConfiguration,
            allowingConfigurationsOutsideSchema: true
        )
        return try DatabasePreparedSchemaGeneration(
            schemaFingerprint: SchemaManifest(schema: schema).fingerprint(),
            runtimeConfiguration: runtimeConfiguration,
            securityConfiguration: securityConfiguration,
            indexPhysicalLayouts: layouts
        )
    }

    @_spi(DatabaseExecution)
    public func initializeSchemaIndexStates(
        for target: Schema,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        transaction: any TransactionAccess
    ) async throws -> Bool {
        var requiresPersistentBuild = false
        // The schema being applied is not published yet, so its declarations
        // are typed by a map derived from it rather than by the container's
        // current one.
        let targetLayers = try DirectoryLayerTagMap(
            entities: target.entities,
            polymorphicGroups: target.polymorphicGroups
        )
        for targetEntity in target.entities {
            let indexes = targetEntity.indexDescriptors
            guard !indexes.isEmpty else { continue }

            if targetEntity.hasDynamicDirectory {
                guard try await partitionCatalogContainsEntries(
                    entity: targetEntity,
                    directoryLayers: targetLayers,
                    transaction: transaction
                ) else {
                    continue
                }
                for descriptor in indexes {
                    try markSchemaIndexBuildPending(
                        entity: targetEntity.name,
                        index: descriptor.name,
                        schema: target,
                        indexPhysicalLayouts: indexPhysicalLayouts,
                        transaction: transaction
                    )
                }
                requiresPersistentBuild = true
                continue
            }

            let subspace = try await resolveDirectory(
                for: targetEntity,
                declaredIn: target,
                directoryLayers: targetLayers,
                transaction: transaction
            )
            let lifecycleStore = IndexLifecycleStore(
                container: self,
                subspace: subspace,
                schema: target,
                indexPhysicalLayouts: indexPhysicalLayouts
            )
            let entityRange = subspace
                .subspace(SubspaceKey.items)
                .subspace(targetEntity.name)
                .range()
            for descriptor in indexes {
                let pending = try await isSchemaIndexBuildPending(
                    scope: Self.entityIndexBuildScope(targetEntity),
                    identity: try DatabaseIndexStorageIdentity.resolve(
                        named: descriptor.name,
                        in: target,
                        physicalLayout: try physicalLayout(
                            for: descriptor.name,
                            in: indexPhysicalLayouts
                        )
                    ),
                    transaction: transaction
                )
                let needsBuild = try await lifecycleStore.prepareSchemaBuild(
                    descriptor.name,
                    entityRange: entityRange,
                    resumesPendingBuild: pending,
                    transaction: transaction
                )
                guard needsBuild else { continue }
                try markSchemaIndexBuildPending(
                    entity: targetEntity.name,
                    index: descriptor.name,
                    schema: target,
                    indexPhysicalLayouts: indexPhysicalLayouts,
                    transaction: transaction
                )
                requiresPersistentBuild = true
            }
        }

        for group in target.polymorphicGroups where !group.indexes.isEmpty {
            let groupPath = try group.resolvedDirectoryPath()
            let subspace = try await resolveDataDirectory(
                relativePath: groupPath,
                layers: targetLayers.layers(forPath: groupPath),
                transaction: transaction
            )
            let lifecycleStore = IndexLifecycleStore(
                container: self,
                subspace: subspace,
                schema: target,
                indexPhysicalLayouts: indexPhysicalLayouts
            )
            let entityRange = subspace.subspace(SubspaceKey.items).range()
            for declaration in group.indexes {
                let pending = try await isSchemaIndexBuildPending(
                    scope: try Self.polymorphicIndexBuildScope(group),
                    identity: try DatabaseIndexStorageIdentity.resolve(
                        named: declaration.name,
                        in: target,
                        physicalLayout: try physicalLayout(
                            for: declaration.name,
                            in: indexPhysicalLayouts
                        )
                    ),
                    transaction: transaction
                )
                let needsBuild = try await lifecycleStore.prepareSchemaBuild(
                    declaration.name,
                    entityRange: entityRange,
                    resumesPendingBuild: pending,
                    transaction: transaction
                )
                guard needsBuild else { continue }
                try markSchemaPolymorphicIndexBuildPending(
                    group: group.identifier,
                    index: declaration.name,
                    schema: target,
                    indexPhysicalLayouts: indexPhysicalLayouts,
                    transaction: transaction
                )
                requiresPersistentBuild = true
            }
        }
        return requiresPersistentBuild
    }

    package func pendingSchemaIndexBuilds(
        entity: String,
        indexes: [String],
        transaction: any TransactionAccess
    ) async throws -> Set<String> {
        let schemaMetadataSubspace = try dataRootSchemaMetadataSubspace()
        guard let entityDeclaration = schema.entity(named: entity) else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "schema index build entity is not declared"
            )
        }
        let scope = Self.entityIndexBuildScope(entityDeclaration)
        var pending = Set<String>()
        pending.reserveCapacity(indexes.count)
        for index in indexes {
            let identity = try DatabaseIndexStorageIdentity.resolve(
                named: index,
                in: schema,
                physicalLayout: try physicalLayout(
                    for: index,
                    in: indexPhysicalLayouts
                )
            )
            if try await transaction.getValue(
                for: Self.schemaIndexBuildPendingKey(
                    scope: scope,
                    identity: identity,
                    metadataSubspace: schemaMetadataSubspace
                ),
                snapshot: false
            ) != nil {
                pending.insert(index)
            }
        }
        return pending
    }

    package func pendingSchemaPolymorphicIndexBuilds(
        group: String,
        indexes: [String],
        transaction: any TransactionAccess
    ) async throws -> Set<String> {
        let schemaMetadataSubspace = try dataRootSchemaMetadataSubspace()
        guard
            let groupDeclaration = schema.polymorphicGroup(
                identifier: group
            )
        else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "schema index build polymorphic group is not declared"
            )
        }
        let scope = try Self.polymorphicIndexBuildScope(groupDeclaration)
        var pending = Set<String>()
        pending.reserveCapacity(indexes.count)
        for index in indexes {
            let identity = try DatabaseIndexStorageIdentity.resolve(
                named: index,
                in: schema,
                physicalLayout: try physicalLayout(
                    for: index,
                    in: indexPhysicalLayouts
                )
            )
            if try await transaction.getValue(
                for: Self.schemaIndexBuildPendingKey(
                    scope: scope,
                    identity: identity,
                    metadataSubspace: schemaMetadataSubspace
                ),
                snapshot: false
            ) != nil {
                pending.insert(index)
            }
        }
        return pending
    }

    @_spi(DatabaseExecution)
    public func completeSchemaIndexBuild(
        _ target: DatabaseIndexTransitionPlan.Target,
        transaction: any TransactionAccess
    ) throws {
        let lease = acquireActiveSchemaLease()
        let activeTarget: DatabaseIndexTransitionPlan.Target
        switch target.scope {
        case .entity(let entity, _):
            guard let declaration = lease.schema.entity(named: entity) else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "completed schema index build entity is not declared"
                )
            }
            activeTarget = try DatabaseIndexTransitionPlan.Target(
                scope: Self.entityIndexBuildScope(declaration),
                identity: try DatabaseIndexStorageIdentity.resolve(
                    named: target.identity.name,
                    in: lease.schema,
                    physicalLayout: try physicalLayout(
                        for: target.identity.name,
                        in: lease.indexPhysicalLayouts
                    )
                )
            )
        case .polymorphicGroup(let group, _):
            guard
                let declaration = lease.schema.polymorphicGroup(
                    identifier: group
                )
            else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "completed schema index build group is not declared"
                )
            }
            activeTarget = try DatabaseIndexTransitionPlan.Target(
                scope: try Self.polymorphicIndexBuildScope(declaration),
                identity: try DatabaseIndexStorageIdentity.resolve(
                    named: target.identity.name,
                    in: lease.schema,
                    physicalLayout: try physicalLayout(
                        for: target.identity.name,
                        in: lease.indexPhysicalLayouts
                    )
                )
            )
        }
        guard activeTarget == target else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "completed schema index build does not match the active physical generation"
            )
        }
        try transaction.clear(
            key: Self.schemaIndexBuildPendingKey(
                scope: target.scope,
                identity: target.identity,
                metadataSubspace: try dataRootSchemaMetadataSubspace()
            )
        )
    }

    package func clearSchemaIndexBuildPending(
        scope: DatabaseIndexStorageScope,
        index: String,
        selection: DatabaseIndexStorageRetirement,
        transaction: any TransactionAccess
    ) throws {
        try clearSchemaIndexBuildPending(
            storageScope: scope,
            index: index,
            selection: selection,
            transaction: transaction
        )
    }

    @_spi(DatabaseExecution)
    public func installDataRootSchemaSnapshot(
        _ schema: Schema,
        transaction: any TransactionAccess
    ) throws {
        let lease = acquireActiveSchemaLease()
        guard lease.schema == schema else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "data-root schema snapshot does not match the active generation"
            )
        }
        try Self.setCurrentSchemaSnapshot(
            schema,
            indexPhysicalFingerprint: lease.indexPhysicalFingerprint,
            executionRuntimeFingerprint: lease.executionRuntimeFingerprint,
            indexPhysicalLayouts: lease.indexPhysicalLayouts,
            metadataSubspace: activeDataRootMetadataSubspace(),
            transaction: transaction
        )
    }

    private func markSchemaIndexBuildPending(
        entity: String,
        index: String,
        schema: Schema,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        transaction: any TransactionAccess
    ) throws {
        let schemaMetadataSubspace = try dataRootSchemaMetadataSubspace()
        guard let entityDeclaration = schema.entity(named: entity) else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "pending schema index build entity is not declared"
            )
        }
        try transaction.setValue(
            [1],
            for: Self.schemaIndexBuildPendingKey(
                scope: Self.entityIndexBuildScope(entityDeclaration),
                identity: try DatabaseIndexStorageIdentity.resolve(
                    named: index,
                    in: schema,
                    physicalLayout: try physicalLayout(
                        for: index,
                        in: indexPhysicalLayouts
                    )
                ),
                metadataSubspace: schemaMetadataSubspace
            )
        )
    }

    private func isSchemaIndexBuildPending(
        scope: DatabaseIndexStorageScope,
        identity: DatabaseIndexStorageIdentity,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        try await transaction.getValue(
            for: Self.schemaIndexBuildPendingKey(
                scope: scope,
                identity: identity,
                metadataSubspace: try dataRootSchemaMetadataSubspace()
            ),
            snapshot: false
        ) != nil
    }

    private func markSchemaPolymorphicIndexBuildPending(
        group: String,
        index: String,
        schema: Schema,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        transaction: any TransactionAccess
    ) throws {
        let schemaMetadataSubspace = try dataRootSchemaMetadataSubspace()
        guard
            let groupDeclaration = schema.polymorphicGroup(
                identifier: group
            )
        else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "pending schema index build group is not declared"
            )
        }
        try transaction.setValue(
            [1],
            for: Self.schemaIndexBuildPendingKey(
                scope: try Self.polymorphicIndexBuildScope(groupDeclaration),
                identity: try DatabaseIndexStorageIdentity.resolve(
                    named: index,
                    in: schema,
                    physicalLayout: try physicalLayout(
                        for: index,
                        in: indexPhysicalLayouts
                    )
                ),
                metadataSubspace: schemaMetadataSubspace
            )
        )
    }

    private func partitionCatalogContainsEntries(
        entity: Schema.Entity,
        directoryLayers layerMap: DirectoryLayerTagMap,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        let page = try await partitionCatalogPage(
            entity: entity,
            directoryLayers: layerMap,
            continuation: nil,
            limit: 1,
            transaction: transaction
        )
        return !page.entries.isEmpty
    }

    private static func schemaIndexBuildPendingKey(
        scope: DatabaseIndexStorageScope,
        identity: DatabaseIndexStorageIdentity,
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .subspace("index-build")
            .pack(
                Tuple(
                    scope.stableOrderingKey,
                    identity.name,
                    identity.definitionFingerprint.bytes,
                    identity.layoutFingerprint
                )
            )
    }

    private func clearSchemaIndexBuildPending(
        storageScope: DatabaseIndexStorageScope,
        index: String,
        selection: DatabaseIndexStorageRetirement,
        transaction: any TransactionAccess
    ) throws {
        let builds = try dataRootSchemaMetadataSubspace()
            .subspace("schema")
            .subspace("index-build")
        let scope = storageScope.stableOrderingKey
        switch selection {
        case .allGenerations:
            let range = builds.subspace(scope).subspace(index).range()
            try transaction.clearRange(
                beginKey: range.begin,
                endKey: range.end
            )
        case .physicalGeneration(
            let definitionFingerprint,
            let layoutFingerprint
        ):
            try transaction.clear(
                key: builds.pack(
                    Tuple(
                        scope,
                        index,
                        definitionFingerprint.bytes,
                        layoutFingerprint
                    )
                )
            )
        }
    }

    private func physicalLayout(
        for indexName: String,
        in layouts: [String: IndexPhysicalLayout]
    ) throws -> IndexPhysicalLayout {
        guard let layout = layouts[indexName] else {
            throw
                DatabaseIndexStorageIdentityError
                .physicalLayoutNotResolved(
                    indexName
                )
        }
        return layout
    }

    private static func entityIndexBuildScope(
        _ entity: Schema.Entity
    ) -> DatabaseIndexStorageScope {
        .entity(
            name: entity.name,
            directoryComponents: entity.directoryComponents
        )
    }

    private static func polymorphicIndexBuildScope(
        _ group: PolymorphicGroup
    ) throws -> DatabaseIndexStorageScope {
        .polymorphicGroup(
            identifier: group.identifier,
            directoryPath: try group.resolvedDirectoryPath()
        )
    }

    package func dataRootSchemaMetadataSubspace() throws -> Subspace {
        try activeDataRootInternalMetadataSubspace()
    }

    private func activeDataRootMetadataSubspace() throws -> Subspace {
        #if DATABASE_MULTI_BASE
        try requireActiveDataRoot().systemRoot.subspace("metadata")
        #else
        defaultTenant.systemRoot.subspace("metadata")
        #endif
    }

    private func activeDataRootInternalMetadataSubspace() throws -> Subspace {
        #if DATABASE_MULTI_BASE
        try requireActiveDataRoot().systemRoot.subspace("_metadata")
        #else
        defaultTenant.systemRoot.subspace("_metadata")
        #endif
    }

    package static func activeSchemaFingerprintKey(
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .pack(Tuple("wireFingerprint"))
    }

    package static func activeExecutionRuntimeFingerprintKey(
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .pack(Tuple("executionRuntimeFingerprint"))
    }

    package static func activeIndexPhysicalFingerprintKey(
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .pack(Tuple("indexPhysicalFingerprint"))
    }

    static func restoreSchemaState(
        storageEngine: any StorageEngine,
        root: Subspace,
        metadataSubspace: Subspace,
        clock: any StorageMonotonicClock
    ) async throws -> DatabaseRestoredSchemaState {
        let registry = SchemaRegistry(
            database: storageEngine,
            root: root,
            clock: clock
        )
        return try await StorageTransactionExecutor(engine: storageEngine)
            .withTransaction(configuration: .default, clock: clock) {
                transaction in
                let entities = try await registry.loadAll(
                    transaction: transaction
                )
                let version = try await loadSchemaVersion(
                    metadataSubspace: metadataSubspace,
                    transaction: transaction
                )
                let fingerprint = try await loadActiveSchemaFingerprint(
                    metadataSubspace: metadataSubspace,
                    transaction: transaction
                )
                let generation = try await loadPersistedSchemaGeneration(
                    metadataSubspace: metadataSubspace,
                    transaction: transaction
                )
                let executionRuntimeFingerprint = try await loadActiveExecutionRuntimeFingerprint(
                    metadataSubspace: metadataSubspace,
                    transaction: transaction
                )
                let indexPhysicalFingerprint = try await loadActiveIndexPhysicalFingerprint(
                    metadataSubspace: metadataSubspace,
                    transaction: transaction
                )
                let indexLayoutFingerprints = try await loadActiveIndexLayoutFingerprints(
                    metadataSubspace: metadataSubspace,
                    transaction: transaction
                )
                if entities.isEmpty,
                    version == nil,
                    fingerprint == nil,
                    generation == nil,
                    indexPhysicalFingerprint == nil,
                    executionRuntimeFingerprint == nil
                {
                    let emptySchema = try Schema(
                        entities: [],
                        version: Schema.Version(0, 0, 0)
                    )
                    let fingerprint = try SchemaManifest(schema: emptySchema)
                        .fingerprint()
                    return DatabaseRestoredSchemaState(
                        schema: emptySchema,
                        fingerprint: fingerprint,
                        generation: 0,
                        indexPhysicalFingerprint: nil,
                        executionRuntimeFingerprint: nil,
                        indexLayoutFingerprints: [:]
                    )
                }

                guard let version else {
                    throw DatabaseSchemaRestorationError.missingVersion
                }
                guard let fingerprint else {
                    throw DatabaseSchemaRestorationError.missingFingerprint
                }
                guard let generation else {
                    throw DatabaseSchemaRestorationError.invalidGeneration
                }
                guard executionRuntimeFingerprint != nil else {
                    throw DatabaseSchemaRestorationError
                        .missingExecutionRuntimeFingerprint
                }
                guard indexPhysicalFingerprint != nil else {
                    throw DatabaseSchemaRestorationError
                        .missingIndexPhysicalFingerprint
                }
                let schema = try Schema(entities: entities, version: version)
                let computed = try SchemaManifest(schema: schema).fingerprint()
                guard computed == fingerprint else {
                    throw DatabaseSchemaRestorationError.fingerprintMismatch
                }
                return DatabaseRestoredSchemaState(
                    schema: schema,
                    fingerprint: fingerprint,
                    generation: generation,
                    indexPhysicalFingerprint: indexPhysicalFingerprint,
                    executionRuntimeFingerprint: executionRuntimeFingerprint,
                    indexLayoutFingerprints: indexLayoutFingerprints
                )
            }
    }

    package static func loadSchemaVersion(
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> Schema.Version? {
        let key =
            metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))
        guard
            let bytes = try await transaction.getValue(
                for: key,
                snapshot: false
            )
        else {
            return nil
        }
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 3,
            case .signedInteger(let major) = try tuple.value(at: 0),
            case .signedInteger(let minor) = try tuple.value(at: 1),
            case .signedInteger(let patch) = try tuple.value(at: 2),
            let majorValue = UInt32(exactly: major),
            let minorValue = UInt32(exactly: minor),
            let patchValue = UInt32(exactly: patch)
        else {
            throw DatabaseSchemaRestorationError.missingVersion
        }
        return Schema.Version(majorValue, minorValue, patchValue)
    }

    private static func schemaPublicationKey(
        idempotencyKey: String,
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .subspace("apply")
            .pack(Tuple(idempotencyKey))
    }

    package static func loadActiveSchemaFingerprint(
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> SchemaFingerprint? {
        guard
            let bytes = try await transaction.getValue(
                for: activeSchemaFingerprintKey(metadataSubspace: metadataSubspace),
                snapshot: false
            )
        else {
            return nil
        }
        do {
            return try SchemaFingerprint(bytes)
        } catch {
            throw DatabaseSchemaPublicationError.corruptedState(
                "active fingerprint has an invalid length"
            )
        }
    }

    package static func loadPersistedSchemaGeneration(
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> UInt64? {
        guard
            let bytes = try await transaction.getValue(
                for: schemaGenerationKey(metadataSubspace: metadataSubspace),
                snapshot: false
            )
        else {
            return nil
        }
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 1,
            let generation = unsignedInteger(
                try tuple.value(at: 0)
            )
        else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "generation has an invalid tuple encoding"
            )
        }
        return generation
    }

    package static func loadActiveExecutionRuntimeFingerprint(
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> ByteString? {
        guard
            let bytes = try await transaction.getValue(
                for: activeExecutionRuntimeFingerprintKey(
                    metadataSubspace: metadataSubspace
                ),
                snapshot: false
            )
        else {
            return nil
        }
        guard bytes.count == SHA256Accumulator.digestByteCount else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "execution runtime fingerprint has an invalid length"
            )
        }
        return bytes
    }

    package static func loadActiveIndexPhysicalFingerprint(
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> ByteString? {
        guard
            let bytes = try await transaction.getValue(
                for: activeIndexPhysicalFingerprintKey(
                    metadataSubspace: metadataSubspace
                ),
                snapshot: false
            )
        else {
            return nil
        }
        guard bytes.count == SHA256Accumulator.digestByteCount else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "index physical fingerprint has an invalid length"
            )
        }
        return bytes
    }

    package static func loadActiveIndexLayoutFingerprints(
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> [String: ByteString] {
        let storage = activeIndexPhysicalLayoutSubspace(
            metadataSubspace: metadataSubspace
        )
        let range = storage.range()
        let maximumCount = DatabaseWireLimits.default.maximumCollectionCount
        let rows = try await TransactionRangeCollection.collect(
            using: transaction,
            from: .firstGreaterOrEqual(range.begin),
            to: .firstGreaterOrEqual(range.end),
            limit: maximumCount + 1,
            snapshot: false,
            streamingMode: .wantAll
        )
        guard rows.count <= maximumCount else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "index layout fingerprint count exceeds the wire limit"
            )
        }
        var result: [String: ByteString] = [:]
        result.reserveCapacity(rows.count)
        for (key, value) in rows {
            let tuple = try storage.unpack(key)
            guard tuple.count == 1,
                case .string(let name) = try tuple.value(at: 0),
                !name.isEmpty,
                value.count == SHA256Accumulator.digestByteCount,
                result.updateValue(value, forKey: name) == nil
            else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "index layout fingerprint entry is invalid"
                )
            }
        }
        return result
    }

    package static func setActiveIndexPhysicalLayouts(
        _ layouts: [String: IndexPhysicalLayout],
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) throws {
        let storage = activeIndexPhysicalLayoutSubspace(
            metadataSubspace: metadataSubspace
        )
        let range = storage.range()
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)
        for name in layouts.keys.sorted() {
            guard !name.isEmpty, let layout = layouts[name] else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "index physical layout is invalid"
                )
            }
            try transaction.setValue(
                layout.fingerprint,
                for: storage.pack(Tuple(name))
            )
        }
    }

    private static func activeIndexPhysicalLayoutSubspace(
        metadataSubspace: Subspace
    ) -> Subspace {
        metadataSubspace
            .subspace("schema")
            .subspace("index-layout")
    }

    private static func loadSchemaPublication(
        idempotencyKey: String,
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> DatabaseSchemaPublicationResult? {
        guard let bytes = try await transaction.getValue(
            for: schemaPublicationKey(
                idempotencyKey: idempotencyKey,
                metadataSubspace: metadataSubspace
            ),
            snapshot: false
        ) else {
            return nil
        }
        return try decodeSchemaPublication(bytes)
    }

    private static func encodeSchemaPublication(
        _ publication: DatabaseSchemaPublicationResult
    ) -> ByteString {
        return Tuple(
            UInt64(6),
            publication.previousFingerprint != nil,
            publication.previousFingerprint?.bytes ?? ByteString(),
            publication.fingerprint.bytes,
            publication.generation,
            publication.indexPhysicalFingerprint,
            publication.executionRuntimeFingerprint,
            UInt64(publication.schemaVersion.major),
            UInt64(publication.schemaVersion.minor),
            UInt64(publication.schemaVersion.patch)
        ).pack()
    }

    private static func decodeSchemaPublication(
        _ bytes: ByteString
    ) throws -> DatabaseSchemaPublicationResult {
        do {
            let tuple = try Tuple(packed: bytes)
            guard tuple.count == 10,
                unsignedInteger(try tuple.value(at: 0)) == 6,
                case .boolean(let hasPrevious) = try tuple.value(at: 1),
                case .bytes(let previousBytes) = try tuple.value(at: 2),
                case .bytes(let fingerprintBytes) = try tuple.value(at: 3),
                let generation = unsignedInteger(try tuple.value(at: 4)),
                case .bytes(let indexPhysicalFingerprint) = try tuple.value(
                    at: 5
                ),
                indexPhysicalFingerprint.count
                    == SHA256Accumulator.digestByteCount,
                case .bytes(let executionRuntimeFingerprint) = try tuple.value(
                    at: 6
                ),
                executionRuntimeFingerprint.count
                    == SHA256Accumulator.digestByteCount,
                let major = unsignedInteger(try tuple.value(at: 7)),
                let minor = unsignedInteger(try tuple.value(at: 8)),
                let patch = unsignedInteger(try tuple.value(at: 9)),
                  let majorValue = UInt32(exactly: major),
                  let minorValue = UInt32(exactly: minor),
                  let patchValue = UInt32(exactly: patch) else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "idempotency record has an invalid tuple encoding"
                )
            }
            let previous = hasPrevious
                ? try SchemaFingerprint(previousBytes)
                : nil
            return DatabaseSchemaPublicationResult(
                previousFingerprint: previous,
                fingerprint: try SchemaFingerprint(fingerprintBytes),
                schemaVersion: Schema.Version(
                    majorValue,
                    minorValue,
                    patchValue
                ),
                generation: generation,
                indexPhysicalFingerprint: indexPhysicalFingerprint,
                executionRuntimeFingerprint: executionRuntimeFingerprint
            )
        } catch let publicationError as DatabaseSchemaPublicationError {
            throw publicationError
        } catch {
            throw DatabaseSchemaPublicationError.corruptedState(
                "idempotency record cannot be decoded"
            )
        }
    }

    private static func unsignedInteger(_ value: TupleValue) -> UInt64? {
        switch value {
        case .unsignedInteger(let value):
            return value
        case .signedInteger(let value) where value >= 0:
            return UInt64(value)
        default:
            return nil
        }
    }
}

private struct SchemaPublicationTransactionOutcome: Sendable {
    let publication: DatabaseSchemaPublicationResult
    let shouldPublishGeneration: Bool
}
