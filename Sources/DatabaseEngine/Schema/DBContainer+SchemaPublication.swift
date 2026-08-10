import DatabaseKit
import DatabaseTypes
@_spi(DatabaseServer) import DatabaseWire
import StorageKit

extension DBContainer {
    package func requireNoActiveSchemaTransition(
        transaction: any TransactionAccess
    ) async throws {
        try await DatabaseSchemaApplicationStore(
            metadataSubspace: metadataSubspace
        ).requireNoActiveTransition(transaction: transaction)
    }

    /// Atomically publishes the complete initial schema catalog for a compiled
    /// application. Existing complete state is validated, never overwritten.
    package func initializeSchemaCatalogIfNeeded(
        _ schema: Schema,
        fingerprint targetFingerprint: SchemaFingerprint
    ) async throws {
        let registry = SchemaRegistry(
            database: storageTopology.controlDomain.engine,
            root: storageTopology.controlDomain.root,
            clock: monotonicClock
        )
        try await controlTransactionExecutor.withTransaction(
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

            if entities.isEmpty,
               version == nil,
               fingerprint == nil,
               generation == nil {
                try await registry.persistInitialSchema(
                    schema,
                    transaction: transaction
                )
                try Self.setCurrentSchemaSnapshot(
                    schema,
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
                return
            }

            guard let version else {
                throw DatabaseSchemaRestorationError.missingVersion
            }
            guard let fingerprint else {
                throw DatabaseSchemaRestorationError.missingFingerprint
            }
            guard generation != nil else {
                throw DatabaseSchemaRestorationError.invalidGeneration
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
        }
    }

    package func storedSchemaPublication(
        idempotencyKey: String,
        matching targetFingerprint: SchemaFingerprint,
        authorization: AuthorizationContext
    ) async throws -> DatabaseSchemaPublicationResult? {
        guard !idempotencyKey.isEmpty else {
            throw DatabaseSchemaPublicationError.invalidIdempotencyKey
        }
        return try await controlTransactionExecutor.withTransaction(
            configuration: .readOnly,
            clock: monotonicClock
        ) { transaction in
            try await self.databaseGrantStore.require(
                .administer,
                authorization: authorization,
                transaction: transaction
            )
            guard let stored = try await Self.loadSchemaPublication(
                idempotencyKey: idempotencyKey,
                metadataSubspace: self.metadataSubspace,
                transaction: transaction
            ) else {
                return nil
            }
            guard stored.fingerprint == targetFingerprint else {
                throw DatabaseSchemaPublicationError.idempotencyKeyReused(
                    idempotencyKey
                )
            }
            return stored
        }
    }

    package func publishSchema(
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

        try validateSchemaGeneration(
            schema,
            runtimeConfiguration: targetRuntimeConfiguration
        )

        let publishedLease = acquirePublishedSchemaLease()
        let leasedFingerprint = publishedLease.fingerprint.detached()
        let leasedGeneration = publishedLease.generation
        let registry = SchemaRegistry(
            database: storageTopology.controlDomain.engine,
            root: storageTopology.controlDomain.root,
            clock: monotonicClock
        )
        let outcome = try await controlTransactionExecutor.withTransaction(
            configuration: .batch,
            clock: monotonicClock,
            executionDeadline: nil
        ) { transaction in
            try await self.databaseGrantStore.require(
                .administer,
                authorization: authorization,
                transaction: transaction
            )
            let databaseTransaction = DatabaseTransaction(
                storageAccess: transaction,
                container: self
            )
            do {
            if let stored = try await Self.loadSchemaPublication(
                idempotencyKey: idempotencyKey,
                metadataSubspace: self.metadataSubspace,
                transaction: transaction
            ) {
                guard stored.fingerprint == targetFingerprint else {
                    throw DatabaseSchemaPublicationError.idempotencyKeyReused(
                        idempotencyKey
                    )
                }
                let activeFingerprint = try await Self
                    .loadActiveSchemaFingerprint(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )
                let activeGeneration = try await Self
                    .loadPersistedSchemaGeneration(
                        metadataSubspace: self.metadataSubspace,
                        transaction: transaction
                    )
                guard activeFingerprint != nil, activeGeneration != nil else {
                    throw DatabaseSchemaPublicationError.corruptedState(
                        "idempotent publication exists without active schema state"
                    )
                }
                let outcome = SchemaPublicationTransactionOutcome(
                    publication: stored,
                    shouldPublishGeneration:
                        activeFingerprint == stored.fingerprint
                            && activeGeneration == stored.generation
                )
                try await databaseTransaction.prepareForCommit()
                return outcome
            }

            let actualFingerprint = try await Self.loadActiveSchemaFingerprint(
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
            let nextGeneration: UInt64
            if actualFingerprint == targetFingerprint {
                nextGeneration = currentGeneration
            } else {
                let incremented = currentGeneration.addingReportingOverflow(1)
                guard !incremented.overflow else {
                    throw DatabaseSchemaPublicationError.generationOverflow
                }
                nextGeneration = incremented.partialValue
            }

            try await registry.persist(
                schema,
                mode: .strict,
                transaction: transaction
            )
            try Self.setCurrentSchemaSnapshot(
                schema,
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
                generation: nextGeneration
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
            publishSchemaGeneration(
                schema,
                fingerprint: outcome.publication.fingerprint,
                runtimeConfiguration: targetRuntimeConfiguration,
                generation: outcome.publication.generation
            )
        }
        return outcome.publication
    }

    package func validateSchemaGeneration(
        _ schema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) throws {
        try runtimeConfiguration.validate(schema: schema)
        try runtimeConfiguration.validateStorageRequirements(
            schema: schema,
            transactionCapabilities: transactionCapabilities
        )
        try IndexRuntimeConfigurationValidator.validate(
            configuration.indexConfigurations,
            schema: schema,
            entityRuntimes: runtimeConfiguration.entityRuntimes
        )
    }

    package func initializeNewSchemaIndexStates(
        from previous: Schema,
        to target: Schema,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        var requiresPersistentBuild = false
        for entity in target.entities where previous.entity(named: entity.name) == nil {
            guard !entity.hasDynamicDirectory,
                  !entity.indexDescriptors.isEmpty else {
                continue
            }
            let subspace = try await resolveDirectory(
                for: entity,
                declaredIn: target,
                transaction: transaction
            )
            try await IndexLifecycleStore(
                container: self,
                subspace: subspace
            ).ensureReadable(
                entity.indexDescriptors.map { $0.name },
                entityRange: subspace
                    .subspace(SubspaceKey.items)
                    .subspace(entity.name)
                    .range(),
                transaction: transaction
            )
        }

        for targetEntity in target.entities {
            guard let previousEntity = previous.entity(named: targetEntity.name)
            else { continue }
            let previousIndexNames = Set(
                previousEntity.indexDescriptors.map { $0.name }
            )
            let addedIndexes = targetEntity.indexDescriptors.filter {
                !previousIndexNames.contains($0.name)
            }
            guard !addedIndexes.isEmpty else { continue }

            if targetEntity.hasDynamicDirectory {
                guard try await partitionCatalogContainsEntries(
                    entity: targetEntity.name,
                    transaction: transaction
                ) else {
                    continue
                }
                for descriptor in addedIndexes {
                    try markSchemaIndexBuildPending(
                        entity: targetEntity.name,
                        index: descriptor.name,
                        transaction: transaction
                    )
                }
                requiresPersistentBuild = true
                continue
            }

            let subspace = try await resolveDirectory(
                for: targetEntity,
                declaredIn: target,
                transaction: transaction
            )
            let lifecycleStore = IndexLifecycleStore(
                container: self,
                subspace: subspace
            )
            let entityRange = subspace
                .subspace(SubspaceKey.items)
                .subspace(targetEntity.name)
                .range()
            for descriptor in addedIndexes {
                let needsBuild = try await lifecycleStore.prepareSchemaBuild(
                    descriptor.name,
                    entityRange: entityRange,
                    transaction: transaction
                )
                guard needsBuild else { continue }
                try markSchemaIndexBuildPending(
                    entity: targetEntity.name,
                    index: descriptor.name,
                    transaction: transaction
                )
                requiresPersistentBuild = true
            }
        }

        for group in target.polymorphicGroups
        where previous.polymorphicGroup(identifier: group.identifier) == nil
            && !group.indexes.isEmpty {
            let subspace = try activeDataSubspace(
                relativePath: group.resolvedDirectoryPath()
            )
            try await IndexLifecycleStore(
                container: self,
                subspace: subspace
            ).ensureReadable(
                group.indexes.map { $0.name },
                entityRange: subspace.subspace(SubspaceKey.items).range(),
                transaction: transaction
            )
        }
        return requiresPersistentBuild
    }

    package func pendingSchemaIndexBuilds(
        entity: String,
        indexes: [String],
        transaction: any TransactionAccess
    ) async throws -> Set<String> {
        let schemaMetadataSubspace = try baseSchemaMetadataSubspace()
        var pending = Set<String>()
        pending.reserveCapacity(indexes.count)
        for index in indexes {
            if try await transaction.getValue(
                for: Self.schemaIndexBuildPendingKey(
                    entity: entity,
                    index: index,
                    metadataSubspace: schemaMetadataSubspace
                ),
                snapshot: false
            ) != nil {
                pending.insert(index)
            }
        }
        return pending
    }

    package func pendingSchemaIndexBuilds(
        in schema: Schema
    ) async throws -> [String: Set<String>] {
        try await transactionExecutor.withTransaction(
            configuration: .default,
            clock: monotonicClock
        ) { transaction in
            try await self.pendingSchemaIndexBuilds(
                in: schema,
                transaction: transaction
            )
        }
    }

    package func pendingSchemaIndexBuilds(
        in schema: Schema,
        transaction: any TransactionAccess
    ) async throws -> [String: Set<String>] {
        var result: [String: Set<String>] = [:]
        for entity in schema.entities {
            let pending = try await pendingSchemaIndexBuilds(
                entity: entity.name,
                indexes: entity.indexDescriptors.map { $0.name },
                transaction: transaction
            )
            if !pending.isEmpty {
                result[entity.name] = pending
            }
        }
        return result
    }

    package func completeSchemaIndexBuild(
        entity: String,
        index: String,
        transaction: any TransactionAccess
    ) throws {
        let schemaMetadataSubspace = try baseSchemaMetadataSubspace()
        try transaction.clear(
            key: Self.schemaIndexBuildPendingKey(
                entity: entity,
                index: index,
                metadataSubspace: schemaMetadataSubspace
            )
        )
    }

    package func installBaseSchemaSnapshot(
        _ schema: Schema,
        transaction: any TransactionAccess
    ) throws {
        try Self.setCurrentSchemaSnapshot(
            schema,
            metadataSubspace: requireBoundBaseLease().root
                .subspace("metadata"),
            transaction: transaction
        )
    }

    private func markSchemaIndexBuildPending(
        entity: String,
        index: String,
        transaction: any TransactionAccess
    ) throws {
        let schemaMetadataSubspace = try baseSchemaMetadataSubspace()
        try transaction.setValue(
            [1],
            for: Self.schemaIndexBuildPendingKey(
                entity: entity,
                index: index,
                metadataSubspace: schemaMetadataSubspace
            )
        )
    }

    private func partitionCatalogContainsEntries(
        entity: String,
        transaction: any TransactionAccess
    ) async throws -> Bool {
        let page = try await partitionCatalogPage(
            entity: entity,
            continuation: nil,
            limit: 1,
            transaction: transaction
        )
        return !page.entries.isEmpty
    }

    private static func schemaIndexBuildPendingKey(
        entity: String,
        index: String,
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .subspace("index-build")
            .pack(Tuple(entity, index))
    }

    private func baseSchemaMetadataSubspace() throws -> Subspace {
        try requireBoundBaseLease().root.subspace("_metadata")
    }

    package static func activeSchemaFingerprintKey(
        metadataSubspace: Subspace
    ) -> ByteString {
        metadataSubspace
            .subspace("schema")
            .pack(Tuple("wireFingerprint"))
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
                if entities.isEmpty,
                   version == nil,
                   fingerprint == nil,
                   generation == nil {
                    let emptySchema = try Schema(
                        entities: [],
                        version: Schema.Version(0, 0, 0)
                    )
                    let fingerprint = try SchemaManifest(schema: emptySchema)
                        .fingerprint()
                    return DatabaseRestoredSchemaState(
                        schema: emptySchema,
                        fingerprint: fingerprint,
                        generation: 0
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
                let schema = try Schema(entities: entities, version: version)
                let computed = try SchemaManifest(schema: schema).fingerprint()
                guard computed == fingerprint else {
                    throw DatabaseSchemaRestorationError.fingerprintMismatch
                }
                return DatabaseRestoredSchemaState(
                    schema: schema,
                    fingerprint: fingerprint,
                    generation: generation
                )
            }
    }

    package static func loadSchemaVersion(
        metadataSubspace: Subspace,
        transaction: any TransactionAccess
    ) async throws -> Schema.Version? {
        let key = metadataSubspace
            .subspace("schema")
            .pack(Tuple("version"))
        guard let bytes = try await transaction.getValue(
            for: key,
            snapshot: false
        ) else {
            return nil
        }
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 3,
              case .signedInteger(let major) = try tuple.value(at: 0),
              case .signedInteger(let minor) = try tuple.value(at: 1),
              case .signedInteger(let patch) = try tuple.value(at: 2),
              let majorValue = UInt32(exactly: major),
              let minorValue = UInt32(exactly: minor),
              let patchValue = UInt32(exactly: patch) else {
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
        guard let bytes = try await transaction.getValue(
            for: activeSchemaFingerprintKey(metadataSubspace: metadataSubspace),
            snapshot: false
        ) else {
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
        guard let bytes = try await transaction.getValue(
            for: schemaGenerationKey(metadataSubspace: metadataSubspace),
            snapshot: false
        ) else {
            return nil
        }
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 1,
              let generation = unsignedInteger(
                  try tuple.value(at: 0)
              ) else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "generation has an invalid tuple encoding"
            )
        }
        return generation
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
            UInt64(3),
            publication.previousFingerprint != nil,
            publication.previousFingerprint?.bytes ?? ByteString(),
            publication.fingerprint.bytes,
            publication.generation,
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
            guard tuple.count == 8,
                  unsignedInteger(try tuple.value(at: 0)) == 3,
                  case .boolean(let hasPrevious) = try tuple.value(at: 1),
                  case .bytes(let previousBytes) = try tuple.value(at: 2),
                  case .bytes(let fingerprintBytes) = try tuple.value(at: 3),
                  let generation = unsignedInteger(try tuple.value(at: 4)),
                  let major = unsignedInteger(try tuple.value(at: 5)),
                  let minor = unsignedInteger(try tuple.value(at: 6)),
                  let patch = unsignedInteger(try tuple.value(at: 7)),
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
                generation: generation
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
