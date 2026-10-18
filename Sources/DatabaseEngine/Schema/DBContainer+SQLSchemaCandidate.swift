import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit

extension DBContainer {
    package static func sqlIndexPhysicalLayouts(_ catalog: SQLCatalog,
        including physicalLayouts: [String: IndexPhysicalLayout]
    ) throws -> [String: IndexPhysicalLayout] {
        var layouts = physicalLayouts
        for identity in try SQLIndexRuntime.identities(catalog: catalog) {
            let layout = try SQLIndexRuntime.physicalLayout()
            guard layouts[identity.name] == nil else {
                throw DatabaseSchemaPublicationError.corruptedState("SQL index storage name collides with a physical index")
            }
            layouts[identity.name] = layout
        }
        return layouts
    }

    package func prepareSQLSchemaCandidate(schema: Schema, catalog: SQLCatalog,
        sourceLease: DatabaseSchemaLease, workMeter: DatabaseWorkMeter
    ) throws -> DatabaseSQLSchemaCandidate {
        try workMeter.consume(at: .validation)
        let schemaBytes = try SchemaManifest(schema: schema).canonicalBytes().count
        let fingerprint = try SchemaManifest(schema: schema).fingerprint()
        let sourceNames = Set(sourceLease.schema.entities.map { SQLCatalogName($0.name) })
        let retainedNames = Set(sourceLease.sqlCatalog.sqlOwnedEntityNames.map(SQLCatalogName.init))
        let sqlOwnedEntityNames = schema.entities.filter {
            retainedNames.contains(SQLCatalogName($0.name)) || !sourceNames.contains(SQLCatalogName($0.name))
        }.map { $0.name }.sorted { $0.utf8.lexicographicallyPrecedes($1.utf8) }
        let admittedCatalogBytes = try SQLCatalogCodec.admit(catalog, fingerprint: fingerprint,
            protectedContracts: sourceLease.sqlCatalog.protectedContracts,
            sqlOwnedEntityNames: sqlOwnedEntityNames, limits: .default)
        let reservation = try workMeter.reserveIntermediate(bytes: UInt64(admittedCatalogBytes) + UInt64(schemaBytes)
            + UInt64(MemoryLayout<DatabaseSchemaGeneration>.stride), at: .validation)
        let runtime = try sourceLease.runtimeConfiguration.replacingPhysicalSchema(schema)
        let snapshot = try sourceLease.sqlCatalog.replacingPhysicalCatalog(catalog,
            physicalSchema: schema, runtimeConfiguration: runtime, sqlOwnedEntityNames: sqlOwnedEntityNames)
        let prepared = try prepareSchemaGeneration(schema, runtimeConfiguration: runtime,
            catalogSnapshot: snapshot)
        let layers = try DirectoryLayerTagMap(entities: schema.entities,
            polymorphicGroups: schema.polymorphicGroups)
        let security: (any DataStoreSecurityDelegate)?
        switch securityConfiguration.policyEvaluation {
        case .enabled:
            security = RequestSecurityPolicyDelegate(policies: runtime.authorizationPolicies, schema: schema)
        case .disabledForTesting: security = nil
        }
        let next = sourceLease.generation.addingReportingOverflow(1)
        guard !next.overflow else { throw DatabaseSchemaPublicationError.generationOverflow }
        let catalogBytes = try SQLCatalogCodec.admit(snapshot.catalog,
            fingerprint: snapshot.physicalSchemaFingerprint, protectedContracts: snapshot.protectedContracts,
            sqlOwnedEntityNames: snapshot.sqlOwnedEntityNames, limits: .default, runtimeConfiguration: runtime)
        if catalogBytes > admittedCatalogBytes {
            try reservation.reserveAdditional(bytes: UInt64(catalogBytes - admittedCatalogBytes), at: .validation)
        }
        let generation = DatabaseSchemaGeneration(identifier: next.partialValue,
            fingerprint: snapshot.physicalSchemaFingerprint,
            indexPhysicalFingerprint: prepared.indexPhysicalFingerprint,
            executionRuntimeFingerprint: prepared.executionRuntimeFingerprint, schema: schema,
            sqlCatalog: snapshot, runtimeConfiguration: runtime,
            indexPhysicalLayouts: prepared.indexPhysicalLayouts, directoryLayers: layers,
            securityDelegate: security)
        return DatabaseSQLSchemaCandidate(sourceLease: sourceLease, generation: generation,
            prepared: prepared, reservation: reservation)
    }
}

extension DBContainer {
    func stageSQLSchemaCandidate(_ candidate: DatabaseSQLSchemaCandidate,
        expectedGeneration: UInt64, idempotencyKey: String, rewrites: [SQLSchemaRewrite],
        authorization: AuthorizationContext, transaction: any TransactionAccess,
        workMeter: DatabaseWorkMeter
    ) async throws -> DatabaseSchemaStagingOutcome {
        guard !idempotencyKey.isEmpty else { throw DatabaseSchemaPublicationError.invalidIdempotencyKey }
        let source = candidate.sourceLease
        let target = candidate.lease
        let changed = Set(source.schema.entities.filter { target.schema.entity(named: $0.name) != $0 }.map { $0.name })
        let rewritten = Set(rewrites.map { $0.entityName })
        guard rewritten.count == rewrites.count, changed.isSubset(of: rewritten),
              rewritten.isSubset(of: Set(source.schema.entities.map { $0.name })) else {
            throw DatabaseSchemaPublicationError.corruptedState("physical candidate requires exactly one rewrite for every changed source entity")
        }
        for old in source.schema.entities {
            if let new = target.schema.entity(named: old.name) {
                guard old.directoryComponents == new.directoryComponents,
                    old.directoryLayer == new.directoryLayer, old.identifierType == new.identifierType else {
                    throw DatabaseSchemaPublicationError.corruptedState("SQL DDL cannot change physical Directory or identifier contracts")
                }
            }
        }
        #if DATABASE_MULTI_BASE
        let schemaEngine = storageTopology.controlDomain.engine
        let schemaRoot = storageTopology.controlDomain.systemRoot
        #else
        let schemaEngine = engine
        let schemaRoot = defaultTenant.systemRoot
        #endif
        guard transaction.transactionDomain === schemaEngine.transactionDomain else {
            throw StorageError.unsupportedOperation("physical SQL DDL requires the schema aggregate transaction domain")
        }
        #if DATABASE_MULTI_BASE
        try await databaseGrantStore.require(.administer, authorization: authorization, transaction: transaction)
        #endif
        let registry = SchemaRegistry(database: schemaEngine, root: schemaRoot, clock: monotonicClock)
        let outcome = try await writeSchemaAggregate(target.schema, fingerprint: target.fingerprint,
            expectedFingerprint: source.fingerprint, expectedGeneration: expectedGeneration,
            idempotencyKey: idempotencyKey, preparedGeneration: candidate.prepared,
            publishedLease: source, sqlCatalog: target.sqlCatalog, requiresIdleSchemaTransition: true,
            registry: registry, transaction: transaction,
            advancesSQLWriterGeneration: try source.fingerprint != target.fingerprint
                || source.sqlCatalog.catalog.indexes != target.sqlCatalog.catalog.indexes
                || Self.changesSQLIntegrity(from: source.sqlCatalog.catalog, to: target.sqlCatalog.catalog),
            registryMode: .allowBreakingChanges(entityNames: changed),
            validateExistingData: {
                try await self.transformSQLSchemaCandidate(candidate, rewrites: rewrites,
                    authorization: authorization, transaction: transaction, workMeter: workMeter)
                try await DatabaseSchemaExecutionScope.$container.withValue(self) {
                    try await DatabaseSchemaExecutionScope.$lease.withValue(target) {
                        try await self.validateSQLPublication(target.sqlCatalog, sourceLease: target,
                            authorization: authorization, transaction: transaction, workMeter: workMeter)
                    }
                }
            })
        guard outcome.shouldPublishGeneration else {
            return DatabaseSchemaStagingOutcome(publication: outcome.publication, state: nil)
        }
        let generation = DatabaseSchemaGeneration(identifier: outcome.publication.generation,
            fingerprint: target.fingerprint, indexPhysicalFingerprint: target.indexPhysicalFingerprint,
            executionRuntimeFingerprint: target.executionRuntimeFingerprint, schema: target.schema,
            sqlCatalog: target.sqlCatalog, runtimeConfiguration: target.runtimeConfiguration,
            indexPhysicalLayouts: target.indexPhysicalLayouts, directoryLayers: target.directoryLayers,
            securityDelegate: target.securityDelegate)
        return DatabaseSchemaStagingOutcome(publication: outcome.publication,
            state: DatabaseSchemaStagingState(container: self, generation: generation,
                sourceLease: source, reservation: candidate.reservation))
    }

    private func transformSQLSchemaCandidate(_ candidate: DatabaseSQLSchemaCandidate,
        rewrites: [SQLSchemaRewrite], authorization: AuthorizationContext,
        transaction: any TransactionAccess, workMeter: DatabaseWorkMeter
    ) async throws {
        #if DATABASE_MULTI_BASE
        let records = try await baseCatalog.loadAll(transaction: transaction)
        // Admit every retained root before the first physical mutation.
        for record in records {
            switch record.lifecycle {
            case .tombstone: continue
            case .active, .retired: break
            default: throw DatabaseSchemaPublicationError.transitionInProgress
            }
            guard let domain = storageTopology.domain(identifiedBy: record.domainID),
                domain.engine.transactionDomain === transaction.transactionDomain else {
                throw StorageError.unsupportedOperation("physical SQL DDL requires all retained roots in one transaction domain")
            }
            let admission = try acquireBaseSchemaMaintenanceLease(record.id)
            defer { admission.finish() }
            guard admission.generation.record == record else { throw DatabaseSchemaPublicationError.transitionInProgress }
            try await withBaseLease(admission) {
                let authority = try await admission.dataRoot.partitionAuthority.lease(in: transaction)
                try await authority.withReadAccess(transaction) { _ in
                    _ = try await DatabaseGrantStore(resource: .base(record.id), root: admission.systemRoot)
                        .require(.administer, authorization: authorization, transaction: transaction)
                }
            }
        }
        for record in records where record.lifecycle != .tombstone {
            let lease = try acquireBaseSchemaMaintenanceLease(record.id)
            defer { lease.finish() }
            guard lease.generation.record == record else { throw DatabaseSchemaPublicationError.transitionInProgress }
            try await withBaseLease(lease) {
                let authority = try await lease.dataRoot.partitionAuthority.lease(in: transaction)
                try await authority.withReadAccess(transaction) { _ in
                    try await self.transformSQLSchemaCandidateRoot(candidate, rewrites: rewrites,
                        authorization: authorization, transaction: transaction, workMeter: workMeter)
                }
            }
        }
        #else
        try await transformSQLSchemaCandidateRoot(candidate, rewrites: rewrites,
            authorization: authorization, transaction: transaction, workMeter: workMeter)
        #endif
    }

    private func transformSQLSchemaCandidateRoot(_ candidate: DatabaseSQLSchemaCandidate,
        rewrites: [SQLSchemaRewrite], authorization: AuthorizationContext,
        transaction: any TransactionAccess, workMeter: DatabaseWorkMeter
    ) async throws {
        let source = candidate.sourceLease
        let target = candidate.lease
        let byEntity = Dictionary(uniqueKeysWithValues: rewrites.map { ($0.entityName, $0) })
        try await DatabaseSchemaExecutionScope.$container.withValue(self) {
            try await DatabaseSchemaExecutionScope.$lease.withValue(source) {
                for entity in source.schema.entities {
                    var continuation: ByteString?
                    repeat {
                        try workMeter.consume(at: .validation)
                        let paths: [AnyDirectoryPath?]
                        if entity.hasDynamicDirectory {
                            let page = try await self.partitionCatalogPage(entity: entity,
                                directoryLayers: source.directoryLayers, continuation: continuation,
                                limit: 64, transaction: transaction)
                            paths = try page.entries.map {
                                try CanonicalPartitionBinding.makeAnyBinding(for: entity, partitions: $0.partitions)
                            }
                            continuation = page.continuation
                        } else {
                            paths = [nil]
                            continuation = nil
                        }
                        for path in paths {
                            guard let root = try await self.openDirectory(for: entity,
                                path: path, transaction: transaction) else { continue }
                            let sourceStore = DatabaseDataStore(container: self, subspace: root, entity: entity,
                                readPolicy: DatabaseReadPolicy(schemaLease: source, authorization: authorization),
                                securityDelegate: source.securityDelegate,
                                indexConfigurations: source.runtimeConfiguration.indexConfigurations)
                            let targetEntity = target.schema.entity(named: entity.name)
                            let targetStore = targetEntity.map {
                                DatabaseDataStore(container: self, subspace: root, entity: $0,
                                    readPolicy: DatabaseReadPolicy(schemaLease: target, authorization: authorization),
                                    securityDelegate: target.securityDelegate,
                                    indexConfigurations: target.runtimeConfiguration.indexConfigurations)
                            }
                            if let rewrite = byEntity[entity.name] {
                                try await sourceStore.rewriteSQLRows(to: targetStore, transform: rewrite,
                                    transaction: transaction, workMeter: workMeter)
                            }
                            if let runtime = target.runtimeConfiguration.entityRuntimes.registration(named: entity.name)
                                ?? source.runtimeConfiguration.entityRuntimes.registration(named: entity.name) {
                                try await SQLIndexRuntime.rebuild(sourceCatalog: source.sqlCatalog.catalog,
                                    targetCatalog: target.sqlCatalog.catalog, physicalSchema: target.schema,
                                    storeSubspace: root, container: self, runtime: runtime,
                                    transaction: transaction, workMeter: workMeter,
                                    skipBackfill: byEntity[entity.name] != nil)
                            }
                        }
                    } while continuation != nil
                }
            }
        }
    }
}
