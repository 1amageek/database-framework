import DatabaseKit
import DatabaseTypes
import StorageKit

extension DBContainer {
    /// Names and input defaults do not change the validity of retained rows.
    static func changesSQLIntegrity(from previous: SQLCatalog, to candidate: SQLCatalog) -> Bool {
        for relation in candidate.relations {
            let old = previous.relations.first { SQLConstraintCompiler.same($0.id, relation.id) }
            if relation.primaryKeyColumnIDs?.map(SQLCatalogName.init) != old?.primaryKeyColumnIDs?.map(SQLCatalogName.init)
                || relation.primaryKeyConstraint?.timing != old?.primaryKeyConstraint?.timing { return true }
            if relation.constraints.count != (old?.constraints.count ?? 0) { return true }
            for constraint in relation.constraints {
                guard let prior = old?.constraints.first(where: { SQLConstraintCompiler.same($0.id, constraint.id) }),
                    prior.kind == constraint.kind, prior.timing == constraint.timing else { return true }
            }
            for column in relation.columns {
                let prior = old?.columns.first { SQLConstraintCompiler.same($0.id, column.id) }
                switch (column.policy, prior?.policy) {
                case (.generated(let expression), .generated(let oldExpression)):
                    if expression != oldExpression || column.backing != prior?.backing { return true }
                case (.generated, _), (.identity, _):
                    if column.policy != prior?.policy || column.backing != prior?.backing { return true }
                case (_, .generated), (_, .identity): return true
                default: break
                }
            }
        }
        return previous.relations.contains { old in
            !candidate.relations.contains(where: { SQLConstraintCompiler.same($0.id, old.id) })
                && (old.primaryKeyColumnIDs != nil || !old.constraints.isEmpty)
        }
    }

    #if DATABASE_MULTI_BASE
    func requireSQLIntegrityPlacement(in domain: DatabaseStorageDomainRuntime,
        transaction: any TransactionAccess) async throws {
        guard domain.engine.transactionDomain !== transaction.transactionDomain else { return }
        guard try await transaction.getValue(for: Self.sqlWriterGenerationKey(metadataSubspace: metadataSubspace),
            snapshot: false) != nil else { return }
        guard let state = try await SQLCatalogStore(root: metadataSubspace).load(transaction: transaction) else { return }
        if Self.hasSQLIntegrityObligations(state.catalog) {
            throw StorageError.unsupportedOperation("SQL integrity publication requires all retained roots in one transaction domain")
        }
    }
    #endif

    static func hasSQLIntegrityObligations(_ catalog: SQLCatalog) -> Bool {
        catalog.relations.contains { relation in
            relation.primaryKeyColumnIDs != nil || !relation.constraints.isEmpty || relation.columns.contains { column in
                guard case .field = column.backing else { return false }
                switch column.policy { case .generated, .identity: return true; case .input: return false }
            }
        }
    }

    static func sqlWriterGenerationKey(metadataSubspace: Subspace) -> ByteString {
        metadataSubspace.subspace("schema").pack(Tuple("minimum-sql-writer-generation"))
    }

    func requireSQLWriterGeneration(_ generation: UInt64, transaction: any TransactionAccess) async throws {
        #if DATABASE_MULTI_BASE
        guard transaction.transactionDomain === storageTopology.controlDomain.engine.transactionDomain else { return }
        #endif
        guard let bytes = try await transaction.getValue(for: Self.sqlWriterGenerationKey(metadataSubspace: metadataSubspace),
            snapshot: false) else { return }
        let tuple = try Tuple(packed: bytes)
        guard tuple.count == 1 else {
            throw DatabaseSchemaPublicationError.corruptedState("SQL writer generation floor has an invalid encoding")
        }
        let minimum: UInt64
        switch try tuple.value(at: 0) {
        case .unsignedInteger(let value): minimum = value
        case .signedInteger(let value) where value >= 0: minimum = UInt64(value)
        default: throw DatabaseSchemaPublicationError.corruptedState("SQL writer generation floor has an invalid encoding")
        }
        guard generation >= minimum else {
            throw DatabaseSchemaPublicationError.generationConflict(expected: minimum, actual: generation)
        }
    }

    func validateSQLPublication(_ snapshot: SQLCatalogSnapshot, sourceLease: DatabaseSchemaLease,
        authorization: AuthorizationContext, transaction: any TransactionAccess, workMeter: DatabaseWorkMeter
    ) async throws {
        #if DATABASE_MULTI_BASE
        let records = try await baseCatalog.loadAll(transaction: transaction)
        for record in records {
            switch record.lifecycle {
            case .tombstone: continue
            case .active, .retired: break
            default: throw DatabaseSchemaPublicationError.transitionInProgress
            }
            guard let domain = storageTopology.domain(identifiedBy: record.domainID) else {
                throw DatabaseBaseExecutionError.storageDomainUnavailable(record.domainID)
            }
            guard domain.engine.transactionDomain === transaction.transactionDomain else {
                throw StorageError.unsupportedOperation("SQL integrity publication requires all retained roots in one transaction domain")
            }
        }
        for record in records where record.lifecycle != .tombstone {
            try workMeter.consume(at: .validation)
            let baseLease = try acquireBaseSchemaMaintenanceLease(record.id)
            defer { baseLease.finish() }
            guard baseLease.generation.record == record else {
                throw DatabaseSchemaPublicationError.transitionInProgress
            }
            try await withBaseLease(baseLease) {
                let authority = try await baseLease.dataRoot.partitionAuthority.lease(in: transaction)
                try await authority.withReadAccess(transaction) { _ in
                    let granted = try await DatabaseGrantStore(resource: .base(record.id), root: baseLease.systemRoot)
                        .require(.administer, authorization: authorization, transaction: transaction)
                    let context = self.session(authorization: authorization).base(record.id).newContext()
                    let validator = DatabaseTransaction(storageAccess: transaction, container: self,
                        readPolicy: DatabaseReadPolicy(schemaLease: sourceLease, authorization: authorization))
                    let binding = try DatabaseTransactionExecutionBinding(context: context, transaction: transaction,
                        transactionInstant: validator.transactionInstant, grantedAccess: granted, databaseTransaction: validator)
                    try await ActiveDatabaseTransactionContext.$binding.withValue(binding.rebindingSchemaLease(sourceLease)) {
                        do {
                            try await validator.validateSQLCatalog(snapshot, workMeter: workMeter)
                            try await validator.prepareForCommit()
                        } catch {
                            await validator.invalidate()
                            throw error
                        }
                    }
                }
            }
        }
        #else
        let validator = DatabaseTransaction(storageAccess: transaction, container: self,
            readPolicy: DatabaseReadPolicy(schemaLease: sourceLease, authorization: authorization))
        do {
            try await validator.validateSQLCatalog(snapshot, workMeter: workMeter)
            try await validator.prepareForCommit()
        } catch {
            await validator.invalidate()
            throw error
        }
        #endif
    }
}
