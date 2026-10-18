import DatabaseKit
import DatabaseTypes
import DatabaseWire

struct SQLCatalogStoredState: Sendable {
    let catalog: SQLCatalog
    let physicalSchemaFingerprint: SchemaFingerprint
    let protectedContracts: [SQLCatalogProtectedContract]
    let digest: ByteString
    let sqlOwnedEntityNames: [String]

    init(catalog: SQLCatalog, physicalSchemaFingerprint: SchemaFingerprint,
        protectedContracts: [SQLCatalogProtectedContract], digest: ByteString,
        sqlOwnedEntityNames: [String] = []
    ) {
        self.catalog = catalog
        self.physicalSchemaFingerprint = physicalSchemaFingerprint
        self.protectedContracts = protectedContracts
        self.digest = digest
        self.sqlOwnedEntityNames = sqlOwnedEntityNames
    }

    /// Composes persisted SQL entities with the current application declaration.
    func applicationSchema(physicalSchema: Schema, declaredSchema: Schema) throws -> Schema {
        try validate(physicalSchema: physicalSchema)
        var entities = declaredSchema.entities
        for name in sqlOwnedEntityNames {
            guard let stored = physicalSchema.entities.first(where: { SQLCatalogName($0.name) == SQLCatalogName(name) }) else {
                throw SQLCatalogBindingError.invalidSQLOwnership(name)
            }
            if let declared = declaredSchema.entity(named: stored.name) {
                guard declared == stored else { throw SQLCatalogBindingError.runtimeContractMismatch(name) }
            } else { entities.append(stored) }
        }
        return try Schema(entities: entities, version: declaredSchema.version)
    }

    func snapshot(physicalSchema: Schema, runtimeConfiguration: DatabaseRuntimeConfiguration,
        limits: StorageFrameLimits = .default
    ) throws -> SQLCatalogSnapshot {
        try validate(physicalSchema: physicalSchema, limits: limits)
        let snapshot = try SQLCatalogSnapshot(catalog: catalog, physicalSchema: physicalSchema,
            runtimeConfiguration: runtimeConfiguration, protectedContracts: protectedContracts, limits: limits,
            sqlOwnedEntityNames: sqlOwnedEntityNames)
        guard snapshot.digest == digest else { throw SQLCatalogBindingError.digestMismatch }
        return snapshot
    }

    /// Validates persisted metadata without inventing an executable old runtime.
    func reconciledForApplication(physicalSchema: Schema, targetPhysicalSchema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration, candidate: SQLCatalog? = nil
    ) throws -> SQLCatalogSnapshot {
        try validate(physicalSchema: physicalSchema)
        let target = try candidate ?? derivedApplicationCatalog(physicalSchema: physicalSchema,
            targetPhysicalSchema: targetPhysicalSchema)
        try SQLCatalogCodec.admit(target,
            fingerprint: SchemaManifest(schema: targetPhysicalSchema).fingerprint(),
            protectedContracts: protectedContracts, sqlOwnedEntityNames: sqlOwnedEntityNames, limits: .default,
            runtimeConfiguration: runtimeConfiguration)
        let targetRelations = Dictionary(uniqueKeysWithValues: target.relations.map {
            (SQLCatalogName($0.id), $0)
        })
        let targetEntities = Dictionary(uniqueKeysWithValues: targetPhysicalSchema.entities.map {
            (SQLCatalogName($0.name), $0)
        })
        let protectedIDs = Set(protectedContracts.map { SQLCatalogName($0.relationID) })
        for old in catalog.relations {
            let id = SQLCatalogName(old.id)
            guard let relation = targetRelations[id] else {
                if protectedIDs.contains(id), targetEntities[SQLCatalogName(old.entityName)] != nil {
                    throw SQLCatalogBindingError.protectedRelationRemoved(old.id)
                }
                continue
            }
            guard SQLCatalogName(relation.entityName) == SQLCatalogName(old.entityName) else {
                throw SQLCatalogBindingError.relationRetargeted(old.id)
            }
            let newColumns = Dictionary(uniqueKeysWithValues: relation.columns.map {
                (SQLCatalogName($0.id), $0)
            })
            for column in old.columns {
                if let newColumn = newColumns[SQLCatalogName(column.id)] {
                    guard newColumn.backing == column.backing else {
                        throw SQLCatalogBindingError.columnRetargeted(relation: old.id, column: column.id)
                    }
                } else if protectedIDs.contains(id) {
                    if let number = column.fieldNumber,
                        targetEntities[SQLCatalogName(old.entityName)]?.fieldMapByNumber[number] == nil {
                        continue
                    }
                    throw SQLCatalogBindingError.protectedColumnRemoved(relation: old.id, column: column.id)
                }
            }
        }
        var protections: [SQLCatalogProtectedContract] = []
        for protection in protectedContracts {
            guard let relation = targetRelations[SQLCatalogName(protection.relationID)] else { continue }
            if runtimeConfiguration.entityRuntimes.registration(named: relation.entityName)?.compiledModelContract == nil {
                protections.append(protection)
            }
        }
        return try SQLCatalogSnapshot(catalog: target, physicalSchema: targetPhysicalSchema,
            runtimeConfiguration: runtimeConfiguration, protectedContracts: protections,
            sqlOwnedEntityNames: sqlOwnedEntityNames)
    }

    func validate(physicalSchema: Schema,
        limits: StorageFrameLimits = .default
    ) throws {
        guard try SchemaManifest(schema: physicalSchema).fingerprint() == physicalSchemaFingerprint else {
            throw SQLCatalogBindingError.physicalFingerprintMismatch
        }
        try SQLCatalogCodec.admit(catalog, fingerprint: physicalSchemaFingerprint,
            protectedContracts: protectedContracts, sqlOwnedEntityNames: sqlOwnedEntityNames, limits: limits)
        let bytes = try SQLCatalogCodec.canonicalBytes(catalog, fingerprint: physicalSchemaFingerprint,
            protectedContracts: protectedContracts, sqlOwnedEntityNames: sqlOwnedEntityNames, limits: limits)
        var accumulator = SHA256Accumulator()
        accumulator.update(bytes)
        guard digest == accumulator.finalize() else { throw SQLCatalogBindingError.digestMismatch }
        let entities = Dictionary(uniqueKeysWithValues: physicalSchema.entities.map {
            (SQLCatalogName($0.name), $0)
        })
        let relations = Dictionary(uniqueKeysWithValues: catalog.relations.map {
            (SQLCatalogName($0.id), $0)
        })
        for relation in catalog.relations {
            guard let entity = entities[SQLCatalogName(relation.entityName)] else {
                throw SQLCatalogBindingError.missingEntity(relation.entityName)
            }
            for column in relation.columns {
                if let number = column.fieldNumber, entity.fieldMapByNumber[number] == nil {
                    throw SQLCatalogBindingError.missingField(entity: entity.name, number: number)
                }
            }
        }
        try SQLCatalogSnapshot.validateSQLOwnedEntities(sqlOwnedEntityNames, catalog: catalog,
            physicalSchema: physicalSchema, protectedContracts: protectedContracts)
        try SQLViews.validate(catalog: catalog, physicalSchema: physicalSchema)
        try SQLIndexCompiler.validate(catalog: catalog, physicalSchema: physicalSchema)
        var protectedIDs: Set<SQLCatalogName> = []
        for protection in protectedContracts {
            let id = SQLCatalogName(protection.relationID)
            guard protectedIDs.insert(id).inserted, let relation = relations[id],
                SQLCatalogName(relation.entityName) == SQLCatalogName(protection.entityName),
                let entity = entities[SQLCatalogName(protection.entityName)],
                try SQLCatalogSnapshot.entityDigest(entity, limits: limits) == protection.entityDigest else {
                throw SQLCatalogBindingError.invalidProtection(protection.relationID)
            }
        }
    }

    /// Persists an admitted migration stage without an executable stage runtime.
    func reconciledForMigration(physicalSchema: Schema,
        targetPhysicalSchema: Schema
    ) throws -> SQLCatalogStoredState {
        try validate(physicalSchema: physicalSchema)
        let target = try derivedApplicationCatalog(physicalSchema: physicalSchema,
            targetPhysicalSchema: targetPhysicalSchema)
        let relations = Dictionary(uniqueKeysWithValues: target.relations.map {
            (SQLCatalogName($0.id), $0)
        })
        let entities = Dictionary(uniqueKeysWithValues: targetPhysicalSchema.entities.map {
            (SQLCatalogName($0.name), $0)
        })
        var protections: [SQLCatalogProtectedContract] = []
        for protection in protectedContracts {
            guard let relation = relations[SQLCatalogName(protection.relationID)] else { continue }
            guard let entity = entities[SQLCatalogName(relation.entityName)],
                SQLCatalogName(protection.entityName) == SQLCatalogName(entity.name) else {
                throw SQLCatalogBindingError.invalidProtection(protection.relationID)
            }
            protections.append(try SQLCatalogProtectedContract(relationID: protection.relationID,
                entityName: protection.entityName,
                entityDigest: SQLCatalogSnapshot.entityDigest(entity, limits: .default),
                source: protection.source))
        }
        return try Self.migrationState(catalog: target, physicalSchema: targetPhysicalSchema,
            protectedContracts: protections, sqlOwnedEntityNames: sqlOwnedEntityNames)
    }

    /// The aggregate owner calls this only after proving complete initial absence.
    static func initialForMigration(physicalSchema: Schema) throws -> SQLCatalogStoredState {
        try migrationState(catalog: SQLCatalogSnapshot.derivedCatalog(physicalSchema: physicalSchema),
            physicalSchema: physicalSchema, protectedContracts: [])
    }

    private static func migrationState(catalog: SQLCatalog, physicalSchema: Schema,
        protectedContracts: [SQLCatalogProtectedContract], sqlOwnedEntityNames: [String] = []
    ) throws -> SQLCatalogStoredState {
        let fingerprint = try SchemaManifest(schema: physicalSchema).fingerprint()
        try SQLCatalogCodec.admit(catalog, fingerprint: fingerprint,
            protectedContracts: protectedContracts, sqlOwnedEntityNames: sqlOwnedEntityNames, limits: .default)
        try SQLConstraintCompiler.validate(catalog: catalog, physicalSchema: physicalSchema)
        let bytes = try SQLCatalogCodec.canonicalBytes(catalog, fingerprint: fingerprint,
            protectedContracts: protectedContracts, sqlOwnedEntityNames: sqlOwnedEntityNames)
        var accumulator = SHA256Accumulator()
        accumulator.update(bytes)
        return SQLCatalogStoredState(catalog: catalog, physicalSchemaFingerprint: fingerprint,
            protectedContracts: protectedContracts, digest: accumulator.finalize(),
            sqlOwnedEntityNames: sqlOwnedEntityNames)
    }

    private func derivedApplicationCatalog(physicalSchema: Schema,
        targetPhysicalSchema: Schema
    ) throws -> SQLCatalog {
        var namespaces = catalog.namespaces
        let previousEntities = Dictionary(uniqueKeysWithValues: physicalSchema.entities.map {
            (SQLCatalogName($0.name), $0)
        })
        let targetEntities = Dictionary(uniqueKeysWithValues: targetPhysicalSchema.entities.map {
            (SQLCatalogName($0.name), $0)
        })
        var relations: [SQLRelation] = []
        let sqlOwned = Set(sqlOwnedEntityNames.map(SQLCatalogName.init))
        for old in catalog.relations {
            if sqlOwned.contains(SQLCatalogName(old.entityName)) {
                guard let original = previousEntities[SQLCatalogName(old.entityName)],
                    let target = targetEntities[SQLCatalogName(old.entityName)], target == original else {
                    throw SQLCatalogBindingError.invalidSQLOwnership(old.entityName)
                }
                relations.append(old)
                continue
            }
            guard let entity = targetEntities[SQLCatalogName(old.entityName)] else { continue }
            let previousFields = previousEntities[SQLCatalogName(old.entityName)]?.fields ?? []
            let numbers = Set(entity.fields.map { $0.fieldNumber })
            var columns = old.columns.filter { column in
                guard let number = column.fieldNumber else { return true }
                return numbers.contains(number)
            }
            if let key = old.primaryKeyColumnIDs {
                for id in key where !columns.contains(where: { SQLCatalogName($0.id) == SQLCatalogName(id) }) {
                    throw SQLCatalogBindingError.primaryKeyColumnRemoved(relation: old.id, column: id)
                }
            }
            let previousNumbers = Set(previousFields.map { $0.fieldNumber })
            for field in entity.fields where !previousNumbers.contains(field.fieldNumber) {
                columns.append(try SQLColumn(id: SQLCatalogSnapshot.availableID("field:\(field.fieldNumber)", used: columns.map { $0.id }),
                    name: field.name, fieldNumber: field.fieldNumber))
            }
            relations.append(try SQLRelation(id: old.id, namespaceID: old.namespaceID, name: old.name,
                entityName: old.entityName, columns: columns, primaryKeyColumnIDs: old.primaryKeyColumnIDs,
                primaryKeyConstraint: old.primaryKeyConstraint, constraints: old.constraints,
                fieldNumberHighWaterMark: max(old.fieldNumberHighWaterMark, columns.compactMap { $0.fieldNumber }.max() ?? 0)))
        }
        for entity in targetPhysicalSchema.entities where previousEntities[SQLCatalogName(entity.name)] == nil {
            let publicNamespace: SQLNamespace
            if let existing = namespaces.first(where: { SQLCatalogName($0.name) == SQLCatalogName("public") }) {
                publicNamespace = existing
            } else {
                publicNamespace = try SQLNamespace(id: SQLCatalogSnapshot.availableID("public", used: namespaces.map { $0.id }), name: "public")
                namespaces.append(publicNamespace)
            }
            let derived = try SQLCatalogSnapshot.derivedRelation(entity, namespaceID: publicNamespace.id)
            relations.append(try SQLRelation(id: SQLCatalogSnapshot.availableID(derived.id, used: relations.map { $0.id }),
                namespaceID: derived.namespaceID, name: derived.name, entityName: derived.entityName,
                columns: derived.columns, primaryKeyColumnIDs: derived.primaryKeyColumnIDs,
                primaryKeyConstraint: derived.primaryKeyConstraint, constraints: derived.constraints))
        }
        return try SQLCatalog(namespaces: namespaces, relations: relations, sequences: catalog.sequences,
            views: catalog.views, materializedViews: catalog.materializedViews, indexes: catalog.indexes)
    }
}
