import DatabaseKit
import DatabaseTypes
import DatabaseWire

/// One immutable reference owner, shared by generation leases and query frames.
final class SQLCatalogSnapshot: Sendable {
    let catalog: SQLCatalog
    let digest: ByteString
    let physicalSchemaFingerprint: SchemaFingerprint
    let protectedContracts: [SQLCatalogProtectedContract]
    let sqlOwnedEntityNames: [String]
    private let namespacesByName: [SQLCatalogName: SQLNamespace]
    private let relationsByNamespace: [SQLCatalogName: [SQLCatalogName: SQLCatalogResolvedRelation]]
    private let relationsByID: [SQLCatalogName: SQLCatalogResolvedRelation]
    private let physicalSchema: Schema
    private let sequenceDefaults: [SQLCatalogName: [SQLCatalogName: SQLSequenceDefaultBindings]]

    init(
        catalog: SQLCatalog,
        physicalSchema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        protectedContracts: [SQLCatalogProtectedContract] = [],
        limits: StorageFrameLimits = .default,
        sqlOwnedEntityNames: [String] = []
    ) throws {
        let sqlOwnedEntityNames = sqlOwnedEntityNames.sorted { $0.utf8.lexicographicallyPrecedes($1.utf8) }
        try Self.validateSQLOwnedEntities(sqlOwnedEntityNames, catalog: catalog,
            physicalSchema: physicalSchema, protectedContracts: protectedContracts)
        let catalog = try Self.normalizedCatalog(catalog, physicalSchema: physicalSchema)
        let fingerprint = try SchemaManifest(schema: physicalSchema).fingerprint()
        // Admit declared footprint before constructing lookup tables or mappings.
        try SQLCatalogCodec.admit(catalog, fingerprint: fingerprint,
            protectedContracts: protectedContracts, sqlOwnedEntityNames: sqlOwnedEntityNames, limits: limits,
            runtimeConfiguration: runtimeConfiguration)
        var entities: [SQLCatalogName: Schema.Entity] = [:]
        for entity in physicalSchema.entities {
            entities[SQLCatalogName(entity.name)] = entity
        }
        var namespaces: [SQLCatalogName: SQLNamespace] = [:]
        for namespace in catalog.namespaces {
            namespaces[SQLCatalogName(namespace.name)] = namespace
        }
        var byNamespace: [SQLCatalogName: [SQLCatalogName: SQLCatalogResolvedRelation]] = [:]
        var byID: [SQLCatalogName: SQLCatalogResolvedRelation] = [:]
        var protectionByID: [SQLCatalogName: SQLCatalogProtectedContract] = [:]
        for protection in protectedContracts {
            let key = SQLCatalogName(protection.relationID)
            guard protection.entityDigest.count == SHA256Accumulator.digestByteCount,
                protectionByID[key] == nil else {
                throw SQLCatalogBindingError.invalidProtection(protection.relationID)
            }
            protectionByID[key] = protection
        }
        for relation in catalog.relations {
            guard let entity = entities[SQLCatalogName(relation.entityName)] else {
                throw SQLCatalogBindingError.missingEntity(relation.entityName)
            }
            guard let runtime = runtimeConfiguration.entityRuntimes.registration(named: entity.name),
                SQLCatalogName(runtime.entity.name) == SQLCatalogName(entity.name) else {
                throw SQLCatalogBindingError.missingRuntime(entity.name)
            }
            let entityDigest = try Self.entityDigest(entity, limits: limits)
            guard try Self.entityDigest(runtime.entity, limits: limits) == entityDigest else {
                throw SQLCatalogBindingError.runtimeContractMismatch(entity.name)
            }
            var columns: [SQLCatalogResolvedColumn] = []
            columns.reserveCapacity(relation.columns.count)
            let fieldsByNumber = entity.fieldMapByNumber
            for column in relation.columns {
                switch column.backing {
                case .field(let number):
                    guard let field = fieldsByNumber[number] else {
                        throw SQLCatalogBindingError.missingField(entity: entity.name, number: number)
                    }
                    columns.append(SQLCatalogResolvedColumn(declaration: column, field: field,
                        type: field.queryType ?? .primitive(field.type, isArray: field.isArray),
                        isNullable: field.isOptional))
                case .virtual(let type):
                    columns.append(SQLCatalogResolvedColumn(declaration: column, field: nil,
                        type: type, isNullable: true))
                }
            }
            let resolved = SQLCatalogResolvedRelation(declaration: relation, entity: entity, columns: columns)
            let key = SQLCatalogName(relation.id)
            byID[key] = resolved
            byNamespace[SQLCatalogName(relation.namespaceID), default: [:]][SQLCatalogName(relation.name)] = resolved
            if let protection = protectionByID[key] {
                guard SQLCatalogName(protection.entityName) == SQLCatalogName(entity.name),
                    protection.entityDigest == entityDigest else {
                    throw SQLCatalogBindingError.invalidProtection(relation.id)
                }
            }
            if let witness = runtime.compiledModelContract {
                guard try Self.entityDigest(witness, limits: limits) == entityDigest else {
                    throw SQLCatalogBindingError.runtimeContractMismatch(entity.name)
                }
                protectionByID[key] = SQLCatalogProtectedContract(relationID: relation.id,
                    entityName: entity.name, entityDigest: entityDigest)
            }
        }
        for protection in protectedContracts where byID[SQLCatalogName(protection.relationID)] == nil {
            throw SQLCatalogBindingError.protectedRelationRemoved(protection.relationID)
        }
        try SQLConstraintCompiler.validate(catalog: catalog, physicalSchema: physicalSchema)
        try SQLViews.validate(catalog: catalog, physicalSchema: physicalSchema)
        try SQLIndexCompiler.validate(catalog: catalog, physicalSchema: physicalSchema)
        var sequenceDefaults: [SQLCatalogName: [SQLCatalogName: SQLSequenceDefaultBindings]] = [:]
        for relation in catalog.relations {
            for column in relation.columns {
                if case .input(let expression) = column.policy, let expression {
                    sequenceDefaults[SQLCatalogName(relation.id), default: [:]][SQLCatalogName(column.id)] =
                        try SQLSequenceDefaultBindings.compile(expression, column: column,
                            relation: relation, catalog: catalog, physicalSchema: physicalSchema)
                }
            }
        }
        let protections = protectionByID.values.sorted {
            $0.relationID.utf8.lexicographicallyPrecedes($1.relationID.utf8)
        }
        try Self.validateSQLOwnedEntities(sqlOwnedEntityNames, catalog: catalog,
            physicalSchema: physicalSchema, protectedContracts: protections)
        try SQLCatalogCodec.admit(catalog, fingerprint: fingerprint,
            protectedContracts: protections, sqlOwnedEntityNames: sqlOwnedEntityNames, limits: limits)
        let bytes = try SQLCatalogCodec.canonicalBytes(catalog, fingerprint: fingerprint,
            protectedContracts: protections, sqlOwnedEntityNames: sqlOwnedEntityNames, limits: limits)
        var accumulator = SHA256Accumulator()
        accumulator.update(bytes)
        self.catalog = catalog
        self.digest = accumulator.finalize()
        self.physicalSchemaFingerprint = fingerprint
        self.protectedContracts = protections
        self.sqlOwnedEntityNames = sqlOwnedEntityNames
        self.namespacesByName = namespaces
        self.relationsByNamespace = byNamespace
        self.relationsByID = byID
        self.physicalSchema = physicalSchema
        self.sequenceDefaults = sequenceDefaults
    }

    func sequenceDefaultBindings(relationID: String, columnID: String) -> SQLSequenceDefaultBindings? {
        sequenceDefaults[SQLCatalogName(relationID)]?[SQLCatalogName(columnID)]
    }

    func resolveRelation(name: String, namespace: String?, searchPath: SQLSearchPath,
        currentUser: String?
    ) throws -> SQLCatalogResolvedRelation {
        if let namespace {
            guard let selected = namespacesByName[SQLCatalogName(namespace)] else {
                throw SQLCatalogBindingError.missingNamespace(namespace)
            }
            guard let relation = relationsByNamespace[SQLCatalogName(selected.id)]?[SQLCatalogName(name)] else {
                throw SQLCatalogBindingError.missingRelation(namespace: namespace, name: name)
            }
            return relation
        }
        for entry in searchPath.entries {
            let namespaceName: String
            switch entry {
            case .schema(let name): namespaceName = name
            case .currentUser:
                guard let currentUser else { continue }
                namespaceName = currentUser
            }
            guard let selected = namespacesByName[SQLCatalogName(namespaceName)] else { continue }
            if let relation = relationsByNamespace[SQLCatalogName(selected.id)]?[SQLCatalogName(name)] {
                return relation
            }
        }
        throw SQLCatalogBindingError.missingRelation(namespace: nil, name: name)
    }

    func replacingCatalog(_ catalog: SQLCatalog, physicalSchema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) throws -> SQLCatalogSnapshot {
        guard try SchemaManifest(schema: physicalSchema).fingerprint() == physicalSchemaFingerprint else {
            throw SQLCatalogBindingError.physicalFingerprintMismatch
        }
        return try replacingPhysicalCatalog(catalog, physicalSchema: physicalSchema,
            runtimeConfiguration: runtimeConfiguration)
    }

    func resolvedRelation(id: String) -> SQLCatalogResolvedRelation? {
        relationsByID[SQLCatalogName(id)]
    }

    /// SQL authority retains every persisted compiled-model witness unchanged.
    func replacingPhysicalCatalog(_ catalog: SQLCatalog, physicalSchema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration, sqlOwnedEntityNames: [String]? = nil
    ) throws -> SQLCatalogSnapshot {
        let candidate = try SQLCatalogSnapshot(catalog: catalog, physicalSchema: physicalSchema,
            runtimeConfiguration: runtimeConfiguration, protectedContracts: protectedContracts,
            sqlOwnedEntityNames: sqlOwnedEntityNames ?? self.sqlOwnedEntityNames)
        for old in self.catalog.relations {
            let key = SQLCatalogName(old.id)
            let isProtected = protectedContracts.contains { SQLCatalogName($0.relationID) == key }
            guard let replacement = candidate.relationsByID[key]?.declaration else {
                if isProtected { throw SQLCatalogBindingError.protectedRelationRemoved(old.id) }
                continue
            }
            guard SQLCatalogName(replacement.entityName) == SQLCatalogName(old.entityName) else {
                throw SQLCatalogBindingError.relationRetargeted(old.id)
            }
            guard replacement.fieldNumberHighWaterMark >= old.fieldNumberHighWaterMark else {
                throw DatabaseSchemaPublicationError.corruptedState("SQL field-number high-water mark cannot decrease")
            }
            let newColumns = Dictionary(uniqueKeysWithValues: replacement.columns.map { (SQLCatalogName($0.id), $0) })
            for column in old.columns {
                guard let newColumn = newColumns[SQLCatalogName(column.id)] else {
                    if isProtected {
                        throw SQLCatalogBindingError.protectedColumnRemoved(relation: old.id, column: column.id)
                    }
                    continue
                }
                if column.policy == newColumn.policy,
                    sequenceDefaultBindings(relationID: old.id, columnID: column.id)
                        != candidate.sequenceDefaultBindings(relationID: old.id, columnID: column.id) {
                    throw SQLConstraintError.invalidDeclaration(relation: old.id,
                        reason: "unchanged default cannot retarget a sequence identity")
                }
                guard newColumn.backing == column.backing else {
                    throw SQLCatalogBindingError.columnRetargeted(relation: old.id, column: column.id)
                }
            }
            if isProtected {
                guard Self.equalKey(old.primaryKeyColumnIDs, replacement.primaryKeyColumnIDs),
                    old.primaryKeyConstraint == replacement.primaryKeyConstraint else {
                    throw SQLCatalogBindingError.protectedPrimaryKeyChanged(old.id)
                }
                guard try SQLCatalogSemanticContract.constraints(old)
                    == SQLCatalogSemanticContract.constraints(replacement) else {
                    throw SQLCatalogBindingError.protectedConstraintsChanged(old.id)
                }
                for column in old.columns {
                    guard let newColumn = newColumns[SQLCatalogName(column.id)],
                        try SQLCatalogSemanticContract.policy(column.policy, relation: old)
                            == SQLCatalogSemanticContract.policy(newColumn.policy, relation: replacement) else {
                        throw SQLCatalogBindingError.protectedColumnPolicyChanged(relation: old.id, column: column.id)
                    }
                }
                for column in replacement.columns where !old.columns.contains(where: {
                    SQLCatalogName($0.id) == SQLCatalogName(column.id)
                }) {
                    guard case .virtual = column.backing else {
                        throw SQLCatalogBindingError.protectedColumnAdded(relation: old.id, column: column.id)
                    }
                }
            }
        }
        for sequence in self.catalog.sequences {
            let referencedByProtected = self.catalog.relations.contains { relation in
                protectedContracts.contains { SQLCatalogName($0.relationID) == SQLCatalogName(relation.id) }
                    && (relation.columns.contains { column in
                        if case .identity(let identity) = column.policy {
                            return SQLCatalogName(identity.sequenceID) == SQLCatalogName(sequence.id)
                        }
                        return false
                    } || sequence.ownedBy.map { SQLCatalogName($0.relationID) == SQLCatalogName(relation.id) } == true)
            }
            if referencedByProtected,
                !candidate.catalog.sequences.contains(where: {
                    SQLCatalogName($0.id) == SQLCatalogName(sequence.id)
                        && $0.declaredType == sequence.declaredType
                        && $0.start == sequence.start && $0.increment == sequence.increment
                        && $0.minimum == sequence.minimum && $0.maximum == sequence.maximum
                        && $0.cycle == sequence.cycle && $0.ownedBy == sequence.ownedBy
                }) {
                throw SQLCatalogBindingError.protectedSequenceChanged(sequence.id)
            }
        }
        return candidate
    }

    private static func normalizedCatalog(_ catalog: SQLCatalog, physicalSchema: Schema) throws -> SQLCatalog {
        var relations: [SQLRelation] = []
        for relation in catalog.relations {
            var columns: [SQLColumn] = []
            for column in relation.columns {
                let bindings: [SQLSequenceBinding]
                if case .input(let expression) = column.policy, let expression {
                    bindings = try SQLSequenceDefaultBindings.compile(expression, column: column,
                        relation: relation, catalog: catalog, physicalSchema: physicalSchema).declarations()
                } else { bindings = column.sequenceBindings }
                columns.append(try SQLColumn(id: column.id, name: column.name, backing: column.backing,
                    policy: column.policy, sequenceBindings: bindings))
            }
            relations.append(try SQLRelation(id: relation.id, namespaceID: relation.namespaceID,
                name: relation.name, entityName: relation.entityName, columns: columns,
                primaryKeyColumnIDs: relation.primaryKeyColumnIDs,
                primaryKeyConstraint: relation.primaryKeyConstraint, constraints: relation.constraints,
                fieldNumberHighWaterMark: relation.fieldNumberHighWaterMark))
        }
        return try SQLCatalog(namespaces: catalog.namespaces, relations: relations, sequences: catalog.sequences,
            views: catalog.views, materializedViews: catalog.materializedViews, indexes: catalog.indexes)
    }

    static func derivedCatalog(physicalSchema: Schema) throws -> SQLCatalog {
        let namespace = try SQLNamespace(id: "public", name: "public")
        let relations = try physicalSchema.entities.map { try derivedRelation($0, namespaceID: namespace.id) }
        return try SQLCatalog(namespaces: [namespace], relations: relations)
    }

    func reconciledForApplication(physicalSchema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration
    ) throws -> SQLCatalogSnapshot {
        try SQLCatalogStoredState(catalog: catalog,
            physicalSchemaFingerprint: physicalSchemaFingerprint,
            protectedContracts: protectedContracts, digest: digest, sqlOwnedEntityNames: sqlOwnedEntityNames)
            .reconciledForApplication(physicalSchema: self.physicalSchema,
                targetPhysicalSchema: physicalSchema, runtimeConfiguration: runtimeConfiguration)
    }

    static func derivedRelation(_ entity: Schema.Entity, namespaceID: String) throws -> SQLRelation {
        let columns = try entity.fields.map {
            try SQLColumn(id: "field:\($0.fieldNumber)", name: $0.name, fieldNumber: $0.fieldNumber)
        }
        let id = columns.first { SQLCatalogName($0.name) == SQLCatalogName("id") }
        return try SQLRelation(id: "entity:\(entity.name)", namespaceID: namespaceID, name: entity.name,
            entityName: entity.name, columns: columns, primaryKeyColumnIDs: id.map { [$0.id] })
    }

    static func availableID(_ proposed: String, used: [String]) -> String {
        let keys = Set(used.map(SQLCatalogName.init))
        var result = proposed
        while keys.contains(SQLCatalogName(result)) { result += ":new" }
        return result
    }

    private static func equalKey(_ lhs: [String]?, _ rhs: [String]?) -> Bool {
        switch (lhs, rhs) {
        case (nil, nil): return true
        case (.some(let lhs), .some(let rhs)):
            return lhs.count == rhs.count && zip(lhs, rhs).allSatisfy {
                SQLCatalogName($0.0) == SQLCatalogName($0.1)
            }
        default: return false
        }
    }

    static func validateSQLOwnedEntities(_ names: [String], catalog: SQLCatalog,
        physicalSchema: Schema? = nil, protectedContracts: [SQLCatalogProtectedContract]
    ) throws {
        var seen: Set<SQLCatalogName> = []
        for name in names {
            let key = SQLCatalogName(name)
            guard !name.isEmpty, !name.utf8.contains(0), seen.insert(key).inserted,
                catalog.relations.contains(where: { SQLCatalogName($0.entityName) == key }),
                physicalSchema.map({ $0.entities.contains { SQLCatalogName($0.name) == key } }) ?? true,
                !protectedContracts.contains(where: { SQLCatalogName($0.entityName) == key }) else {
                throw SQLCatalogBindingError.invalidSQLOwnership(name)
            }
        }
    }

    static func entityDigest(_ entity: Schema.Entity, limits: StorageFrameLimits) throws -> ByteString {
        let bytes = try SchemaEntityEntryCodec.encode(entity, limits: limits)
        var accumulator = SHA256Accumulator()
        accumulator.update(bytes)
        return accumulator.finalize()
    }
}
