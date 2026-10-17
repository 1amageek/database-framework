import DatabaseKit
import DatabaseTypes
import DatabaseWire

/// One immutable reference owner, shared by generation leases and query frames.
final class SQLCatalogSnapshot: Sendable {
    let catalog: SQLCatalog
    let digest: ByteString
    let physicalSchemaFingerprint: SchemaFingerprint
    let protectedContracts: [SQLCatalogProtectedContract]
    private let namespacesByName: [SQLCatalogName: SQLNamespace]
    private let relationsByNamespace: [SQLCatalogName: [SQLCatalogName: SQLCatalogResolvedRelation]]
    private let relationsByID: [SQLCatalogName: SQLCatalogResolvedRelation]
    private let physicalSchema: Schema

    init(
        catalog: SQLCatalog,
        physicalSchema: Schema,
        runtimeConfiguration: DatabaseRuntimeConfiguration,
        protectedContracts: [SQLCatalogProtectedContract] = [],
        limits: StorageFrameLimits = .default
    ) throws {
        let fingerprint = try SchemaManifest(schema: physicalSchema).fingerprint()
        // Admit declared footprint before constructing lookup tables or mappings.
        try SQLCatalogCodec.admit(catalog, fingerprint: fingerprint,
            protectedContracts: protectedContracts, limits: limits,
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
                guard let field = fieldsByNumber[column.fieldNumber] else {
                    throw SQLCatalogBindingError.missingField(entity: entity.name, number: column.fieldNumber)
                }
                columns.append(SQLCatalogResolvedColumn(declaration: column, field: field))
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
        let protections = protectionByID.values.sorted {
            $0.relationID.utf8.lexicographicallyPrecedes($1.relationID.utf8)
        }
        try SQLCatalogCodec.admit(catalog, fingerprint: fingerprint,
            protectedContracts: protections, limits: limits)
        let bytes = try SQLCatalogCodec.canonicalBytes(catalog, fingerprint: fingerprint,
            protectedContracts: protections, limits: limits)
        var accumulator = SHA256Accumulator()
        accumulator.update(bytes)
        self.catalog = catalog
        self.digest = accumulator.finalize()
        self.physicalSchemaFingerprint = fingerprint
        self.protectedContracts = protections
        self.namespacesByName = namespaces
        self.relationsByNamespace = byNamespace
        self.relationsByID = byID
        self.physicalSchema = physicalSchema
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
        let candidate = try SQLCatalogSnapshot(catalog: catalog, physicalSchema: physicalSchema,
            runtimeConfiguration: runtimeConfiguration, protectedContracts: protectedContracts)
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
            let newColumns = Dictionary(uniqueKeysWithValues: replacement.columns.map { (SQLCatalogName($0.id), $0) })
            for column in old.columns {
                guard let newColumn = newColumns[SQLCatalogName(column.id)] else {
                    if isProtected {
                        throw SQLCatalogBindingError.protectedColumnRemoved(relation: old.id, column: column.id)
                    }
                    continue
                }
                guard newColumn.fieldNumber == column.fieldNumber else {
                    throw SQLCatalogBindingError.columnRetargeted(relation: old.id, column: column.id)
                }
            }
            if isProtected, !Self.equalKey(old.primaryKeyColumnIDs, replacement.primaryKeyColumnIDs) {
                throw SQLCatalogBindingError.protectedPrimaryKeyChanged(old.id)
            }
        }
        return candidate
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
            protectedContracts: protectedContracts, digest: digest)
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

    static func entityDigest(_ entity: Schema.Entity, limits: StorageFrameLimits) throws -> ByteString {
        let bytes = try SchemaEntityEntryCodec.encode(entity, limits: limits)
        var accumulator = SHA256Accumulator()
        accumulator.update(bytes)
        return accumulator.finalize()
    }
}
