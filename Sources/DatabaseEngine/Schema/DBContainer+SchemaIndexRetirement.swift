import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit

/// The captured source Directory contract of one durable retirement marker.
private struct IndexRetirementScopeFrame: StorageFrameValue {
    private static let version: UInt8 = 3
    let scope: DatabaseIndexStorageScope
    let directoryLayers: [DirectoryLayer]?

    init(_ retirement: DatabasePendingIndexRetirement) {
        scope = retirement.scope
        directoryLayers = retirement.directoryLayers
    }

    func encode(to encoder: inout StorageFrameEncoder) throws(StorageFrameError) {
        encoder.writeUInt8(Self.version)
        let bytes: ByteString
        do { bytes = try DatabaseIndexStorageScopeCodec.encode(scope, limits: encoder.limits) }
        catch let error as StorageFrameError { throw error }
        catch { throw .invalidValue }
        try encoder.writeBytes(bytes)
        encoder.writeBool(directoryLayers != nil)
        if let directoryLayers {
            try encoder.writeCount(directoryLayers.count)
            for layer in directoryLayers { encoder.writeUInt8(Self.image(of: layer)) }
        }
    }

    init(from decoder: inout StorageFrameDecoder) throws(StorageFrameError) {
        guard try decoder.readUInt8() == Self.version else { throw .invalidValue }
        do { scope = try DatabaseIndexStorageScopeCodec.decode(decoder.readBytes(), limits: decoder.limits) }
        catch let error as StorageFrameError { throw error }
        catch { throw .invalidValue }
        if try decoder.readBool() {
            let expected: Int
            switch scope {
            case .entity(_, let components, _): expected = components.count
            case .polymorphicGroup(_, let path): expected = path.count
            }
            guard try decoder.readCount() == expected else { throw .invalidValue }
            var layers: [DirectoryLayer] = []
            layers.reserveCapacity(expected)
            for _ in 0..<expected {
                guard let layer = Self.layer(ofImage: try decoder.readUInt8()) else { throw .invalidValue }
                layers.append(layer)
            }
            directoryLayers = layers
        } else { directoryLayers = nil }
    }

    private static func image(of layer: DirectoryLayer) -> UInt8 {
        switch layer {
        case .default: 0
        case .partition: 1
        }
    }

    private static func layer(ofImage image: UInt8) -> DirectoryLayer? {
        switch image {
        case 0: .default
        case 1: .partition
        default: nil
        }
    }
}

extension DBContainer {
    /// Reconciles and persists exact index generations that remain to be
    /// retired for this data root.
    ///
    /// Existing work is retained across host-job replacement. A generation
    /// active in the new target schema is removed from the cleanup set, which
    /// makes an explicit schema reversal safe.
    @_spi(DatabaseExecution)
    public func stageSchemaIndexRetirements(
        _ additions: [DatabasePendingIndexRetirement],
        validFor target: Schema,
        indexPhysicalLayouts: [String: IndexPhysicalLayout],
        transaction: any TransactionAccess
    ) async throws {
        for addition in additions {
            try validateSchemaIndexRetirement(addition)
        }
        let storage = try schemaIndexRetirementSubspace()
        // The additions were planned against the published generation, which
        // is still the source here because staging precedes publication. That
        // generation is the only one that can type the paths being retired.
        let source = acquirePublishedSchemaLease()
        // Identity ignores the recorded layers, so a marker already staged is
        // the same work as an addition repeating it. The record that carries
        // layers wins, and an already recorded vector is kept: it was derived
        // from the generation that created the storage, which is at least as
        // close to it as the generation staging now.
        var pending: [DatabasePendingIndexRetirement: DatabasePendingIndexRetirement] = [:]
        for retirement in try await loadSchemaIndexRetirements(
            transaction: transaction
        ) {
            pending[retirement] = retirement
        }
        for addition in additions {
            guard pending[addition]?.directoryLayers == nil else { continue }
            pending[addition] = addition.recording(
                directoryLayers: try Self.declaredRetirementLayers(
                    for: addition.scope,
                    in: source
                )
            )
        }
        let retained = try pending.values.filter { retirement in
            try activeIndexIdentity(
                matching: retirement,
                in: target,
                indexPhysicalLayouts: indexPhysicalLayouts
            ) != ActiveIndexIdentity(retirement)
        }.sorted(by: Self.indexRetirementLessThan)

        let range = storage.range()
        try transaction.clearRange(beginKey: range.begin, endKey: range.end)
        for retirement in retained {
            try validateSchemaIndexRetirement(retirement)
            try transaction.setValue(
                try StorageFrameCodec.encode(
                    IndexRetirementScopeFrame(retirement)
                ),
                for: Self.schemaIndexRetirementKey(
                    retirement,
                    storage: storage
                )
            )
        }
    }

    /// Returns canonical pending cleanup work after excluding the generation
    /// selected by the supplied target schema.
    @_spi(DatabaseExecution)
    public func pendingSchemaIndexRetirements(
        validFor target: Schema,
        transaction: any TransactionAccess
    ) async throws -> [DatabasePendingIndexRetirement] {
        let lease = acquireActiveSchemaLease()
        guard lease.schema == target else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "schema retirement target does not match the active generation"
            )
        }
        return try await loadSchemaIndexRetirements(transaction: transaction)
            .filter { retirement in
                try activeIndexIdentity(
                    matching: retirement,
                    in: target,
                    indexPhysicalLayouts: lease.indexPhysicalLayouts
                ) != ActiveIndexIdentity(retirement)
            }
            .sorted(by: Self.indexRetirementLessThan)
    }

    /// Removes one durable cleanup marker in the same transaction that retired
    /// its physical generation.
    @_spi(DatabaseExecution)
    public func completeSchemaIndexRetirement(
        _ retirement: DatabasePendingIndexRetirement,
        transaction: any TransactionAccess
    ) throws {
        try transaction.clear(
            key: Self.schemaIndexRetirementKey(
                retirement,
                storage: try schemaIndexRetirementSubspace()
            )
        )
    }

    private func loadSchemaIndexRetirements(
        transaction: any TransactionAccess
    ) async throws -> [DatabasePendingIndexRetirement] {
        let storage = try schemaIndexRetirementSubspace()
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
                "schema index retirement marker count exceeds the wire limit"
            )
        }

        var retirements: [DatabasePendingIndexRetirement] = []
        retirements.reserveCapacity(rows.count)
        for (key, value) in rows {
            let tuple = try storage.unpack(key)
            guard tuple.count == 4,
                  case .string(let scopeKey) = try tuple.value(at: 0),
                  case .string(let index) = try tuple.value(at: 1),
                  case .bytes(let fingerprintBytes) = try tuple.value(at: 2),
                  case .bytes(let layoutFingerprint) = try tuple.value(at: 3),
                  layoutFingerprint.count == SHA256Accumulator.digestByteCount,
                  !scopeKey.isEmpty,
                  !index.isEmpty else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema index retirement marker cannot be decoded"
                )
            }
            let frame: IndexRetirementScopeFrame
            do {
                frame = try StorageFrameCodec.decode(
                    IndexRetirementScopeFrame.self,
                    from: value
                )
            } catch {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema index retirement scope cannot be decoded"
                )
            }
            let scope = frame.scope
            guard scope.stableOrderingKey == scopeKey else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema index retirement scope is invalid"
                )
            }
            let fingerprint: SchemaFingerprint
            do {
                fingerprint = try SchemaFingerprint(fingerprintBytes)
            } catch {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema index retirement fingerprint is invalid"
                )
            }
            let identity: DatabaseIndexStorageIdentity
            do {
                identity = try DatabaseIndexStorageIdentity(
                    name: index,
                    definitionFingerprint: fingerprint,
                    layoutFingerprint: layoutFingerprint
                )
            } catch {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema index retirement identity is invalid"
                )
            }
            let retirement = DatabasePendingIndexRetirement(
                scope: scope,
                identity: identity,
                directoryLayers: frame.directoryLayers
            )
            try validateSchemaIndexRetirement(retirement)
            retirements.append(retirement)
        }
        guard Set(retirements).count == retirements.count else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "schema index retirement markers contain duplicates"
            )
        }
        return retirements
    }

    private func validateSchemaIndexRetirement(
        _ retirement: DatabasePendingIndexRetirement
    ) throws {
        do {
            try retirement.scope.validate()
        } catch {
            throw DatabaseSchemaPublicationError.corruptedState(
                "schema index retirement scope is invalid"
            )
        }
    }

    /// Enumerates the captured source declaration after its entity is removed.
    @_spi(DatabaseExecution)
    public func executionRetirementPartitionCatalogPage(
        _ retirement: DatabasePendingIndexRetirement,
        continuation: ByteString? = nil,
        limit: Int,
        transaction: any TransactionAccess
    ) async throws -> DatabasePartitionCatalogPage {
        try validateSchemaIndexRetirement(retirement)
        guard case .entity(let name, let components, let fields) = retirement.scope,
              let layers = retirement.directoryLayers,
              layers.count == components.count else {
            throw DatabaseSchemaPublicationError.corruptedState("retirement source Directory metadata is incomplete")
        }
        return try await DatabaseDirectoryPartitionEnumerator.page(
            entityName: name, components: components, fieldSchemas: fields.map { $0.schema }, layers: layers,
            below: operationDataDirectory(), access: operationDirectoryAccess(),
            continuation: continuation, limit: limit, transaction: transaction)
    }

    /// Retires the exact generation using the source directory contract stored
    /// with the durable marker. The published target schema is intentionally
    /// not consulted because the source entity or group may have been removed;
    /// the marker carries the layer of each component of its own path, so the
    /// address is still verified against the declaration that issued it.
    @_spi(DatabaseExecution)
    public func retireSchemaIndexStorage(
        _ retirement: DatabasePendingIndexRetirement,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) async throws {
        try validateSchemaIndexRetirement(retirement)
        let path: [String]
        switch retirement.scope {
        case .entity(_, let components, let fields):
            path = try Self.resolveRetirementDirectory(
                components,
                fields: fields,
                partitions: partitions
            )
        case .polymorphicGroup(_, let directoryPath):
            guard partitions.isEmpty else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "polymorphic retirement cannot contain partitions"
                )
            }
            path = directoryPath
        }
        let selection = DatabaseIndexStorageRetirement.physicalGeneration(
            definitionFingerprint: retirement.identity.definitionFingerprint,
            layoutFingerprint: retirement.identity.layoutFingerprint
        )
        let subspace: Subspace?
        if let layers = retirement.directoryLayers {
            guard layers.count == path.count else {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema index retirement layers do not describe its path"
                )
            }
            // A node recreated under another layer since this work was staged
            // holds storage this record never described, so the mismatch the
            // Directory reports must stop the retirement rather than clear it.
            subspace = try await openDataDirectory(
                relativePath: path,
                layers: layers,
                transaction: transaction
            )
        } else {
            // An undeclared scope had no source layer at staging. Existing
            // static-path cleanup uses the stored node tag for addressing.
            subspace = try await openUnverifiedDataDirectory(
                relativePath: path,
                transaction: transaction
            )
        }
        // A directory that no longer exists holds no storage to retire.
        if let subspace {
            try IndexStorageRetirer.retire(
                indexName: retirement.identity.name,
                selection: selection,
                storeSubspace: subspace,
                transaction: transaction
            )
        }
        try clearSchemaIndexBuildPending(
            scope: retirement.scope,
            index: retirement.identity.name,
            selection: selection,
            transaction: transaction
        )
    }

    /// The layer of each component of a retiring path, taken from the schema
    /// generation the retirement was planned against.
    ///
    /// Only a declaration that still matches the recorded scope exactly types
    /// it: the same name over different components describes a different
    /// position, and a generation that no longer declares it types nothing. A
    /// path this cannot type is recorded without layers rather than with an
    /// invented contract, which leaves the stored tag authoritative for
    /// addressing exactly as it was before layers were recorded.
    private static func declaredRetirementLayers(
        for scope: DatabaseIndexStorageScope,
        in lease: DatabaseSchemaLease
    ) throws -> [DirectoryLayer]? {
        switch scope {
        case .entity(let name, _, _):
            guard let entity = lease.schema.entity(named: name),
                  DatabaseIndexStorageScope.entity(entity) == scope else {
                return nil
            }
            return try DBContainer.declaredLayers(
                for: entity,
                in: lease.directoryLayers
            )
        case .polymorphicGroup(let identifier, let directoryPath):
            guard let group = lease.schema.polymorphicGroup(
                identifier: identifier
            ), try group.resolvedDirectoryPath() == directoryPath else {
                return nil
            }
            return lease.directoryLayers.layers(forPath: directoryPath)
        }
    }

    private func activeIndexIdentity(
        matching retirement: DatabasePendingIndexRetirement,
        in target: Schema,
        indexPhysicalLayouts: [String: IndexPhysicalLayout]
    ) throws -> ActiveIndexIdentity? {
        let index = retirement.identity.name
        let isDeclared: Bool
        switch retirement.scope {
        case .entity(let name, _, _):
            isDeclared = target.entity(named: name).map { entity in
                DatabaseIndexStorageScope.entity(entity) == retirement.scope
                    && entity.indexDescriptors.contains { $0.name == index }
            } ?? false
        case .polymorphicGroup(let identifier, let directoryPath):
            if let group = target.polymorphicGroup(identifier: identifier) {
                isDeclared = try group.resolvedDirectoryPath() == directoryPath
                    && group.indexes.contains { $0.name == index }
            } else {
                isDeclared = false
            }
        }
        guard isDeclared else { return nil }
        guard let layout = indexPhysicalLayouts[index] else {
            throw DatabaseIndexStorageIdentityError
                .physicalLayoutNotResolved(index)
        }
        return ActiveIndexIdentity(
            definitionFingerprint: try DatabaseIndexStorageIdentity
                .definitionFingerprint(named: index, in: target),
            layoutFingerprint: layout.fingerprint
        )
    }

    private func schemaIndexRetirementSubspace() throws -> Subspace {
        try dataRootSchemaMetadataSubspace()
            .subspace("schema")
            .subspace("index-retirement")
    }

    private static func schemaIndexRetirementKey(
        _ retirement: DatabasePendingIndexRetirement,
        storage: Subspace
    ) -> ByteString {
        return storage.pack(
            Tuple(
                retirement.scope.stableOrderingKey,
                retirement.identity.name,
                retirement.identity.definitionFingerprint.bytes,
                retirement.identity.layoutFingerprint
            )
        )
    }

    private static func indexRetirementLessThan(
        _ lhs: DatabasePendingIndexRetirement,
        _ rhs: DatabasePendingIndexRetirement
    ) -> Bool {
        return (
            lhs.scope.stableOrderingKey,
            lhs.identity.name,
            lhs.identity.definitionFingerprint.bytes,
            lhs.identity.layoutFingerprint
        ) < (
            rhs.scope.stableOrderingKey,
            rhs.identity.name,
            rhs.identity.definitionFingerprint.bytes,
            rhs.identity.layoutFingerprint
        )
    }

    private static func resolveRetirementDirectory(
        _ components: [DirectoryPathComponent],
        fields: [DatabaseIndexStorageScope.DirectoryField],
        partitions: FieldObject
    ) throws -> [String] {
        let dynamicNames = components.compactMap { component in
            if case .dynamicField(let name) = component { return name }
            return nil
        }
        guard Set(partitions.fields.map { $0.key }) == Set(dynamicNames) else {
            throw DatabaseSchemaPublicationError.corruptedState(
                "index retirement partitions do not match the source directory"
            )
        }
        var result: [String] = []
        result.reserveCapacity(components.count)
        for component in components {
            switch component {
            case .staticPath(let value):
                result.append(value)
            case .dynamicField(let name):
                guard let value = partitions[name] else {
                    throw DatabaseSchemaPublicationError.corruptedState(
                        "index retirement partition is missing"
                    )
                }
                guard let field = fields.first(where: { $0.name == name }) else {
                    throw DatabaseSchemaPublicationError.corruptedState("index retirement source descriptor is missing")
                }
                result.append(try DirectoryComponentCodec.encode(value, schema: field.schema))
            }
        }
        return result
    }

    private struct ActiveIndexIdentity: Equatable {
        let definitionFingerprint: SchemaFingerprint
        let layoutFingerprint: ByteString

        init(
            definitionFingerprint: SchemaFingerprint,
            layoutFingerprint: ByteString
        ) {
            self.definitionFingerprint = definitionFingerprint
            self.layoutFingerprint = layoutFingerprint
        }

        init(_ retirement: DatabasePendingIndexRetirement) {
            self.init(
                definitionFingerprint:
                    retirement.identity.definitionFingerprint,
                layoutFingerprint: retirement.identity.layoutFingerprint
            )
        }
    }
}
