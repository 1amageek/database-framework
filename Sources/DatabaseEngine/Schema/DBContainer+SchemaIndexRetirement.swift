import DatabaseKit
import DatabaseTypes
@_spi(DatabaseExecution) import DatabaseWire
import StorageKit

private struct IndexRetirementScopeFrame: StorageFrameValue {
    let scope: DatabaseIndexStorageScope

    init(_ scope: DatabaseIndexStorageScope) {
        self.scope = scope
    }

    func encode(
        to encoder: inout StorageFrameEncoder
    ) throws(StorageFrameError) {
        encoder.writeUInt8(1)
        switch scope {
        case .entity(let name, let components):
            encoder.writeUInt8(0)
            try encoder.writeString(name)
            try encoder.writeCount(components.count)
            for component in components {
                switch component {
                case .staticPath(let value):
                    encoder.writeUInt8(0)
                    try encoder.writeString(value)
                case .dynamicField(let name):
                    encoder.writeUInt8(1)
                    try encoder.writeString(name)
                }
            }
        case .polymorphicGroup(let identifier, let directoryPath):
            encoder.writeUInt8(1)
            try encoder.writeString(identifier)
            try encoder.writeCount(directoryPath.count)
            for component in directoryPath {
                try encoder.writeString(component)
            }
        }
    }

    init(
        from decoder: inout StorageFrameDecoder
    ) throws(StorageFrameError) {
        guard try decoder.readUInt8() == 1 else {
            throw .invalidValue
        }
        let kind = try decoder.readUInt8()
        let identifier = try decoder.readString()
        let count = try decoder.readCount()
        switch kind {
        case 0:
            var components: [DirectoryPathComponent] = []
            components.reserveCapacity(count)
            for _ in 0..<count {
                let componentKind = try decoder.readUInt8()
                let value = try decoder.readString()
                switch componentKind {
                case 0:
                    components.append(.staticPath(value))
                case 1:
                    components.append(.dynamicField(fieldName: value))
                default:
                    throw .invalidValue
                }
            }
            scope = .entity(
                name: identifier,
                directoryComponents: components
            )
        case 1:
            var directoryPath: [String] = []
            directoryPath.reserveCapacity(count)
            for _ in 0..<count {
                directoryPath.append(try decoder.readString())
            }
            scope = .polymorphicGroup(
                identifier: identifier,
                directoryPath: directoryPath
            )
        default:
            throw .invalidValue
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
        var pending = Set(
            try await loadSchemaIndexRetirements(transaction: transaction)
        )
        pending.formUnion(additions)
        let retained = try pending.filter { retirement in
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
                    IndexRetirementScopeFrame(retirement.scope)
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
        transaction: any TransactionReadAccess
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
        transaction: any TransactionReadAccess
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
            let scope: DatabaseIndexStorageScope
            do {
                scope = try StorageFrameCodec.decode(
                    IndexRetirementScopeFrame.self,
                    from: value
                ).scope
            } catch {
                throw DatabaseSchemaPublicationError.corruptedState(
                    "schema index retirement scope cannot be decoded"
                )
            }
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
                identity: identity
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

    /// Retires the exact generation using the source directory contract stored
    /// with the durable marker. The published target schema is intentionally
    /// not consulted because the source entity or group may have been removed.
    @_spi(DatabaseExecution)
    public func retireSchemaIndexStorage(
        _ retirement: DatabasePendingIndexRetirement,
        partitions: FieldObject,
        transaction: any TransactionAccess
    ) throws {
        try validateSchemaIndexRetirement(retirement)
        let path: [String]
        switch retirement.scope {
        case .entity(_, let components):
            path = try Self.resolveRetirementDirectory(
                components,
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
        let subspace = try operationDataSubspace(relativePath: path)
        try IndexStorageRetirer.retire(
            indexName: retirement.identity.name,
            selection: selection,
            storeSubspace: subspace,
            transaction: transaction
        )
        try clearSchemaIndexBuildPending(
            scope: retirement.scope,
            index: retirement.identity.name,
            selection: selection,
            transaction: transaction
        )
    }

    private func activeIndexIdentity(
        matching retirement: DatabasePendingIndexRetirement,
        in target: Schema,
        indexPhysicalLayouts: [String: IndexPhysicalLayout]
    ) throws -> ActiveIndexIdentity? {
        let index = retirement.identity.name
        let isDeclared: Bool
        switch retirement.scope {
        case .entity(let name, let directoryComponents):
            isDeclared = target.entity(named: name).map { entity in
                entity.directoryComponents == directoryComponents
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
                result.append(try CanonicalDirectoryPartitionCodec.encode(value))
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
