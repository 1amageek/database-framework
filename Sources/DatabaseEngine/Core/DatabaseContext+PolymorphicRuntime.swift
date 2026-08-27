import DatabaseKit
import StorageKit

public enum PolymorphicRowAnnotation {
    public static let typeName = "_typeName"
    public static let typeCode = "_typeCode"
}

public struct PolymorphicEntity: Sendable {
    public let item: PersistedModel
    public let typeName: String
    public let typeCode: Int64
    public let polymorphicIdentifier: Tuple

    public init(
        item: PersistedModel,
        typeName: String,
        typeCode: Int64,
        polymorphicIdentifier: Tuple
    ) {
        self.item = item
        self.typeName = typeName
        self.typeCode = typeCode
        self.polymorphicIdentifier = polymorphicIdentifier
    }
}

extension DatabaseContext {
    public func executeCanonicalRead<T: Sendable>(
        configuration: TransactionConfiguration = .default,
        _ operation: @Sendable @escaping (any TransactionAccess) async throws -> T
    ) async throws -> T {
        try await withStorageAccess(
            requiredAccess: .read,
            configuration: configuration,
            operation
        )
    }

    /// Scans polymorphic rows into one request-owned aggregate.
    ///
    /// Collection storage is admitted before the cursor is created. Each row
    /// slot is then admitted before identifier decoding, model decoding, or
    /// row-level authorization begins.
    package func scanRetainedPolymorphicItems(
        group: PolymorphicGroup,
        session: DatabaseReadSession,
        admission: DatabaseReadSession.PolymorphicReadAdmission
    ) async throws -> sending DatabaseRetainedPolymorphicEntities {
        try admission.require(
            groupIdentifier: group.identifier,
            operation: .scan
        )
        let workMeter = admission.workMeter
        let transaction = session.transaction
        let typeMap = try session.polymorphicTypeMap(for: group)
        var builder = try DatabaseRetainedPolymorphicEntities.Builder(
            workMeter: workMeter,
            stage: .storageRow
        )
        guard let subspace = try await container.openPolymorphicDirectory(
            for: group.identifier,
            transaction: transaction.storageAccess
        ) else {
            return builder.finish()
        }
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let storage = container.itemStorageFactory.make(
            transaction: transaction.storageAccess,
            blobsSubspace: blobsSubspace
        )
        let (begin, end) = itemSubspace.range()
        try await storage.consumeRetainedScan(
            begin: begin,
            end: end,
            snapshot: true,
            workMeter: workMeter,
            stage: .storageRow
        ) { key, data in
            let admission = try builder.prepareEntry(at: .storageRow)
            guard key.count >= itemSubspace.prefix.count else {
                throw PolymorphicRuntimeError.invalidStoredIdentifier
            }
            let identifier = try self.retainedPolymorphicIdentifier(
                packed: key[itemSubspace.prefix.count..<key.count],
                workMeter: workMeter,
                stage: .storageRow
            )
            let typeCode = try self.polymorphicTypeCode(in: identifier)
            guard let runtime = typeMap[typeCode] else {
                throw PolymorphicRuntimeError.unknownTypeCode(typeCode)
            }
            let model = try DatabaseRetainedStoredModel.decode(
                data,
                entity: runtime.entity.name,
                runtime: runtime,
                workMeter: workMeter,
                stage: .storageRow
            )
            try model.withModel { try session.authorizeGet($0) }
            try builder.append(
                model: model,
                identifier: identifier,
                runtime: runtime,
                using: admission
            )
        }
        return builder.finish()
    }

    /// Fetches one retained polymorphic slot for every retained identifier.
    /// Missing values remain explicit nil slots in the original order.
    package func fetchRetainedPolymorphicItemsPreservingOrder(
        group: PolymorphicGroup,
        ids: any DatabaseRetainedPrimaryKeyCollection,
        session: DatabaseReadSession,
        snapshot: Bool,
        admission: DatabaseReadSession.PolymorphicReadAdmission
    ) async throws -> sending DatabaseRetainedPolymorphicEntities {
        try admission.require(
            groupIdentifier: group.identifier,
            operation: .orderedPointFetch
        )
        let workMeter = admission.workMeter
        guard ids.workMeter === workMeter else {
            throw DatabaseIntermediateReservationError.workMeterMismatch
        }
        let transaction = session.transaction
        let typeMap = try session.polymorphicTypeMap(for: group)
        var builder = try DatabaseRetainedPolymorphicEntities.Builder(
            workMeter: workMeter,
            stage: .storageRow,
            expectedCount: ids.count
        )
        guard let subspace = try await container.openPolymorphicDirectory(
            for: group.identifier,
            transaction: transaction.storageAccess
        ) else {
            for _ in 0..<ids.count {
                let admission = try builder.prepareEntry(at: .storageRow)
                builder.appendMissing(using: admission)
            }
            return builder.finish()
        }
        let itemSubspace = subspace.subspace(SubspaceKey.items)
        let blobsSubspace = subspace.subspace(SubspaceKey.blobs)
        let storage = container.itemStorageFactory.make(
            transaction: transaction.storageAccess,
            blobsSubspace: blobsSubspace
        )

        for index in 0..<ids.count {
            try workMeter.consume(at: .storageRow)
            let admission = try builder.prepareEntry(at: .storageRow)
            var retainedIdentifier: DatabaseRetainedPrimaryKey?
            var retainedKey: ByteString?
            try ids.withRetainedPrimaryKey(at: index) { identifier in
                retainedIdentifier = try self.retainedPolymorphicIdentifier(
                    identifier,
                    workMeter: workMeter,
                    stage: .storageRow
                )
                retainedKey = try self.retainedPolymorphicStorageKey(
                    identifier,
                    subspace: itemSubspace,
                    workMeter: workMeter,
                    stage: .storageRow
                )
            }
            guard let identifier = retainedIdentifier,
                  let key = retainedKey else {
                preconditionFailure(
                    "A retained primary-key collection did not yield its element"
                )
            }
            let typeCode = try polymorphicTypeCode(
                in: identifier,
                invalidIdentifierError: .invalidRequestedIdentifier
            )
            guard let runtime = typeMap[typeCode] else {
                throw PolymorphicRuntimeError.unknownTypeCode(typeCode)
            }
            guard let data = try await storage.readRetained(
                for: key,
                snapshot: snapshot,
                workMeter: workMeter,
                stage: .storageRow
            ) else {
                builder.appendMissing(using: admission)
                continue
            }
            let model = try DatabaseRetainedStoredModel.decode(
                data,
                entity: runtime.entity.name,
                runtime: runtime,
                workMeter: workMeter,
                stage: .storageRow
            )
            try model.withModel { try session.authorizeGet($0) }
            try builder.append(
                model: model,
                identifier: identifier,
                runtime: runtime,
                using: admission
            )
        }
        return builder.finish()
    }

    private func retainedPolymorphicIdentifier(
        _ identifier: borrowing Tuple,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseRetainedPrimaryKey {
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(identifier.packedByteCount),
            at: stage
        )
        do {
            let packed = identifier.pack()
            let retained = try DatabaseRetainedByteString.make(
                packed,
                reservation: reservation,
                at: stage
            )
            let tuple = try Tuple(packed: retained) { additionalByteCount in
                try reservation.reserveAdditional(
                    bytes: UInt64(additionalByteCount),
                    at: stage
                )
            }
            return DatabaseRetainedPrimaryKey(
                value: tuple,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func retainedPolymorphicIdentifier(
        packed: ByteString,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> DatabaseRetainedPrimaryKey {
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(packed.count),
            at: stage
        )
        do {
            let retained = try DatabaseRetainedByteString.copying(
                packed,
                reservation: reservation,
                at: stage
            )
            let tuple = try Tuple(packed: retained) { additionalByteCount in
                try reservation.reserveAdditional(
                    bytes: UInt64(additionalByteCount),
                    at: stage
                )
            }
            return DatabaseRetainedPrimaryKey(
                value: tuple,
                reservation: reservation
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func retainedPolymorphicStorageKey(
        _ identifier: borrowing Tuple,
        subspace: Subspace,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> ByteString {
        let (keyByteCount, overflow) = subspace.prefix.count
            .addingReportingOverflow(identifier.packedByteCount)
        guard !overflow else {
            throw DatabaseIntermediateFootprintError.byteAdditionOverflow(
                left: UInt64(subspace.prefix.count),
                right: UInt64(identifier.packedByteCount)
            )
        }
        let reservation = try workMeter.reserveIntermediate(
            bytes: UInt64(keyByteCount),
            at: stage
        )
        do {
            return try DatabaseRetainedByteString.make(
                subspace.pack(identifier),
                reservation: reservation,
                at: stage
            )
        } catch {
            reservation.release()
            throw error
        }
    }

    private func polymorphicTypeCode(
        in identifier: DatabaseRetainedPrimaryKey,
        invalidIdentifierError: PolymorphicRuntimeError =
            .invalidStoredIdentifier
    ) throws -> Int64 {
        var result: Int64?
        try identifier.withValue { tuple in
            guard tuple.count > 0,
                  case .signedInteger(let typeCode) = try tuple.value(at: 0)
            else {
                throw invalidIdentifierError
            }
            result = typeCode
        }
        guard let result else {
            throw invalidIdentifierError
        }
        return result
    }

    public func fetchPolymorphicItems(
        group: PolymorphicGroup,
        ids: [Tuple],
        configuration: TransactionConfiguration = .default
    ) async throws -> [PolymorphicEntity] {
        try await withDataOperation { [self] in
            let authorization = try readPolicy()
                .authorizePolymorphicModelRead(group: group)
            let workMeter = DatabaseWorkMeter(
                budget: ExecutionBudget(),
                monotonicClock: container.monotonicClock
            )
            let retainedIdentifiers = try retainedPolymorphicIdentifiers(
                ids,
                workMeter: workMeter
            )
            return try await withReadSnapshot(
                configuration: configuration,
                workMeter: workMeter
            ) { snapshot in
                let session = try snapshot.session.authorizedSession(
                    authorization
                )
                let entities = try await session
                    .fetchRetainedPolymorphicItemsPreservingOrder(
                        group: group,
                        ids: retainedIdentifiers
                    )
                return entities.promoteEntitiesToPublicOutput()
            }
        }
    }

    private func retainedPolymorphicIdentifiers(
        _ identifiers: [Tuple],
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseRetainedPrimaryKeys {
        var builder = try DatabaseRetainedArrayBuilder<
            DatabaseRetainedPrimaryKey
        >(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(
                DatabaseRetainedPrimaryKey.self
            ),
            expectedCount: identifiers.count
        )
        for identifier in identifiers {
            try builder.append(
                footprint: DatabaseIntermediateFootprint(rows: 1),
                at: .storageRow
            ) {
                try retainedPolymorphicIdentifier(
                    identifier,
                    workMeter: workMeter,
                    stage: .storageRow
                )
            }
        }
        return try DatabaseRetainedPrimaryKeys(buffer: builder.finish())
    }

    package func polymorphicTypeMap(
        for group: PolymorphicGroup
    ) throws -> [Int64: EntityRuntimeRegistration] {
        var result: [Int64: EntityRuntimeRegistration] = [:]
        for typeName in group.memberTypeNames {
            guard let runtime = container.runtimeConfiguration.entityRuntimes.registration(
                named: typeName
            ) else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledPolymorphicMemberType(
                        groupIdentifier: group.identifier,
                        memberTypeName: typeName
                    )
            }
            guard runtime.entity.polymorphicMembership?.identifier == group.identifier else {
                throw PolymorphicRuntimeError.nonPolymorphableMember(
                    groupIdentifier: group.identifier,
                    memberTypeName: typeName
                )
            }
            result[PolymorphicTypeCode.value(for: runtime.entity.name)] = runtime
        }
        return result
    }

    public func authorizePolymorphicListAccess(
        group: PolymorphicGroup,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        for typeName in group.memberTypeNames {
            guard let runtime = container.runtimeConfiguration.entityRuntimes.registration(
                named: typeName
            ) else {
                throw DatabaseRuntimeConfigurationError
                    .missingCompiledPolymorphicMemberType(
                        groupIdentifier: group.identifier,
                        memberTypeName: typeName
                    )
            }
            try evaluatePolymorphicListAccess(
                for: runtime.entity,
                limit: limit,
                offset: offset,
                orderBy: orderBy
            )
        }
    }

    private func evaluatePolymorphicListAccess(
        for entity: Schema.Entity,
        limit: Int?,
        offset: Int?,
        orderBy: [String]?
    ) throws {
        try container.securityDelegate?.evaluateList(
            entity: entity.name,
            limit: limit,
            offset: offset,
            orderBy: orderBy
        )
    }

}
