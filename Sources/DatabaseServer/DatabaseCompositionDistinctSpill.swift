import DatabaseEngine
import DatabaseKit
import DatabaseTypes
import StorageKit

/// Exact, bounded-memory DISTINCT state stored in the control domain for the
/// lifetime of one unpublished or published Composition snapshot.
actor DatabaseCompositionDistinctSpill {
    struct Result: Sendable {
        let row: DatabaseEngine.QueryRow
        let origin: CompositionOrigin
    }

    private struct Record: StorageFrameValue {
        let identity: ByteString
        let annotations: ByteString
        let version: String?
        let sequence: UInt64
        let contributors: [Base.ID]

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            try encoder.writeBytes(identity)
            try encoder.writeBytes(annotations)
            try encoder.writeOptionalString(version)
            encoder.writeUInt64(sequence)
            try encoder.writeCount(contributors.count)
            for contributor in contributors {
                try encoder.writeString(contributor.value)
            }
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            identity = try decoder.readBytes()
            annotations = try decoder.readBytes()
            version = try decoder.readOptionalString()
            sequence = try decoder.readUInt64()
            let count = try decoder.readCount()
            guard count > 0 else { throw .invalidValue }
            var contributors: [Base.ID] = []
            contributors.reserveCapacity(count)
            do {
                for _ in 0..<count {
                    contributors.append(try Base.ID(decoder.readString()))
                }
            } catch {
                throw .invalidValue
            }
            for index in 1..<contributors.count {
                guard contributors[index - 1] < contributors[index] else {
                    throw .invalidValue
                }
            }
            self.contributors = contributors
        }

        init(
            identity: ByteString,
            annotations: ByteString,
            version: String?,
            sequence: UInt64,
            contributors: [Base.ID]
        ) {
            self.identity = identity
            self.annotations = annotations
            self.version = version
            self.sequence = sequence
            self.contributors = contributors
        }
    }

    private struct SequencePointer: StorageFrameValue {
        let fingerprint: ByteString
        let collisionOrdinal: UInt64

        func encode(
            to encoder: inout StorageFrameEncoder
        ) throws(StorageFrameError) {
            try encoder.writeBytes(fingerprint)
            encoder.writeUInt64(collisionOrdinal)
        }

        init(
            from decoder: inout StorageFrameDecoder
        ) throws(StorageFrameError) {
            fingerprint = try decoder.readBytes()
            collisionOrdinal = try decoder.readUInt64()
            guard fingerprint.count == 32 else { throw .invalidValue }
        }

        init(fingerprint: ByteString, collisionOrdinal: UInt64) {
            self.fingerprint = fingerprint
            self.collisionOrdinal = collisionOrdinal
        }
    }

    private static let identityMagic: [UInt8] = [0x44, 0x43, 0x44, 0x52]
    private static let identityVersion: UInt16 = 1
    private static let identityEntity = "composition-distinct-row"
    private static let annotationsEntity =
        "composition-distinct-row-annotations"
    private static let maximumDigestCollisionRecords = 1_024
    private static let newRecordStorageOverhead: UInt64 = 256

    private let container: DBContainer
    private let workMeter: DatabaseWorkMeter
    private let identityFingerprint: @Sendable (ByteString) -> ByteString
    private let maximumPayloadBytes: UInt64
    private let identityRecords: Subspace
    private let sequenceRecords: Subspace
    private var persistedPayloadBytes: UInt64 = 0

    init(
        snapshotStore: DatabaseCompositionSnapshotStore,
        reservation: DatabaseCompositionSnapshotStore.WriteReservation,
        maximumIntermediateBytes: UInt64,
        workMeter: DatabaseWorkMeter,
        identityFingerprint: (@Sendable (ByteString) -> ByteString)? = nil
    ) {
        self.container = snapshotStore.controlContainer
        self.workMeter = workMeter
        self.identityFingerprint = identityFingerprint ?? { identity in
            Self.fingerprint(identity)
        }
        self.maximumPayloadBytes = min(
            maximumIntermediateBytes,
            16 * 1_024 * 1_024
        )
        let workspace = snapshotStore.distinctWorkspace(in: reservation)
        self.identityRecords = workspace.subspace("identities")
        self.sequenceRecords = workspace.subspace("sequence")
    }

    var payloadByteCount: UInt64 {
        persistedPayloadBytes
    }

    func insert(
        _ row: DatabaseEngine.QueryRow,
        origin: CompositionOrigin,
        sequence: UInt64
    ) async throws {
        let identity = try Self.encodeFields(
            row.fields,
            entity: Self.identityEntity
        )
        let fingerprint = identityFingerprint(identity)
        guard fingerprint.count == 32 else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        try workMeter.consume(at: .deduplication)
        let annotations = try Self.encodeFields(
            row.annotations,
            entity: Self.annotationsEntity
        )
        let incomingContributors = Self.contributors(origin)
        let consumedBefore = persistedPayloadBytes
        let nextPayloadBytes = try await container
            .withControlMetadataTransaction { transaction in
                let collisionSpace = self.identityRecords.subspace(fingerprint)
                let range = collisionSpace.range()
                let entries = try await TransactionRangeCollection.collect(
                    using: transaction.storageAccess,
                    from: .firstGreaterOrEqual(range.begin),
                    to: .firstGreaterOrEqual(range.end),
                    limit: Self.maximumDigestCollisionRecords + 1,
                    reverse: false,
                    snapshot: false,
                    streamingMode: .iterator
                )
                guard entries.count <= Self.maximumDigestCollisionRecords else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                for (key, bytes) in entries {
                    let record = try Self.decodeRecord(bytes)
                    guard record.identity == identity else { continue }
                    let contributors = Self.mergeContributors(
                        record.contributors,
                        incomingContributors
                    )
                    guard contributors != record.contributors else {
                        return consumedBefore
                    }
                    let encoded = try StorageFrameCodec.encode(
                        Record(
                            identity: record.identity,
                            annotations: record.annotations,
                            version: record.version,
                            sequence: record.sequence,
                            contributors: contributors
                        )
                    )
                    let additional = max(0, encoded.count - bytes.count)
                    let next = try Self.admit(
                        consumed: consumedBefore,
                        requested: UInt64(additional),
                        maximum: self.maximumPayloadBytes
                    )
                    try transaction.storageAccess.setValue(encoded, for: key)
                    return next
                }

                guard entries.count
                        < Self.maximumDigestCollisionRecords else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }

                guard let collisionOrdinal = UInt64(exactly: entries.count)
                else {
                    throw DatabaseQueryExecutionError
                        .compositionSnapshotCorrupted
                }
                let record = try StorageFrameCodec.encode(
                    Record(
                        identity: identity,
                        annotations: annotations,
                        version: row.version?.value,
                        sequence: sequence,
                        contributors: incomingContributors
                    )
                )
                let pointer = try StorageFrameCodec.encode(
                    SequencePointer(
                        fingerprint: fingerprint,
                        collisionOrdinal: collisionOrdinal
                    )
                )
                let requested = try Self.adding(
                    UInt64(record.count),
                    UInt64(pointer.count),
                    Self.newRecordStorageOverhead
                )
                let next = try Self.admit(
                    consumed: consumedBefore,
                    requested: requested,
                    maximum: self.maximumPayloadBytes
                )
                try transaction.storageAccess.setValue(
                    record,
                    for: collisionSpace.pack(Tuple(collisionOrdinal))
                )
                try transaction.storageAccess.setValue(
                    pointer,
                    for: self.sequenceRecords.pack(Tuple(sequence))
                )
                return next
            }
        persistedPayloadBytes = nextPayloadBytes
    }

    func forEachResult(
        batchSize: Int,
        _ body: @Sendable (Result) async throws -> Bool
    ) async throws {
        guard batchSize > 0 else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        let range = sequenceRecords.range()
        var begin = KeySelector.firstGreaterOrEqual(range.begin)
        while true {
            let currentBegin = begin
            let batch: [(ByteString, Result)] = try await container
                .withControlMetadataTransaction(configuration: .readOnly) {
                    transaction in
                    let pointers = try await TransactionRangeCollection.collect(
                        using: transaction.storageAccess,
                        from: currentBegin,
                        to: .firstGreaterOrEqual(range.end),
                        limit: batchSize,
                        reverse: false,
                        snapshot: false,
                        streamingMode: .iterator
                    )
                    var results: [(ByteString, Result)] = []
                    results.reserveCapacity(pointers.count)
                    for (key, pointerBytes) in pointers {
                        let pointer = try Self.decodePointer(pointerBytes)
                        guard let recordBytes = try await transaction
                            .storageAccess.getValue(
                                for: self.identityRecords
                                    .subspace(pointer.fingerprint)
                                    .pack(Tuple(pointer.collisionOrdinal)),
                                snapshot: false
                            ) else {
                            throw DatabaseQueryExecutionError
                                .compositionSnapshotCorrupted
                        }
                        let record = try Self.decodeRecord(recordBytes)
                        let fields = try Self.decodeFields(
                            record.identity,
                            entity: Self.identityEntity
                        )
                        let annotations = try Self.decodeFields(
                            record.annotations,
                            entity: Self.annotationsEntity
                        )
                        let origin: CompositionOrigin = record.contributors.count
                            == 1
                            ? .source(record.contributors[0])
                            : .derived(contributors: record.contributors)
                        results.append(
                            (
                                key,
                                Result(
                                    row: DatabaseEngine.QueryRow(
                                        fields: fields,
                                        annotations: annotations,
                                        version: record.version.map(
                                            PersistableVersionToken.init
                                        )
                                    ),
                                    origin: origin
                                )
                            )
                        )
                    }
                    return results
                }
            guard !batch.isEmpty else { return }
            for (_, result) in batch {
                guard try await body(result) else { return }
            }
            guard batch.count == batchSize,
                  let lastKey = batch.last?.0 else { return }
            begin = .firstGreaterThan(lastKey)
        }
    }

    private static func encodeFields(
        _ values: [String: FieldValue],
        entity: String
    ) throws -> ByteString {
        let names = values.keys.sorted()
        let fields = try names.enumerated().map { offset, name in
            guard let number = UInt32(exactly: offset + 1),
                  let value = values[name] else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
            return try PersistableField(
                number: number,
                name: name,
                value: value
            )
        }
        return try PersistableFieldFrameCodec.encode(
            magic: identityMagic,
            version: identityVersion,
            entity: entity,
            fields: fields
        )
    }

    private static func decodeFields(
        _ bytes: ByteString,
        entity: String
    ) throws -> [String: FieldValue] {
        let decoded = try PersistableFieldFrameCodec.decode(
            bytes,
            magic: identityMagic,
            version: identityVersion,
            expectedEntity: entity
        )
        var fields: [String: FieldValue] = [:]
        fields.reserveCapacity(decoded.fields.count)
        for field in decoded.fields {
            guard fields.updateValue(field.value, forKey: field.name) == nil else {
                throw DatabaseQueryExecutionError
                    .compositionSnapshotCorrupted
            }
        }
        return fields
    }

    private static func fingerprint(_ identity: ByteString) -> ByteString {
        var hasher = SHA256Accumulator()
        hasher.update(identity)
        return hasher.finalize()
    }

    private static func decodeRecord(_ bytes: ByteString) throws -> Record {
        do { return try StorageFrameCodec.decode(Record.self, from: bytes) }
        catch {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
    }

    private static func decodePointer(
        _ bytes: ByteString
    ) throws -> SequencePointer {
        do {
            return try StorageFrameCodec.decode(
                SequencePointer.self,
                from: bytes
            )
        } catch {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
    }

    private static func contributors(
        _ origin: CompositionOrigin
    ) -> [Base.ID] {
        switch origin {
        case .source(let baseID): [baseID]
        case .derived(let contributors): contributors
        }
    }

    private static func mergeContributors(
        _ lhs: [Base.ID],
        _ rhs: [Base.ID]
    ) -> [Base.ID] {
        var values = Set(lhs)
        values.formUnion(rhs)
        return values.sorted()
    }

    private static func admit(
        consumed: UInt64,
        requested: UInt64,
        maximum: UInt64
    ) throws -> UInt64 {
        guard requested <= maximum - consumed else {
            throw DatabaseWorkLimitError.maximumIntermediateBytes(
                stage: .deduplication,
                consumed: consumed,
                requested: requested,
                maximum: maximum
            )
        }
        return consumed + requested
    }

    private static func adding(
        _ first: UInt64,
        _ second: UInt64,
        _ third: UInt64
    ) throws -> UInt64 {
        let partial = first.addingReportingOverflow(second)
        guard !partial.overflow else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        let result = partial.partialValue.addingReportingOverflow(third)
        guard !result.overflow else {
            throw DatabaseQueryExecutionError.compositionSnapshotCorrupted
        }
        return result.partialValue
    }
}
