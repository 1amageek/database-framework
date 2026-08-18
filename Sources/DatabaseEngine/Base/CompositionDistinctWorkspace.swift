#if DATABASE_MULTIPLE_BASES
import DatabaseKit
import DatabaseTypes

/// Exact, request-local DISTINCT state with a hard retained-byte limit.
/// Durable result paging belongs to a host adapter, not this workspace.
package actor CompositionDistinctWorkspace {
    package struct Result: Sendable {
        package let row: QueryRow
        package let origin: CompositionOrigin
    }

    private struct Entry: Sendable {
        let identity: ByteString
        let row: QueryRow
        var contributors: [Base.ID]
    }

    private struct Pointer: Sendable {
        let sequence: UInt64
        let fingerprint: ByteString
        let collisionOrdinal: Int
    }

    private enum State: Sendable, Equatable {
        case accumulating
        case reading
        case removed
    }

    private static let identityMagic: [UInt8] = [0x44, 0x43, 0x44, 0x52]
    private static let identityVersion: UInt16 = 1
    private static let identityEntity = "composition-distinct-row"
    private static let annotationsEntity =
        "composition-distinct-row-annotations"
    private static let maximumDigestCollisionRecords = 1_024
    private static let newEntryOverhead: UInt64 = 256

    private let workMeter: DatabaseWorkMeter
    private let identityFingerprint: @Sendable (ByteString) -> ByteString
    private let maximumPayloadBytes: UInt64
    private var entries: [ByteString: [Entry]] = [:]
    private var pointers: [Pointer] = []
    private var previousSequence: UInt64?
    private var retainedPayloadBytes: UInt64 = 0
    private var reservation: DatabaseIntermediateReservation?
    private var state: State = .accumulating

    private init(
        maximumIntermediateBytes: UInt64,
        workMeter: DatabaseWorkMeter,
        identityFingerprint: (@Sendable (ByteString) -> ByteString)?
    ) {
        self.workMeter = workMeter
        self.identityFingerprint = identityFingerprint ?? { identity in
            Self.fingerprint(identity)
        }
        self.maximumPayloadBytes = maximumIntermediateBytes
    }

    package static func create(
        maximumIntermediateBytes: UInt64,
        workMeter: DatabaseWorkMeter,
        identityFingerprint: (@Sendable (ByteString) -> ByteString)? = nil
    ) -> CompositionDistinctWorkspace {
        CompositionDistinctWorkspace(
            maximumIntermediateBytes: maximumIntermediateBytes,
            workMeter: workMeter,
            identityFingerprint: identityFingerprint
        )
    }

    package func insert(
        _ row: QueryRow,
        origin: CompositionOrigin,
        sequence: UInt64
    ) throws {
        guard state == .accumulating else {
            throw CompositionQueryError.workspaceCorrupted
        }
        guard previousSequence.map({ $0 < sequence }) ?? true else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let identity = try Self.encodeFields(
            row.fields,
            entity: Self.identityEntity
        )
        let fingerprint = identityFingerprint(identity)
        guard fingerprint.count == 32 else {
            throw CompositionQueryError.workspaceCorrupted
        }
        try workMeter.consume(at: .deduplication)
        let incomingContributors = try Self.validatedContributors(origin)

        if let index = entries[fingerprint]?.firstIndex(where: {
            $0.identity == identity
        }) {
            guard let existing = entries[fingerprint]?[index] else {
                throw CompositionQueryError.workspaceCorrupted
            }
            let merged = Self.mergeContributors(
                existing.contributors,
                incomingContributors
            )
            guard merged != existing.contributors else {
                previousSequence = sequence
                return
            }
            let oldBytes = try Self.contributorByteCount(
                existing.contributors
            )
            let newBytes = try Self.contributorByteCount(merged)
            guard newBytes >= oldBytes else {
                throw CompositionQueryError.workspaceCorrupted
            }
            try admit(bytes: newBytes - oldBytes)
            entries[fingerprint, default: []][index].contributors = merged
            previousSequence = sequence
            return
        }

        let collisionCount = entries[fingerprint]?.count ?? 0
        guard collisionCount < Self.maximumDigestCollisionRecords else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let annotations = try Self.encodeFields(
            row.annotations,
            entity: Self.annotationsEntity
        )
        var requested = Self.newEntryOverhead
        requested = try Self.adding(requested, UInt64(identity.count))
        requested = try Self.adding(requested, UInt64(annotations.count))
        requested = try Self.adding(requested, UInt64(fingerprint.count))
        if let version = row.version {
            requested = try Self.adding(
                requested,
                UInt64(version.value.utf8.count)
            )
        }
        let contributorBytes = try Self.contributorByteCount(
            incomingContributors
        )
        requested = try Self.adding(requested, contributorBytes)
        try admit(rows: 1, bytes: requested)
        entries[fingerprint, default: []].append(
            Entry(
                identity: identity,
                row: row,
                contributors: incomingContributors
            )
        )
        pointers.append(
            Pointer(
                sequence: sequence,
                fingerprint: fingerprint,
                collisionOrdinal: collisionCount
            )
        )
        previousSequence = sequence
    }

    package func forEachResult(
        batchSize: Int,
        _ body: @Sendable (Result) async throws -> Bool
    ) async throws {
        guard batchSize > 0, state == .accumulating else {
            throw CompositionQueryError.workspaceCorrupted
        }
        state = .reading
        var index = 0
        while index < pointers.count {
            guard state == .reading else {
                throw CompositionQueryError.workspaceCorrupted
            }
            let pointer = pointers[index]
            if index > 0 {
                guard pointers[index - 1].sequence < pointer.sequence else {
                    throw CompositionQueryError.workspaceCorrupted
                }
            }
            guard let bucket = entries[pointer.fingerprint],
                  bucket.indices.contains(pointer.collisionOrdinal) else {
                throw CompositionQueryError.workspaceCorrupted
            }
            let entry = bucket[pointer.collisionOrdinal]
            let origin: CompositionOrigin = entry.contributors.count == 1
                ? .source(entry.contributors[0])
                : .derived(contributors: entry.contributors)
            guard try await body(Result(row: entry.row, origin: origin)) else {
                return
            }
            index += 1
            if index % batchSize == 0 {
                await Task.yield()
            }
        }
    }

    package func removeAll() {
        reservation?.release()
        reservation = nil
        entries.removeAll(keepingCapacity: false)
        pointers.removeAll(keepingCapacity: false)
        previousSequence = nil
        retainedPayloadBytes = 0
        state = .removed
    }

    private func admit(rows: UInt64 = 0, bytes: UInt64) throws {
        let nextPayloadBytes = try Self.admit(
            consumed: retainedPayloadBytes,
            requested: bytes,
            maximum: maximumPayloadBytes
        )
        if let reservation {
            try reservation.reserveAdditional(
                rows: rows,
                bytes: bytes,
                at: .deduplication
            )
        } else {
            reservation = try workMeter.reserveIntermediate(
                rows: rows,
                bytes: bytes,
                at: .deduplication
            )
        }
        retainedPayloadBytes = nextPayloadBytes
    }

    private static func encodeFields(
        _ values: [String: FieldValue],
        entity: String
    ) throws -> ByteString {
        let names = values.keys.sorted()
        let fields = try names.enumerated().map { offset, name in
            guard let number = UInt32(exactly: offset + 1),
                  let value = values[name] else {
                throw CompositionQueryError.workspaceCorrupted
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

    private static func fingerprint(_ identity: ByteString) -> ByteString {
        var hasher = SHA256Accumulator()
        hasher.update(identity)
        return hasher.finalize()
    }

    private static func validatedContributors(
        _ origin: CompositionOrigin
    ) throws -> [Base.ID] {
        let contributors: [Base.ID]
        switch origin {
        case .source(let baseID):
            contributors = [baseID]
        case .derived(let values):
            contributors = values
        }
        guard !contributors.isEmpty else {
            throw CompositionQueryError.workspaceCorrupted
        }
        for (previous, current) in zip(
            contributors,
            contributors.dropFirst()
        ) {
            guard previous < current else {
                throw CompositionQueryError.workspaceCorrupted
            }
        }
        return contributors
    }

    private static func mergeContributors(
        _ lhs: [Base.ID],
        _ rhs: [Base.ID]
    ) -> [Base.ID] {
        var values = Set(lhs)
        values.formUnion(rhs)
        return values.sorted()
    }

    private static func contributorByteCount(
        _ contributors: [Base.ID]
    ) throws -> UInt64 {
        var count: UInt64 = 0
        for contributor in contributors {
            count = try adding(
                count,
                UInt64(contributor.value.utf8.count) + 16
            )
        }
        return count
    }

    private static func admit(
        consumed: UInt64,
        requested: UInt64,
        maximum: UInt64
    ) throws -> UInt64 {
        guard consumed <= maximum,
              requested <= maximum - consumed else {
            throw DatabaseWorkLimitError.maximumIntermediateBytes(
                stage: .deduplication,
                consumed: consumed,
                requested: requested,
                maximum: maximum
            )
        }
        return consumed + requested
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        let result = lhs.addingReportingOverflow(rhs)
        guard !result.overflow else {
            throw CompositionQueryError.workspaceCorrupted
        }
        return result.partialValue
    }
}

#endif
