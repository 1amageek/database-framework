#if DATABASE_MULTI_BASE
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
        let fingerprint: ByteString
        let identity: ByteString
        let ownedRow: DatabaseQueryScopedQueryRowOwner
        var contributors: [Base.ID]
    }

    private struct FieldFramePlan: Sendable {
        let fields: [PersistableField]
        let encodedByteCount: UInt64
        let scratchReservation: DatabaseIntermediateReservation
    }

    private enum ContributorInput: Sendable {
        case single(Base.ID)
        case multiple([Base.ID])

        var count: Int {
            switch self {
            case .single: return 1
            case .multiple(let values): return values.count
            }
        }

        subscript(index: Int) -> Base.ID {
            switch self {
            case .single(let value):
                precondition(index == 0)
                return value
            case .multiple(let values):
                return values[index]
            }
        }
    }

    private struct ContributorPlan: Sendable {
        let count: Int
        let capacity: Int
        let retainedByteCount: UInt64
        let addsContributor: Bool
    }

    private struct ArrayStorageReplacement: Sendable {
        let capacity: Int
        let oldFootprint: UInt64
        let newFootprint: UInt64
    }

    private enum State: Sendable, Equatable {
        case accumulating
        case reading
        case removed
    }

    private static let identityMagic: [UInt8] = [0x44, 0x43, 0x44, 0x52]
    private static let identityVersion: UInt16 = 1
    private static let identityEntity = "composition-distinct-row"
    private static let maximumDigestCollisionRecords = 1_024
    private static let fingerprintByteCount: UInt64 = 32
    private static let initialSlotCapacity = 8

    private let workMeter: DatabaseWorkMeter
    private let identityFingerprint: @Sendable (ByteString) -> ByteString
    private let maximumPayloadBytes: UInt64
    private var entries: [Entry] = []
    private var slots: [Int?] = []
    private var accountedEntryCapacity = 0
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
        _ producedRow: consuming DatabaseQueryScopedQueryRow,
        origin: CompositionOrigin,
        sequence: UInt64
    ) throws {
        try insert(
            producedRow.moveToRetainedOwner(),
            origin: origin,
            sequence: sequence
        )
    }

    package func insert(
        _ ownedRow: DatabaseQueryScopedQueryRowOwner,
        origin: CompositionOrigin,
        sequence: UInt64
    ) throws {
        guard state == .accumulating,
              ownedRow.workMeter === workMeter,
              ownedRow.footprint.rows == 1,
              previousSequence.map({ $0 < sequence }) ?? true else {
            throw CompositionQueryError.workspaceCorrupted
        }

        try workMeter.consume(at: .deduplication)
        let contributorInput = try Self.validatedContributorInput(origin)
        let framePlan = try ownedRow.withRow { row in
            try Self.makeFieldFramePlan(
                row.fields,
                entity: Self.identityEntity,
                workMeter: workMeter
            )
        }
        defer { framePlan.scratchReservation.release() }

        let identityClaimBytes = try Self.adding(
            framePlan.encodedByteCount,
            Self.fingerprintByteCount
        )
        let identityReservation = try workMeter.reserveIntermediate(
            bytes: identityClaimBytes,
            at: .deduplication
        )
        var identityWasAdopted = false
        defer {
            if !identityWasAdopted {
                identityReservation.release()
            }
        }

        let identity = try PersistableFieldFrameCodec.encode(
            magic: Self.identityMagic,
            version: Self.identityVersion,
            entity: Self.identityEntity,
            fields: framePlan.fields
        )
        guard UInt64(identity.count) == framePlan.encodedByteCount else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let fingerprint = identityFingerprint(identity)
        guard UInt64(fingerprint.count) == Self.fingerprintByteCount else {
            throw CompositionQueryError.workspaceCorrupted
        }

        let lookup = try findEntry(
            fingerprint: fingerprint,
            identity: identity
        )
        if let index = lookup.index {
            guard let reservation else {
                throw CompositionQueryError.workspaceCorrupted
            }
            let mergePlan = try Self.mergedContributorPlan(
                entries[index].contributors,
                contributorInput
            )
            guard mergePlan.addsContributor else {
                previousSequence = sequence
                return
            }
            let oldBytes = try Self.contributorFootprint(
                .multiple(entries[index].contributors)
            ).retainedByteCount
            let nextPayloadBytes = try Self.replacing(
                consumed: retainedPayloadBytes,
                removed: oldBytes,
                inserted: mergePlan.retainedByteCount,
                maximum: maximumPayloadBytes
            )
            let mergedReservation = try workMeter.reserveIntermediate(
                bytes: mergePlan.retainedByteCount,
                at: .deduplication
            )
            let merged = Self.mergeContributors(
                entries[index].contributors,
                contributorInput,
                capacity: mergePlan.capacity
            )
            guard merged.count == mergePlan.count,
                  try Self.contributorFootprint(.multiple(merged))
                    .retainedByteCount == mergePlan.retainedByteCount else {
                mergedReservation.release()
                throw CompositionQueryError.workspaceCorrupted
            }
            do {
                try reservation.absorbAll(from: mergedReservation)
            } catch {
                mergedReservation.release()
                throw error
            }
            try replaceContributors(
                at: index,
                with: consume merged
            )
            reservation.releaseGuaranteedPartial(bytes: oldBytes)
            retainedPayloadBytes = nextPayloadBytes
            previousSequence = sequence
            return
        }

        guard lookup.collisionCount < Self.maximumDigestCollisionRecords else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let contributorPlan = try Self.contributorFootprint(contributorInput)
        let entryLayout = try DatabaseRetainedArrayLayout.forElement(Entry.self)
        let requiredEntryCount = try Self.incremented(entries.count)
        let entryReplacement = try makeEntryReplacementPlan(
            layout: entryLayout,
            requiredEntryCount: requiredEntryCount
        )
        let entryOwnerBytes = reservation == nil
            ? entryLayout.containerByteCount
            : 0
        let slotReplacement = try makeSlotReplacementPlan(
            requiredEntryCount: requiredEntryCount
        )

        var finalPayloadBytes = retainedPayloadBytes
        if let entryReplacement {
            finalPayloadBytes = try Self.replacing(
                consumed: finalPayloadBytes,
                removed: entryReplacement.oldFootprint,
                inserted: entryReplacement.newFootprint,
                maximum: maximumPayloadBytes
            )
        }
        if let slotReplacement {
            finalPayloadBytes = try Self.replacing(
                consumed: finalPayloadBytes,
                removed: slotReplacement.oldFootprint,
                inserted: slotReplacement.newFootprint,
                maximum: maximumPayloadBytes
            )
        }
        var retainedEntryBytes = try Self.adding(
            ownedRow.footprint.bytes,
            identityClaimBytes
        )
        retainedEntryBytes = try Self.adding(
            retainedEntryBytes,
            contributorPlan.retainedByteCount
        )
        retainedEntryBytes = try Self.adding(
            retainedEntryBytes,
            entryOwnerBytes
        )
        let nextPayloadBytes = try Self.admit(
            consumed: finalPayloadBytes,
            requested: retainedEntryBytes,
            maximum: maximumPayloadBytes
        )

        var additionalAdmissionBytes = try Self.adding(
            contributorPlan.retainedByteCount,
            entryOwnerBytes
        )
        if let entryReplacement {
            additionalAdmissionBytes = try Self.adding(
                additionalAdmissionBytes,
                entryReplacement.newFootprint
            )
        }
        if let slotReplacement {
            additionalAdmissionBytes = try Self.adding(
                additionalAdmissionBytes,
                slotReplacement.newFootprint
            )
        }
        try identityReservation.reserveAdditional(
            bytes: additionalAdmissionBytes,
            at: .deduplication
        )

        let replacementSlots = try slotReplacement.map {
            try materializeSlotReplacement(capacity: $0.capacity)
        }

        let contributors = Self.materializeContributors(
            contributorInput,
            capacity: contributorPlan.capacity
        )
        guard contributors.count == contributorPlan.count,
              try Self.contributorFootprint(.multiple(contributors))
                .retainedByteCount == contributorPlan.retainedByteCount else {
            throw CompositionQueryError.workspaceCorrupted
        }
        let entry = Entry(
            fingerprint: fingerprint,
            identity: identity,
            ownedRow: ownedRow,
            contributors: contributors
        )

        if let reservation {
            try reservation.absorbAll(from: identityReservation)
        } else {
            self.reservation = identityReservation
        }
        identityWasAdopted = true
        guard let activeReservation = self.reservation else {
            throw CompositionQueryError.workspaceCorrupted
        }

        if let entryReplacement {
            entries.reserveCapacity(entryReplacement.capacity)
            accountedEntryCapacity = entryReplacement.capacity
            activeReservation.releaseGuaranteedPartial(
                bytes: entryReplacement.oldFootprint
            )
        }
        if let slotReplacement, let replacementSlots {
            slots = replacementSlots
            activeReservation.releaseGuaranteedPartial(
                bytes: slotReplacement.oldFootprint
            )
        }
        entries.append(entry)
        try placeEntryIndex(entries.count - 1)
        retainedPayloadBytes = nextPayloadBytes
        previousSequence = sequence
    }

    package func forEachResult(
        batchSize: Int,
        _ body: @Sendable (Result) async throws -> Bool
    ) async throws {
        guard batchSize > 0, state == .accumulating,
              let activeReservation = reservation else {
            throw CompositionQueryError.workspaceCorrupted
        }
        state = .reading
        defer { withExtendedLifetime(activeReservation) {} }
        var index = 0
        while index < entries.count {
            guard state == .reading else {
                throw CompositionQueryError.workspaceCorrupted
            }
            let entry = entries[index]
            let origin: CompositionOrigin = entry.contributors.count == 1
                ? .source(entry.contributors[0])
                : .derived(contributors: entry.contributors)
            let result = entry.ownedRow.withRow {
                Result(row: $0, origin: origin)
            }
            guard try await body(result) else { return }
            index += 1
            if index % batchSize == 0 {
                await Task.yield()
            }
        }
    }

    package func removeAll() {
        state = .removed
        let retainedReservation = reservation
        reservation = nil
        entries.removeAll(keepingCapacity: false)
        slots.removeAll(keepingCapacity: false)
        accountedEntryCapacity = 0
        previousSequence = nil
        retainedPayloadBytes = 0
        withExtendedLifetime(retainedReservation) {}
    }

    private func findEntry(
        fingerprint: ByteString,
        identity: ByteString
    ) throws -> (index: Int?, collisionCount: Int) {
        guard !slots.isEmpty else { return (nil, 0) }
        var slot = Self.slotIndex(for: fingerprint, capacity: slots.count)
        var collisionCount = 0
        for _ in 0..<slots.count {
            guard let entryIndex = slots[slot] else {
                return (nil, collisionCount)
            }
            guard entries.indices.contains(entryIndex) else {
                throw CompositionQueryError.workspaceCorrupted
            }
            let entry = entries[entryIndex]
            if entry.fingerprint == fingerprint {
                if entry.identity == identity {
                    return (entryIndex, collisionCount)
                }
                collisionCount += 1
            }
            slot = (slot + 1) & (slots.count - 1)
        }
        throw CompositionQueryError.workspaceCorrupted
    }

    private func replaceContributors(
        at index: Int,
        with contributors: consuming [Base.ID]
    ) throws {
        guard entries.indices.contains(index) else {
            throw CompositionQueryError.workspaceCorrupted
        }
        entries[index].contributors = consume contributors
    }

    private func makeEntryReplacementPlan(
        layout: DatabaseRetainedArrayLayout,
        requiredEntryCount: Int
    ) throws -> ArrayStorageReplacement? {
        let growth = try layout.growth(
            from: accountedEntryCapacity,
            toFit: requiredEntryCount
        )
        guard growth.capacity != accountedEntryCapacity else { return nil }
        return ArrayStorageReplacement(
            capacity: growth.capacity,
            oldFootprint: try Self.entryStorageFootprint(
                capacity: accountedEntryCapacity,
                layout: layout
            ),
            newFootprint: try Self.entryStorageFootprint(
                capacity: growth.capacity,
                layout: layout
            )
        )
    }

    private func makeSlotReplacementPlan(
        requiredEntryCount: Int
    ) throws -> ArrayStorageReplacement? {
        let targetCapacity = try Self.slotCapacity(
            current: slots.count,
            requiredEntryCount: requiredEntryCount
        )
        guard targetCapacity != slots.count else { return nil }
        let oldFootprint = try Self.slotStorageFootprint(capacity: slots.count)
        let newFootprint = try Self.slotStorageFootprint(capacity: targetCapacity)
        return ArrayStorageReplacement(
            capacity: targetCapacity,
            oldFootprint: oldFootprint,
            newFootprint: newFootprint
        )
    }

    private func materializeSlotReplacement(
        capacity: Int
    ) throws -> [Int?] {
        var replacement = Array<Int?>(repeating: nil, count: capacity)
        for index in entries.indices {
            try Self.place(
                index,
                fingerprint: entries[index].fingerprint,
                in: &replacement
            )
        }
        return replacement
    }

    private func placeEntryIndex(_ index: Int) throws {
        guard entries.indices.contains(index) else {
            throw CompositionQueryError.workspaceCorrupted
        }
        try Self.place(
            index,
            fingerprint: entries[index].fingerprint,
            in: &slots
        )
    }

    private static func place(
        _ index: Int,
        fingerprint: ByteString,
        in slots: inout [Int?]
    ) throws {
        guard !slots.isEmpty else {
            throw CompositionQueryError.workspaceCorrupted
        }
        var slot = slotIndex(for: fingerprint, capacity: slots.count)
        for _ in 0..<slots.count {
            if slots[slot] == nil {
                slots[slot] = index
                return
            }
            slot = (slot + 1) & (slots.count - 1)
        }
        throw CompositionQueryError.workspaceCorrupted
    }

    private static func slotIndex(
        for fingerprint: ByteString,
        capacity: Int
    ) -> Int {
        precondition(capacity > 0 && capacity.isPowerOfTwo)
        return Int(UInt(bitPattern: fingerprint.hashValue) & UInt(capacity - 1))
    }

    private static func slotCapacity(
        current: Int,
        requiredEntryCount: Int
    ) throws -> Int {
        var capacity = max(initialSlotCapacity, current)
        while requiredEntryCount > capacity / 2 {
            let next = capacity.multipliedReportingOverflow(by: 2)
            guard !next.overflow else {
                throw CompositionQueryError.workspaceCorrupted
            }
            capacity = next.partialValue
        }
        return capacity
    }

    private static func slotStorageFootprint(capacity: Int) throws -> UInt64 {
        guard capacity > 0 else { return 0 }
        let layout = try DatabaseRetainedArrayLayout.forElement(Int?.self)
        let growth = try layout.growth(from: 0, toFit: capacity)
        return try adding(layout.containerByteCount, growth.additionalByteCount)
    }

    private static func entryStorageFootprint(
        capacity: Int,
        layout: DatabaseRetainedArrayLayout
    ) throws -> UInt64 {
        guard capacity > 0 else { return 0 }
        return try layout.growth(
            from: 0,
            toFit: capacity
        ).additionalByteCount
    }

    private static func makeFieldFramePlan(
        _ values: [String: FieldValue],
        entity: String,
        workMeter: DatabaseWorkMeter
    ) throws -> FieldFramePlan {
        let layout = try DatabaseRetainedArrayLayout.forElement(
            PersistableField.self
        )
        let growth = try layout.growth(from: 0, toFit: values.count)
        let scratchBytes = try adding(
            layout.containerByteCount,
            growth.additionalByteCount
        )
        let scratchReservation = try workMeter.reserveIntermediate(
            bytes: scratchBytes,
            at: .deduplication
        )
        do {
            var fields: [PersistableField] = []
            fields.reserveCapacity(growth.capacity)
            for (name, value) in values {
                fields.append(
                    try PersistableField(number: 1, name: name, value: value)
                )
            }
            fields.sort { $0.name < $1.name }
            for index in fields.indices {
                guard let number = UInt32(exactly: index + 1) else {
                    throw CompositionQueryError.workspaceCorrupted
                }
                let field = fields[index]
                fields[index] = try PersistableField(
                    number: number,
                    name: field.name,
                    value: field.value
                )
            }
            let encodedByteCount = try PersistableFieldFrameCodec
                .encodedByteCount(
                    magic: identityMagic,
                    version: identityVersion,
                    entity: entity,
                    fields: fields
                )
            guard let encodedByteCount = UInt64(exactly: encodedByteCount)
            else {
                throw CompositionQueryError.workspaceCorrupted
            }
            return FieldFramePlan(
                fields: fields,
                encodedByteCount: encodedByteCount,
                scratchReservation: scratchReservation
            )
        } catch {
            scratchReservation.release()
            throw error
        }
    }

    private static func fingerprint(_ identity: ByteString) -> ByteString {
        var hasher = SHA256Accumulator()
        hasher.update(identity)
        return hasher.finalize()
    }

    private static func validatedContributorInput(
        _ origin: CompositionOrigin
    ) throws -> ContributorInput {
        switch origin {
        case .source(let baseID):
            return .single(baseID)
        case .derived(let values):
            guard !values.isEmpty else {
                throw CompositionQueryError.workspaceCorrupted
            }
            for (previous, current) in zip(values, values.dropFirst()) {
                guard previous < current else {
                    throw CompositionQueryError.workspaceCorrupted
                }
            }
            return .multiple(values)
        }
    }

    private static func contributorFootprint(
        _ input: ContributorInput
    ) throws -> ContributorPlan {
        let layout = try DatabaseRetainedArrayLayout.forElement(Base.ID.self)
        let growth = try layout.growth(from: 0, toFit: input.count)
        var bytes = try adding(
            layout.containerByteCount,
            growth.additionalByteCount
        )
        for index in 0..<input.count {
            bytes = try adding(bytes, UInt64(input[index].value.utf8.count))
        }
        return ContributorPlan(
            count: input.count,
            capacity: growth.capacity,
            retainedByteCount: bytes,
            addsContributor: input.count > 0
        )
    }

    private static func mergedContributorPlan(
        _ lhs: borrowing [Base.ID],
        _ rhs: ContributorInput
    ) throws -> ContributorPlan {
        var left = 0
        var right = 0
        var count = 0
        var utf8Bytes: UInt64 = 0
        var addsContributor = false
        while left < lhs.count || right < rhs.count {
            let value: Base.ID
            if right >= rhs.count
                || (left < lhs.count && lhs[left] < rhs[right]) {
                value = lhs[left]
                left += 1
            } else if left >= lhs.count || rhs[right] < lhs[left] {
                value = rhs[right]
                right += 1
                addsContributor = true
            } else {
                value = lhs[left]
                left += 1
                right += 1
            }
            count = try incremented(count)
            utf8Bytes = try adding(
                utf8Bytes,
                UInt64(value.value.utf8.count)
            )
        }
        let layout = try DatabaseRetainedArrayLayout.forElement(Base.ID.self)
        let growth = try layout.growth(from: 0, toFit: count)
        let retainedByteCount = try adding(
            try adding(
                layout.containerByteCount,
                growth.additionalByteCount
            ),
            utf8Bytes
        )
        return ContributorPlan(
            count: count,
            capacity: growth.capacity,
            retainedByteCount: retainedByteCount,
            addsContributor: addsContributor
        )
    }

    private static func materializeContributors(
        _ input: ContributorInput,
        capacity: Int
    ) -> [Base.ID] {
        var result: [Base.ID] = []
        result.reserveCapacity(capacity)
        for index in 0..<input.count {
            result.append(input[index])
        }
        return result
    }

    private static func mergeContributors(
        _ lhs: borrowing [Base.ID],
        _ rhs: ContributorInput,
        capacity: Int
    ) -> [Base.ID] {
        var merged: [Base.ID] = []
        merged.reserveCapacity(capacity)
        var left = 0
        var right = 0
        while left < lhs.count || right < rhs.count {
            if right >= rhs.count
                || (left < lhs.count && lhs[left] < rhs[right]) {
                merged.append(lhs[left])
                left += 1
            } else if left >= lhs.count || rhs[right] < lhs[left] {
                merged.append(rhs[right])
                right += 1
            } else {
                merged.append(lhs[left])
                left += 1
                right += 1
            }
        }
        return merged
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

    private static func replacing(
        consumed: UInt64,
        removed: UInt64,
        inserted: UInt64,
        maximum: UInt64
    ) throws -> UInt64 {
        guard removed <= consumed else {
            throw CompositionQueryError.workspaceCorrupted
        }
        return try admit(
            consumed: consumed - removed,
            requested: inserted,
            maximum: maximum
        )
    }

    private static func incremented(_ value: Int) throws -> Int {
        let result = value.addingReportingOverflow(1)
        guard !result.overflow else {
            throw CompositionQueryError.workspaceCorrupted
        }
        return result.partialValue
    }

    private static func adding(_ lhs: UInt64, _ rhs: UInt64) throws -> UInt64 {
        let result = lhs.addingReportingOverflow(rhs)
        guard !result.overflow else {
            throw CompositionQueryError.workspaceCorrupted
        }
        return result.partialValue
    }
}

private extension Int {
    var isPowerOfTwo: Bool { self > 0 && (self & (self - 1)) == 0 }
}

#endif
