import DatabaseKit
import DatabaseTypes
import StorageKit

/// Engine-owned immutable candidate rows for one Fusion stage.
///
/// Feature modules can inspect only canonical primary keys. Materialized rows,
/// identity values, and retained-storage ownership remain inside DatabaseEngine.
package struct FusionCandidateDomain: Sendable {
    struct Entry: Sendable {
        let identity: FieldValue
        let packedPrimaryKey: ByteString
        let row: QueryRow
        let rowFootprint: DatabaseIntermediateFootprint
        let retainedFootprint: DatabaseIntermediateFootprint
    }

    private let entries: DatabaseSharedRetainedArray<Entry>

    init(entries: DatabaseSharedRetainedArray<Entry>) {
        self.entries = entries
    }

    package var count: Int { entries.count }
    package var isEmpty: Bool { entries.isEmpty }

    package func primaryKey(at index: Int) -> ByteString {
        entries[index].packedPrimaryKey
    }

    func withEntry<Result>(
        at index: Int,
        _ body: (borrowing Entry) throws -> Result
    ) rethrows -> Result {
        try entries.withElement(at: index, body)
    }

    package func contains(
        primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> Bool {
        try insertionIndex(
            for: primaryKey,
            workMeter: workMeter
        ).match != nil
    }

    static func empty(
        workMeter: DatabaseWorkMeter
    ) throws -> FusionCandidateDomain {
        let builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self)
        )
        return FusionCandidateDomain(
            entries: try builder.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
        )
    }

    func entry(
        for primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> Entry? {
        guard let index = try insertionIndex(
            for: primaryKey,
            workMeter: workMeter
        ).match else {
            return nil
        }
        return entries[index]
    }

    func index(
        forPrimaryKey primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> Int? {
        try insertionIndex(for: primaryKey, workMeter: workMeter).match
    }

    func forEachEntry(
        _ body: (borrowing Entry) throws -> Void
    ) rethrows {
        for index in entries.indices {
            try entries.withElement(at: index) { entry in
                try body(entry)
            }
        }
    }

    static func make<Rows: Collection>(
        rows: Rows,
        entity: Schema.Entity,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .bindingCandidate
    ) throws -> FusionCandidateDomain where Rows.Element == QueryRow {
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: rows.count
        )
        for row in rows {
            try workMeter.consume(at: stage)
            guard let identity = row.fields["id"], identity != .null else {
                throw FusionExecutionContractError.missingIdentity(field: "id")
            }
            let primaryKey = try PersistableIdentifierKeyCodec
                .tuple(forPersistedIdentifier: identity)
            _ = try PersistableIdentifierKeyCodec.value(
                from: primaryKey,
                expectedType: entity.identifierType
            )
            let packedPrimaryKeyByteCount = primaryKey.packedByteCount
            let rowFootprint = try CanonicalRelationalFootprintMeter.footprint(
                of: row,
                workMeter: workMeter
            )
            let footprint = try rowFootprint.adding(
                DatabaseIntermediateFootprint(
                    rows: 1,
                    bytes: UInt64(packedPrimaryKeyByteCount) + 64
                )
            )
            try builder.append(footprint: footprint, at: stage) {
                let packedPrimaryKey = primaryKey.pack()
                precondition(
                    packedPrimaryKey.count == packedPrimaryKeyByteCount
                )
                return Entry(
                    identity: identity,
                    packedPrimaryKey: packedPrimaryKey,
                    row: row,
                    rowFootprint: rowFootprint,
                    retainedFootprint: footprint
                )
            }
        }

        let sorted = try builder.finish().sortingElements {
            try workMeter.consume(at: .sortComparison)
            return try primaryKeyOrder(
                $0.packedPrimaryKey,
                $1.packedPrimaryKey,
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
        try sorted.withSpan { entries in
            guard entries.count > 1 else { return }
            for index in 1..<entries.count {
                try workMeter.consume(at: stage)
                let previous = entries[index - 1]
                let current = entries[index]
                guard try primaryKeyOrder(
                    previous.packedPrimaryKey,
                    current.packedPrimaryKey,
                    workMeter: workMeter,
                    stage: stage
                ) != 0 else {
                    if previous.identity == current.identity {
                        throw FusionExecutionContractError.duplicateIdentity(
                            current.identity
                        )
                    }
                    throw FusionExecutionContractError.duplicatePrimaryKey(
                        current.packedPrimaryKey
                    )
                }
            }
        }
        return FusionCandidateDomain(
            entries: try sorted.moveToSharedOwnership(at: stage)
        )
    }

    static func make(
        selecting primaryKeys: some Collection<ByteString>,
        from source: FusionCandidateDomain,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .bindingCandidate
    ) throws -> FusionCandidateDomain {
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: stage,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: primaryKeys.count
        )
        for primaryKey in primaryKeys {
            try workMeter.consume(at: stage)
            guard let entry = try source.entry(
                for: primaryKey,
                workMeter: workMeter
            ) else {
                throw FusionExecutionContractError.missingCandidateRow(primaryKey)
            }
            try builder.append(
                footprint: entry.retainedFootprint,
                at: stage,
                make: { entry }
            )
        }
        let sorted = try builder.finish().sortingElements {
            try workMeter.consume(at: .sortComparison)
            return try primaryKeyOrder(
                $0.packedPrimaryKey,
                $1.packedPrimaryKey,
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
        return FusionCandidateDomain(entries: try sorted.moveToSharedOwnership(
            at: stage
        ))
    }

    static func make(
        models: DatabaseRetainedPersistedModels,
        primaryKeys: [Tuple],
        entity: Schema.Entity,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionCandidateDomain {
        guard models.count == primaryKeys.count else {
            throw FusionExecutionContractError.entityReadCountMismatch(
                expected: primaryKeys.count,
                actual: models.count
            )
        }
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .storageRow,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: models.count
        )
        for index in primaryKeys.indices {
            try workMeter.consume(at: .storageRow)
            let primaryKey = primaryKeys[index]
            let packedPrimaryKeyByteCount = primaryKey.packedByteCount
            try models.withEntry(at: index) { retained in
                guard let retained else {
                    throw FusionExecutionContractError.missingCandidateRow(
                        primaryKey.pack()
                    )
                }
                let footprint = try retained.queryRowFootprint.adding(
                    DatabaseIntermediateFootprint(
                        bytes: UInt64(packedPrimaryKeyByteCount) + 64
                    )
                )
                try builder.append(footprint: footprint) {
                    let packedPrimaryKey = primaryKey.pack()
                    precondition(
                        packedPrimaryKey.count == packedPrimaryKeyByteCount
                    )
                    var candidate: Entry?
                    try retained.withModel { model in
                        let row = try QueryRowCodec.encode(model)
                        guard let identity = row.fields["id"],
                              identity != .null else {
                            throw FusionExecutionContractError
                                .missingIdentity(field: "id")
                        }
                        let actualPrimaryKey = try PersistableIdentifierKeyCodec
                            .tuple(forPersistedIdentifier: identity)
                        _ = try PersistableIdentifierKeyCodec.value(
                            from: actualPrimaryKey,
                            expectedType: entity.identifierType
                        )
                        // Structural comparison avoids materializing a second
                        // packed identifier while the retained key is alive.
                        guard actualPrimaryKey == primaryKey else {
                            throw FusionExecutionContractError
                                .inconsistentPayload(packedPrimaryKey)
                        }
                        candidate = Entry(
                            identity: identity,
                            packedPrimaryKey: packedPrimaryKey,
                            row: row,
                            rowFootprint: retained.queryRowFootprint,
                            retainedFootprint: footprint
                        )
                    }
                    guard let candidate else {
                        preconditionFailure(
                            "Scoped retained model did not produce a candidate"
                        )
                    }
                    return candidate
                }
            }
        }
        let sorted = try builder.finish().sortingElements {
            try workMeter.consume(at: .sortComparison)
            return try primaryKeyOrder(
                $0.packedPrimaryKey,
                $1.packedPrimaryKey,
                workMeter: workMeter,
                stage: .sortComparison
            ) < 0
        }
        return FusionCandidateDomain(entries: try sorted.moveToSharedOwnership(
            at: .storageRow
        ))
    }

    func union(
        _ other: FusionCandidateDomain,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionCandidateDomain {
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: try checkedCombinedCount(other)
        )
        var left = entries.startIndex
        var right = other.entries.startIndex
        while left < entries.endIndex || right < other.entries.endIndex {
            try workMeter.consume(at: .bindingCandidate)
            let selected: Entry
            if left == entries.endIndex {
                selected = other.entries[right]
                right += 1
            } else if right == other.entries.endIndex {
                selected = entries[left]
                left += 1
            } else {
                let lhs = entries[left]
                let rhs = other.entries[right]
                let order = try Self.primaryKeyOrder(
                    lhs.packedPrimaryKey,
                    rhs.packedPrimaryKey,
                    workMeter: workMeter,
                    stage: .bindingCandidate
                )
                if order == 0 {
                    try Self.requireEquivalentPayload(
                        lhs,
                        rhs,
                        workMeter: workMeter
                    )
                    selected = lhs
                    left += 1
                    right += 1
                } else if order < 0 {
                    selected = lhs
                    left += 1
                } else {
                    selected = rhs
                    right += 1
                }
            }
            try builder.append(
                footprint: selected.retainedFootprint,
                make: { selected }
            )
        }
        return FusionCandidateDomain(
            entries: try builder.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
        )
    }

    func intersection(
        _ other: FusionCandidateDomain,
        workMeter: DatabaseWorkMeter
    ) throws -> FusionCandidateDomain {
        var builder = try DatabaseRetainedArrayBuilder<Entry>(
            workMeter: workMeter,
            stage: .bindingCandidate,
            layout: try DatabaseRetainedArrayLayout.forElement(Entry.self),
            expectedCount: min(count, other.count)
        )
        var left = entries.startIndex
        var right = other.entries.startIndex
        while left < entries.endIndex, right < other.entries.endIndex {
            try workMeter.consume(at: .bindingCandidate)
            let lhs = entries[left]
            let rhs = other.entries[right]
            let order = try Self.primaryKeyOrder(
                lhs.packedPrimaryKey,
                rhs.packedPrimaryKey,
                workMeter: workMeter,
                stage: .bindingCandidate
            )
            if order == 0 {
                try Self.requireEquivalentPayload(
                    lhs,
                    rhs,
                    workMeter: workMeter
                )
                try builder.append(
                    footprint: lhs.retainedFootprint,
                    make: { lhs }
                )
                left += 1
                right += 1
            } else if order < 0 {
                left += 1
            } else {
                right += 1
            }
        }
        return FusionCandidateDomain(
            entries: try builder.finish().moveToSharedOwnership(
                at: .bindingCandidate
            )
        )
    }

    private func checkedCombinedCount(
        _ other: FusionCandidateDomain
    ) throws -> Int {
        let (combined, overflow) = count.addingReportingOverflow(other.count)
        guard !overflow else {
            throw FusionExecutionContractError.candidateCountOverflow
        }
        return combined
    }

    private func insertionIndex(
        for primaryKey: ByteString,
        workMeter: DatabaseWorkMeter
    ) throws -> (index: Int, match: Int?) {
        var lower = entries.startIndex
        var upper = entries.endIndex
        while lower < upper {
            try workMeter.consume(at: .bindingCandidate)
            let middle = lower + (upper - lower) / 2
            let candidate = entries[middle].packedPrimaryKey
            let order = try Self.primaryKeyOrder(
                candidate,
                primaryKey,
                workMeter: workMeter,
                stage: .bindingCandidate
            )
            if order == 0 {
                return (middle, middle)
            }
            if order < 0 {
                lower = middle + 1
            } else {
                upper = middle
            }
        }
        return (lower, nil)
    }

    /// Compares retained primary-key bytes without materializing either side.
    /// The full possible scan is charged before the synchronous borrow.
    static func primaryKeyOrder(
        _ lhs: ByteString,
        _ rhs: ByteString,
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage
    ) throws -> Int {
        try DatabaseByteProcessingMeter.consume(
            byteCount: max(lhs.count, rhs.count),
            workMeter: workMeter,
            stage: stage
        )
        return lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                let sharedCount = min(lhsBytes.count, rhsBytes.count)
                for index in 0..<sharedCount {
                    if lhsBytes[index] < rhsBytes[index] { return -1 }
                    if lhsBytes[index] > rhsBytes[index] { return 1 }
                }
                if lhsBytes.count < rhsBytes.count { return -1 }
                if lhsBytes.count > rhsBytes.count { return 1 }
                return 0
            }
        }
    }

    private static func requireEquivalentPayload(
        _ lhs: borrowing Entry,
        _ rhs: borrowing Entry,
        workMeter: DatabaseWorkMeter
    ) throws {
        try DatabaseByteProcessingMeter.consume(
            byteCount: max(
                lhs.rowFootprint.bytes,
                rhs.rowFootprint.bytes
            ),
            workMeter: workMeter,
            stage: .bindingCandidate
        )
        guard lhs.identity == rhs.identity,
              lhs.row.fields == rhs.row.fields,
              lhs.row.annotations == rhs.row.annotations,
              lhs.row.version == rhs.row.version else {
            throw FusionExecutionContractError.inconsistentPayload(
                lhs.packedPrimaryKey
            )
        }
    }
}
