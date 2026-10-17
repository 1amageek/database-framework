import DatabaseKit
import DatabaseTypes

/// Preserves the first-seen order and net effect of transaction mutations.
struct TransactionMutationJournal: Sendable {
    private struct Entry: Sendable {
        let identity: EntityReference
        let originallyExisted: Bool
        var currentModel: PersistedModel?
        var currentFootprint: DatabaseIntermediateFootprint
        let payloadReservation: DatabaseIntermediateReservation?
    }

    private struct Accounting: Sendable {
        let workMeter: DatabaseWorkMeter
        let reservation: DatabaseIntermediateReservation
        let entryLayout: DatabaseRetainedArrayLayout
        let lookupLayout: DatabaseRetainedHashTableLayout
        var entryCapacity: Int
        var lookupCapacity: Int
        let undoLayout: DatabaseRetainedArrayLayout
        var undoCapacity: Int
    }

    private struct LookupSlot: Sendable {
        let identity: EntityReference
        let entryIndex: Int
    }

    private var entries: [Entry] = []
    private var lookup: [EntityReference: Int] = [:]
    private var accounting: Accounting?
    private struct Undo: Sendable {
        let index: Int
        let previous: Entry?
    }
    private var undo: [Undo] = []
    private var recordsUndo = false

    mutating func checkpointPosition() -> Int {
        recordsUndo = true
        return undo.count
    }

    var undoCount: Int { undo.count }

    mutating func rollback(to position: Int) {
        precondition(position >= 0 && position <= undo.count)
        while undo.count > position {
            let change = undo.removeLast()
            if let previous = change.previous {
                entries[change.index] = previous
            } else {
                precondition(change.index == entries.count - 1)
                lookup.removeValue(forKey: entries.removeLast().identity)
            }
        }
    }

    mutating func releaseCheckpointHistory() {
        undo.removeAll(keepingCapacity: false)
        recordsUndo = false
        if var accounting {
            accounting.reservation.releaseGuaranteedPartial(
                bytes: UInt64(accounting.undoCapacity) * accounting.undoLayout.elementCapacitySlotByteCount
            )
            accounting.undoCapacity = 0
            self.accounting = accounting
        }
    }

    var workMeter: DatabaseWorkMeter? { accounting?.workMeter }

    mutating func bind(to workMeter: DatabaseWorkMeter) throws {
        if let accounting {
            guard accounting.workMeter === workMeter else {
                throw DatabaseTransactionMutationError.workMeterMismatch
            }
            return
        }
        guard entries.isEmpty else {
            throw DatabaseTransactionMutationError.workMeterBoundAfterMutation
        }

        let entryLayout = try DatabaseRetainedArrayLayout.forElement(Entry.self)
        let lookupLayout = try DatabaseRetainedHashTableLayout.validated(
            containerByteCount: UInt64(
                MemoryLayout<[EntityReference: Int]>.stride
            ),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<LookupSlot>.stride)
            )
        )
        let entryGrowth = try entryLayout.growth(
            from: 0,
            toFit: entries.count
        )
        let lookupGrowth = try lookupLayout.growth(
            from: 0,
            toFit: lookup.count
        )
        let footprint = try DatabaseIntermediateFootprint(
            bytes: entryLayout.containerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: lookupLayout.containerByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<[Undo]>.stride))
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: entryGrowth.additionalByteCount
            )
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: lookupGrowth.additionalByteCount
            )
        )
        let reservation = try workMeter.reserveIntermediate(
            rows: footprint.rows,
            bytes: footprint.bytes,
            at: .mutationPlanning
        )
        entries.reserveCapacity(entryGrowth.capacity)
        lookup.reserveCapacity(lookupGrowth.capacity)
        accounting = Accounting(
            workMeter: workMeter,
            reservation: reservation,
            entryLayout: entryLayout,
            lookupLayout: lookupLayout,
            entryCapacity: entryGrowth.capacity,
            lookupCapacity: lookupGrowth.capacity,
            undoLayout: try DatabaseRetainedArrayLayout.forElement(Undo.self),
            undoCapacity: 0
        )
    }

    mutating func record(
        identity: EntityReference,
        previousModel: PersistedModel?,
        currentModel: PersistedModel?
    ) throws {
        let existingIndex = lookup[identity]
        let index = existingIndex ?? entries.count
        let previous = existingIndex.map { entries[$0] }
        let footprint: DatabaseIntermediateFootprint
        let payloadReservation: DatabaseIntermediateReservation?
        if var accounting {
            footprint = try Self.currentFootprint(identity: identity, model: currentModel,
                workMeter: accounting.workMeter)
            let previousFootprint = previous?.currentFootprint ?? DatabaseIntermediateFootprint()
            let sharesPrevious = !recordsUndo && previous?.payloadReservation != nil
            payloadReservation = try accounting.reservation.reserveChild(
                rows: sharesPrevious ? max(footprint.rows, previousFootprint.rows) - previousFootprint.rows : footprint.rows,
                bytes: sharesPrevious ? max(footprint.bytes, previousFootprint.bytes) - previousFootprint.bytes : footprint.bytes,
                at: .mutationPlanning)
            let requiredEntries = entries.count + (existingIndex == nil ? 1 : 0)
            let entryGrowth = try accounting.entryLayout.growth(from: accounting.entryCapacity, toFit: requiredEntries)
            let lookupGrowth = try accounting.lookupLayout.growth(from: accounting.lookupCapacity, toFit: requiredEntries)
            let undoGrowth = try accounting.undoLayout.growth(from: accounting.undoCapacity,
                toFit: undo.count + (recordsUndo ? 1 : 0))
            let growth = try DatabaseIntermediateFootprint(bytes: entryGrowth.additionalByteCount)
                .adding(DatabaseIntermediateFootprint(bytes: lookupGrowth.additionalByteCount))
                .adding(DatabaseIntermediateFootprint(bytes: undoGrowth.additionalByteCount))
            try accounting.reservation.reserveAdditional(bytes: growth.bytes, at: .mutationPlanning)
            if entryGrowth.capacity != accounting.entryCapacity { entries.reserveCapacity(entryGrowth.capacity) }
            if lookupGrowth.capacity != accounting.lookupCapacity { lookup.reserveCapacity(lookupGrowth.capacity) }
            if undoGrowth.capacity != accounting.undoCapacity { undo.reserveCapacity(undoGrowth.capacity) }
            accounting.entryCapacity = entryGrowth.capacity
            accounting.lookupCapacity = lookupGrowth.capacity
            accounting.undoCapacity = undoGrowth.capacity
            self.accounting = accounting
            if sharesPrevious, let previousReservation = previous?.payloadReservation,
               let payloadReservation {
                // These claims move only after every fallible admission has succeeded.
                payloadReservation.absorbGuaranteedPartial(from: previousReservation,
                    rows: min(footprint.rows, previousFootprint.rows),
                    bytes: min(footprint.bytes, previousFootprint.bytes))
            }
        } else {
            footprint = DatabaseIntermediateFootprint()
            payloadReservation = nil
        }
        if recordsUndo { undo.append(Undo(index: index, previous: previous)) }
        let entry = Entry(identity: identity,
            originallyExisted: previous?.originallyExisted ?? (previousModel != nil),
            currentModel: currentModel, currentFootprint: footprint,
            payloadReservation: payloadReservation)
        if existingIndex != nil {
            entries[index] = entry
        } else {
            entries.append(entry)
            lookup[identity] = index
        }
    }

    func currentVersion(for identity: EntityReference) throws -> ByteString? {
        guard let index = lookup[identity], let model = entries[index].currentModel else { return nil }
        return try PersistableVersionTokenCodec.digest(fields: model.fields)
    }

    func currentModels() -> [PersistedModel] {
        entries.compactMap { $0.currentModel }
    }

    func retainedCurrentModels() throws
        -> DatabaseSharedRetainedArray<PersistedModel>? {
        guard let accounting else { return nil }
        var models = try DatabaseRetainedArrayBuilder<PersistedModel>(
            workMeter: accounting.workMeter,
            stage: .mutationPlanning,
            layout: try DatabaseRetainedArrayLayout.forElement(PersistedModel.self),
            expectedCount: currentModelCount
        )
        for entry in entries {
            guard let model = entry.currentModel else { continue }
            // The journal continues to own and account for the model payload
            // throughout final validation. This Array adds only COW value slots;
            // charging the payload again would reject a valid request solely
            // because the maintainer API requires a contiguous borrowed view.
            try models.append(
                footprint: DatabaseIntermediateFootprint(),
                make: { model }
            )
        }
        return try models.finish().moveToSharedOwnership(
            at: .mutationPlanning
        )
    }

    var persistedEffectCount: Int {
        entries.reduce(into: 0) { count, entry in
            if entry.originallyExisted || entry.currentModel != nil {
                count += 1
            }
        }
    }

    private var currentModelCount: Int {
        entries.reduce(into: 0) { count, entry in
            if entry.currentModel != nil {
                count += 1
            }
        }
    }

    func persistedEffects() -> [PersistableMutationEffect] {
        mapPersistedEffects { $0 }
    }

    func mapPersistedEffects<Output>(
        _ transform: (PersistableMutationEffect) throws -> Output
    ) rethrows -> [Output] {
        var output: [Output] = []
        output.reserveCapacity(persistedEffectCount)
        for entry in entries {
            if let effect = Self.persistedEffect(for: entry) {
                output.append(try transform(effect))
            }
        }
        return output
    }

    mutating func removeAll() {
        entries.removeAll()
        lookup.removeAll()
        undo.removeAll()
        recordsUndo = false
        accounting = nil
    }

    private static func persistedEffect(
        for entry: Entry
    ) -> PersistableMutationEffect? {
        switch (entry.originallyExisted, entry.currentModel) {
        case (false, .some(let model)):
            return PersistableMutationEffect(
                kind: .insert,
                identity: entry.identity,
                model: model
            )
        case (true, .some(let model)):
            return PersistableMutationEffect(
                kind: .update,
                identity: entry.identity,
                model: model
            )
        case (true, nil):
            return PersistableMutationEffect(
                kind: .delete,
                identity: entry.identity,
                model: nil
            )
        case (false, nil):
            return nil
        }
    }

    private static func currentFootprint(
        identity: EntityReference,
        model: PersistedModel?,
        workMeter: DatabaseWorkMeter
    ) throws -> DatabaseIntermediateFootprint {
        guard let model else { return DatabaseIntermediateFootprint() }
        return try DatabaseEntityMutationFootprintMeter.footprint(
            identity: identity,
            model: model,
            workMeter: workMeter
        )
    }

}
