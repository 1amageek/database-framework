import DatabaseKit
import DatabaseTypes

/// Preserves the first-seen order and net effect of transaction mutations.
struct TransactionMutationJournal: Sendable {
    private struct Entry: Sendable {
        let identity: EntityReference
        let originallyExisted: Bool
        var currentModel: PersistedModel?
        var currentFootprint: DatabaseIntermediateFootprint
    }

    private struct Accounting: Sendable {
        let workMeter: DatabaseWorkMeter
        let reservation: DatabaseIntermediateReservation
        let entryLayout: DatabaseRetainedArrayLayout
        let lookupLayout: DatabaseRetainedHashTableLayout
        var entryCapacity: Int
        var lookupCapacity: Int
    }

    private struct LookupSlot: Sendable {
        let identity: EntityReference
        let entryIndex: Int
    }

    private var entries: [Entry] = []
    private var lookup: [EntityReference: Int] = [:]
    private var accounting: Accounting?

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

        let entryLayout = try CanonicalRelationalFootprintMeter
            .retainedArrayLayout(for: Entry.self)
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
            lookupCapacity: lookupGrowth.capacity
        )
    }

    mutating func record(
        identity: EntityReference,
        previousModel: PersistedModel?,
        currentModel: PersistedModel?
    ) throws {
        if let entryIndex = lookup[identity] {
            let newCurrentFootprint: DatabaseIntermediateFootprint
            if let accounting {
                newCurrentFootprint = try Self.currentFootprint(
                    identity: identity,
                    model: currentModel,
                    workMeter: accounting.workMeter
                )
                try Self.reserveGrowth(
                    from: entries[entryIndex].currentFootprint,
                    to: newCurrentFootprint,
                    using: accounting.reservation
                )
            } else {
                newCurrentFootprint = DatabaseIntermediateFootprint()
            }
            let previousFootprint = entries[entryIndex].currentFootprint
            entries[entryIndex].currentModel = currentModel
            entries[entryIndex].currentFootprint = newCurrentFootprint
            if let accounting {
                Self.releaseShrinkage(
                    from: previousFootprint,
                    to: newCurrentFootprint,
                    using: accounting.reservation
                )
            }
            return
        }

        let currentFootprint: DatabaseIntermediateFootprint
        if var accounting {
            currentFootprint = try Self.currentFootprint(
                identity: identity,
                model: currentModel,
                workMeter: accounting.workMeter
            )
            let (requiredCount, countOverflow) = entries.count
                .addingReportingOverflow(1)
            guard !countOverflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: accounting.entryCapacity
                )
            }
            let entryGrowth = try accounting.entryLayout.growth(
                from: accounting.entryCapacity,
                toFit: requiredCount
            )
            let lookupGrowth = try accounting.lookupLayout.growth(
                from: accounting.lookupCapacity,
                toFit: requiredCount
            )
            let growthFootprint = try currentFootprint
                .adding(
                    DatabaseIntermediateFootprint(
                        bytes: entryGrowth.additionalByteCount
                    )
                )
                .adding(
                    DatabaseIntermediateFootprint(
                        bytes: lookupGrowth.additionalByteCount
                    )
                )
            try accounting.reservation.reserveAdditional(
                rows: growthFootprint.rows,
                bytes: growthFootprint.bytes,
                at: .mutationPlanning
            )
            if entryGrowth.capacity != accounting.entryCapacity {
                entries.reserveCapacity(entryGrowth.capacity)
                accounting.entryCapacity = entryGrowth.capacity
            }
            if lookupGrowth.capacity != accounting.lookupCapacity {
                lookup.reserveCapacity(lookupGrowth.capacity)
                accounting.lookupCapacity = lookupGrowth.capacity
            }
            self.accounting = accounting
        } else {
            currentFootprint = DatabaseIntermediateFootprint()
        }

        let entryIndex = entries.count
        entries.append(
            Entry(
                identity: identity,
                originallyExisted: previousModel != nil,
                currentModel: currentModel,
                currentFootprint: currentFootprint
            )
        )
        lookup[identity] = entryIndex
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
            layout: try CanonicalRelationalFootprintMeter
                .retainedArrayLayout(for: PersistedModel.self),
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

    private static func reserveGrowth(
        from previous: DatabaseIntermediateFootprint,
        to replacement: DatabaseIntermediateFootprint,
        using reservation: DatabaseIntermediateReservation
    ) throws {
        try reservation.reserveAdditional(
            rows: replacement.rows > previous.rows
                ? replacement.rows - previous.rows
                : 0,
            bytes: replacement.bytes > previous.bytes
                ? replacement.bytes - previous.bytes
                : 0,
            at: .mutationPlanning
        )
    }

    private static func releaseShrinkage(
        from previous: DatabaseIntermediateFootprint,
        to replacement: DatabaseIntermediateFootprint,
        using reservation: DatabaseIntermediateReservation
    ) {
        reservation.releaseGuaranteedPartial(
            rows: previous.rows > replacement.rows
                ? previous.rows - replacement.rows
                : 0,
            bytes: previous.bytes > replacement.bytes
                ? previous.bytes - replacement.bytes
                : 0
        )
    }
}
