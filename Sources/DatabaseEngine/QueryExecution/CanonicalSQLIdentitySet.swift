/// Task-local relational keys with explicit throwing collision comparisons.
struct CanonicalSQLIdentitySet: ~Copyable {
    private struct Entry {
        let values: [CanonicalSQLValueIdentity]
        let hash: Int
    }
    private var entries: [Entry]
    private var slots: [Int?]
    private var entryCapacity: Int
    private let reservation: DatabaseIntermediateReservation
    private let workMeter: DatabaseWorkMeter
    private let stage: DatabaseWorkStage
    private let forcedHash: Int?

    init(workMeter: DatabaseWorkMeter, stage: DatabaseWorkStage, expectedCount: Int = 0,
        forcedHash: Int? = nil) throws {
        guard expectedCount >= 0 else {
            throw DatabaseRetainedArrayLayoutError.invalidRequiredCount(expectedCount)
        }
        let reservation = try workMeter.reserveIntermediate(bytes:
            UInt64(MemoryLayout<[Entry]>.stride + MemoryLayout<[Int?]>.stride), at: stage)
        var entries: [Entry] = []
        var slots: [Int?] = []
        var capacity = 0
        if expectedCount > 0 {
            let layout = try DatabaseRetainedArrayLayout.forElement(Entry.self)
            let growth = try layout.growth(from: 0, toFit: expectedCount)
            let doubled = growth.capacity.multipliedReportingOverflow(by: 2)
            guard !doubled.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
            let slotCount = max(2, doubled.partialValue)
            let slotBytes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<Int?>.stride))
                .multiplied(by: UInt64(slotCount))
            let pending = try workMeter.reserveIntermediate(bytes: try slotBytes.adding(
                DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)).bytes, at: stage)
            defer { pending.release() }
            slots = [Int?](repeating: nil, count: slotCount)
            entries.reserveCapacity(growth.capacity)
            try reservation.absorbAll(from: pending)
            capacity = growth.capacity
        }
        // Install self only after all fallible admission and allocation have completed.
        self.entries = entries
        self.slots = slots
        self.entryCapacity = capacity
        self.reservation = reservation
        self.workMeter = workMeter
        self.stage = stage
        self.forcedHash = forcedHash
    }

    mutating func insert(_ values: [CanonicalSQLValueIdentity]) throws -> Bool {
        let hash = try CanonicalSQLValueIdentity.hash(values, workMeter: workMeter, stage: stage)
        if try contains(values, hash: hash) { return false }
        try grow(toFit: entries.count + 1)
        let rowLayout = try DatabaseRetainedArrayLayout.forElement(CanonicalSQLValueIdentity.self)
        let rowGrowth = try rowLayout.growth(from: 0, toFit: values.count)
        var payload = try DatabaseIntermediateFootprint(bytes: rowLayout.containerByteCount)
            .adding(DatabaseIntermediateFootprint(bytes: rowGrowth.additionalByteCount))
        for value in values { payload = try payload.adding(value.payloadFootprint(workMeter: workMeter, stage: stage)) }
        let pending = try workMeter.reserveIntermediate(bytes: payload.bytes, at: stage)
        defer { pending.release() }
        var owned: [CanonicalSQLValueIdentity] = []
        owned.reserveCapacity(rowGrowth.capacity)
        for value in values { try workMeter.consume(at: stage); owned.append(value) }
        let slot = try vacant(hash: hash)
        try reservation.absorbAll(from: pending)
        entries.append(Entry(values: owned, hash: hash))
        slots[slot] = entries.count - 1
        return true
    }

    func contains(_ values: [CanonicalSQLValueIdentity]) throws -> Bool {
        try contains(values, hash: CanonicalSQLValueIdentity.hash(values, workMeter: workMeter, stage: stage))
    }

    mutating func insert(_ value: CanonicalSQLValueIdentity) throws -> Bool {
        let scratch = try workMeter.reserveIntermediate(bytes:
            DatabaseIntermediateCollectionMeter.arrayFootprint(count: 1, element: CanonicalSQLValueIdentity.self).bytes, at: stage)
        defer { scratch.release() }
        return try insert([value])
    }

    func contains(_ value: CanonicalSQLValueIdentity) throws -> Bool {
        let scratch = try workMeter.reserveIntermediate(bytes:
            DatabaseIntermediateCollectionMeter.arrayFootprint(count: 1, element: CanonicalSQLValueIdentity.self).bytes, at: stage)
        defer { scratch.release() }
        return try contains([value])
    }

    private func contains(_ values: [CanonicalSQLValueIdentity], hash: Int) throws -> Bool {
        guard !slots.isEmpty else { return false }
        var slot = start(hash)
        while let index = slots[slot] {
            try workMeter.consume(at: stage)
            let entry = entries[index]
            if entry.hash == hash || forcedHash != nil {
                if try CanonicalSQLValueIdentity.equal(entry.values, values, workMeter: workMeter, stage: stage) { return true }
            }
            slot = (slot + 1) & (slots.count - 1)
        }
        return false
    }

    private func start(_ hash: Int) -> Int { (forcedHash ?? hash) & (slots.count - 1) }
    private func vacant(hash: Int) throws -> Int {
        var slot = start(hash)
        while slots[slot] != nil {
            try workMeter.consume(at: stage)
            slot = (slot + 1) & (slots.count - 1)
        }
        return slot
    }

    private mutating func grow(toFit count: Int) throws {
        guard count > entryCapacity else { return }
        let entriesLayout = try DatabaseRetainedArrayLayout.forElement(Entry.self)
        let growth = try entriesLayout.growth(from: entryCapacity, toFit: count)
        let doubled = growth.capacity.multipliedReportingOverflow(by: 2)
        guard !doubled.overflow else { throw DatabaseExpressionEvaluationError.numericOverflow }
        let newSlotCount = max(2, doubled.partialValue)
        let slotBytes = try DatabaseIntermediateFootprint(bytes: UInt64(MemoryLayout<Int?>.stride))
            .multiplied(by: UInt64(newSlotCount))
        // Old and new slot owners overlap during rehash; admit the complete new table first.
        let pending = try workMeter.reserveIntermediate(bytes: try slotBytes.adding(
            DatabaseIntermediateFootprint(bytes: growth.additionalByteCount)).bytes, at: stage)
        defer { pending.release() }
        var replacement = [Int?](repeating: nil, count: newSlotCount)
        for (index, entry) in entries.enumerated() {
            try workMeter.consume(at: stage)
            var slot = (forcedHash ?? entry.hash) & (newSlotCount - 1)
            while replacement[slot] != nil {
                try workMeter.consume(at: stage)
                slot = (slot + 1) & (newSlotCount - 1)
            }
            replacement[slot] = index
        }
        let oldSlotBytes = UInt64(slots.count * MemoryLayout<Int?>.stride)
        entries.reserveCapacity(growth.capacity)
        try reservation.absorbAll(from: pending)
        slots = replacement
        entryCapacity = growth.capacity
        reservation.releaseGuaranteedPartial(bytes: oldSlotBytes)
    }
}
