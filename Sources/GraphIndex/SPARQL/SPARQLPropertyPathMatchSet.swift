import DatabaseEngine

/// Request-accounted distinctness sidecar for property-path matches.
///
/// Duplicate probes do not change capacity or retained memory. For a new
/// match, its complete payload and any Set capacity growth are admitted before
/// `reserveCapacity` or `insert` can allocate retained storage.
struct SPARQLPropertyPathMatchSet: ~Copyable {
    // Declaration order keeps Set storage destruction ahead of reservation
    // release, matching the ownership ordering used by retained scan results.
    private var storage: Set<SPARQLPropertyPathMatch>
    private let reservation: DatabaseIntermediateReservation
    private let footprintMeter: SPARQLPropertyPathMatchFootprintMeter
    private let defaultStage: DatabaseWorkStage
    private var accountedCapacity: Int

    private static let containerByteCount: UInt64 = 64
    private static let capacitySlotByteCount: UInt64 = 64

    private init(
        storage: consuming Set<SPARQLPropertyPathMatch>,
        reservation: DatabaseIntermediateReservation,
        footprintMeter: SPARQLPropertyPathMatchFootprintMeter,
        defaultStage: DatabaseWorkStage,
        accountedCapacity: Int
    ) {
        self.storage = storage
        self.reservation = reservation
        self.footprintMeter = footprintMeter
        self.defaultStage = defaultStage
        self.accountedCapacity = accountedCapacity
    }

    static func make(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage = .deduplication
    ) throws -> SPARQLPropertyPathMatchSet {
        let footprintMeter = try SPARQLPropertyPathMatchFootprintMeter.make(
            workMeter: workMeter,
            stage: stage
        )
        do {
            let reservation = try workMeter.reserveIntermediate(
                bytes: containerByteCount,
                at: stage
            )
            return SPARQLPropertyPathMatchSet(
                storage: Set(),
                reservation: reservation,
                footprintMeter: footprintMeter,
                defaultStage: stage,
                accountedCapacity: 0
            )
        } catch {
            footprintMeter.shutdown()
            throw error
        }
    }

    var count: Int { storage.count }
    var isEmpty: Bool { storage.isEmpty }

    borrowing func contains(
        _ match: borrowing SPARQLPropertyPathMatch
    ) -> Bool {
        storage.contains(match)
    }

    /// Inserts one match after a non-mutating duplicate probe.
    ///
    /// Returns `false` for an existing match without measuring its footprint
    /// or changing any reservation.
    @discardableResult
    mutating func insert(
        _ match: borrowing SPARQLPropertyPathMatch,
        at stage: DatabaseWorkStage? = nil
    ) throws -> Bool {
        guard !storage.contains(match) else { return false }

        let footprint = try footprintMeter.footprint(of: match)
        let (requiredCount, countOverflow) = storage.count
            .addingReportingOverflow(1)
        guard !countOverflow else {
            throw SPARQLPropertyPathMatchStorageError.setCountOverflow
        }
        let growth = try Self.capacityGrowth(
            from: accountedCapacity,
            toFit: requiredCount
        )
        let admitted = try footprint.adding(
            DatabaseIntermediateFootprint(
                bytes: growth.additionalByteCount
            )
        )
        try reservation.reserveAdditional(
            rows: admitted.rows,
            bytes: admitted.bytes,
            at: stage ?? defaultStage
        )

        if growth.capacity != accountedCapacity {
            storage.reserveCapacity(growth.capacity)
            accountedCapacity = growth.capacity
        }
        let insertion = storage.insert(copy match)
        precondition(
            insertion.inserted,
            "A property-path match changed equality during insertion"
        )
        return true
    }

    static func capacityGrowth(
        from currentCapacity: Int,
        toFit requiredCount: Int
    ) throws -> (capacity: Int, additionalByteCount: UInt64) {
        guard currentCapacity >= 0 else {
            throw SPARQLPropertyPathMatchStorageError
                .invalidSetCapacity(currentCapacity)
        }
        guard requiredCount >= 0 else {
            throw SPARQLPropertyPathMatchStorageError
                .invalidSetRequiredCount(requiredCount)
        }
        guard requiredCount > currentCapacity else {
            return (currentCapacity, 0)
        }

        var capacity = max(1, currentCapacity)
        while capacity < requiredCount {
            let (doubled, overflow) = capacity
                .multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw SPARQLPropertyPathMatchStorageError
                    .setCapacityOverflow(currentCapacity: capacity)
            }
            capacity = doubled
        }

        let additionalSlots = UInt64(capacity - currentCapacity)
        let additionalByteCount = try DatabaseIntermediateFootprint(
            bytes: capacitySlotByteCount
        ).multiplied(by: additionalSlots).bytes
        return (capacity, additionalByteCount)
    }
}
