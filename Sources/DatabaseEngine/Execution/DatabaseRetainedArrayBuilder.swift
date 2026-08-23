/// Task-local builder that reserves request memory before retaining elements.
package struct DatabaseRetainedArrayBuilder<Element: Sendable>: ~Copyable {
    private var elements: [Element]
    private let reservation: DatabaseIntermediateReservation
    private let defaultStage: DatabaseWorkStage
    private let layout: DatabaseRetainedArrayLayout
    private var accountedCapacity: Int

    package init(
        workMeter: DatabaseWorkMeter,
        stage: DatabaseWorkStage,
        layout: DatabaseRetainedArrayLayout,
        expectedCount: Int = 0
    ) throws {
        let initialGrowth = try layout.growth(
            from: 0,
            toFit: expectedCount
        )
        let initialFootprint = try DatabaseIntermediateFootprint(
            bytes: layout.containerByteCount
        ).adding(
            DatabaseIntermediateFootprint(
                bytes: initialGrowth.additionalByteCount
            )
        )
        let reservation = try workMeter.reserveIntermediate(
            bytes: initialFootprint.bytes,
            at: stage
        )
        var elements: [Element] = []
        elements.reserveCapacity(initialGrowth.capacity)
        let actualCapacity = elements.capacity
        precondition(actualCapacity >= initialGrowth.capacity)
        if actualCapacity > initialGrowth.capacity {
            try reservation.reserveAdditional(
                bytes: try layout.capacityByteCount(
                    actualCapacity - initialGrowth.capacity
                ),
                at: stage
            )
        }
        self.elements = elements
        self.reservation = reservation
        self.defaultStage = stage
        self.layout = layout
        self.accountedCapacity = actualCapacity
    }

    init(
        elements: consuming [Element],
        reservation: DatabaseIntermediateReservation,
        defaultStage: DatabaseWorkStage,
        layout: DatabaseRetainedArrayLayout,
        accountedCapacity: Int
    ) {
        precondition(accountedCapacity >= elements.count)
        self.elements = elements
        self.reservation = reservation
        self.defaultStage = defaultStage
        self.layout = layout
        self.accountedCapacity = accountedCapacity
    }

    package var count: Int { elements.count }
    package var isEmpty: Bool { elements.isEmpty }
    package var workMeter: DatabaseWorkMeter { reservation.workMeter }

    /// Admits the next element and any required Array growth before the
    /// element is constructed. The returned linear token may live across an
    /// async decision; abandonment rolls its element claim back.
    package mutating func prepareAppend(
        footprint: DatabaseIntermediateFootprint,
        at stage: DatabaseWorkStage? = nil
    ) throws -> DatabaseRetainedArrayAppendAdmission<Element> {
        let (requiredCount, countOverflow) = elements.count
            .addingReportingOverflow(1)
        guard !countOverflow else {
            throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                currentCapacity: accountedCapacity
            )
        }
        let growth = try layout.growth(
            from: accountedCapacity,
            toFit: requiredCount
        )
        let admittedFootprint = try footprint.adding(
            DatabaseIntermediateFootprint(
                bytes: layout.appendAdmissionByteCount
            )
        )
        let claimReservation = try reservation.reserveChild(
            rows: admittedFootprint.rows,
            bytes: admittedFootprint.bytes,
            at: stage ?? defaultStage
        )
        if growth.capacity != accountedCapacity {
            do {
                // Reallocation temporarily retains both Array buffers. The new
                // target buffer is admitted independently before construction;
                // any allocator spare capacity is reconciled before publication.
                let replacementReservation = try reservation.reserveChild(
                    bytes: try layout.capacityByteCount(growth.capacity),
                    at: stage ?? defaultStage
                )
                var replacement = elements
                replacement.reserveCapacity(growth.capacity)
                let actualCapacity = replacement.capacity
                precondition(actualCapacity >= growth.capacity)
                if actualCapacity > growth.capacity {
                    try replacementReservation.reserveAdditional(
                        bytes: try layout.capacityByteCount(
                            actualCapacity - growth.capacity
                        ),
                        at: stage ?? defaultStage
                    )
                }
                let replacementBytes = try layout.capacityByteCount(
                    actualCapacity
                )
                reservation.absorbGuaranteedPartial(
                    from: replacementReservation,
                    bytes: replacementBytes
                )
                let replacedBytes = try layout.capacityByteCount(
                    accountedCapacity
                )
                elements = replacement
                reservation.releaseGuaranteedPartial(bytes: replacedBytes)
                accountedCapacity = actualCapacity
            } catch {
                claimReservation.release()
                throw error
            }
        }
        return DatabaseRetainedArrayAppendAdmission(
            sourceReservation: reservation,
            claimReservation: claimReservation,
            payloadFootprint: footprint,
            expectedElementCount: elements.count
        )
    }

    /// Moves a previously admitted element into the retained Array. Capacity
    /// was already materialized by `prepareAppend`, so this does not create a
    /// second element buffer.
    package mutating func append(
        _ element: consuming Element,
        using admission: consuming DatabaseRetainedArrayAppendAdmission<Element>
    ) {
        admission.commit(
            to: reservation,
            at: elements.count
        )
        elements.append(element)
    }

    /// Reserves the declared footprint before evaluating `make` or growing the
    /// Array. If element creation fails, the exact claim is rolled back.
    package mutating func append<Failure: Error>(
        footprint: DatabaseIntermediateFootprint,
        at stage: DatabaseWorkStage? = nil,
        make: () throws(Failure) -> Element
    ) throws -> Void {
        let admission = try prepareAppend(
            footprint: footprint,
            at: stage
        )
        let element = try make()
        append(element, using: admission)
    }

    package consuming func finish() -> DatabaseRetainedBuffer<Element> {
        DatabaseRetainedBuffer(
            elements: elements,
            reservation: reservation,
            layout: layout,
            accountedCapacity: accountedCapacity
        )
    }
}
