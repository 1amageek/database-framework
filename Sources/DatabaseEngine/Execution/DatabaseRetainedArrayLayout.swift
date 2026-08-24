/// Platform-independent admission model for retained Array storage.
///
/// Byte values are canonical budget units, not allocator introspection. The
/// capacity-slot value covers the inline Array slot and deterministic spare
/// capacity selected for that element type. Element payload is admitted
/// separately for each append.
package struct DatabaseRetainedArrayLayout: Sendable, Equatable {
    package let containerByteCount: UInt64
    package let elementCapacitySlotByteCount: UInt64
    package let sharedOwnerByteCount: UInt64
    package let appendAdmissionByteCount: UInt64

    private init(
        containerByteCount: UInt64,
        elementCapacitySlotByteCount: UInt64,
        sharedOwnerByteCount: UInt64,
        appendAdmissionByteCount: UInt64
    ) {
        self.containerByteCount = containerByteCount
        self.elementCapacitySlotByteCount = elementCapacitySlotByteCount
        self.sharedOwnerByteCount = sharedOwnerByteCount
        self.appendAdmissionByteCount = appendAdmissionByteCount
    }

    package static func validated(
        containerByteCount: UInt64,
        elementCapacitySlotByteCount: UInt64,
        sharedOwnerByteCount: UInt64,
        appendAdmissionByteCount: UInt64
    ) throws -> DatabaseRetainedArrayLayout {
        guard containerByteCount > 0 else {
            throw DatabaseRetainedArrayLayoutError.zeroContainerByteCount
        }
        guard elementCapacitySlotByteCount > 0 else {
            throw DatabaseRetainedArrayLayoutError
                .zeroElementCapacitySlotByteCount
        }
        guard sharedOwnerByteCount > 0 else {
            throw DatabaseRetainedArrayLayoutError.zeroSharedOwnerByteCount
        }
        guard appendAdmissionByteCount > 0 else {
            throw DatabaseRetainedArrayLayoutError
                .zeroAppendAdmissionByteCount
        }
        return DatabaseRetainedArrayLayout(
            containerByteCount: containerByteCount,
            elementCapacitySlotByteCount: elementCapacitySlotByteCount,
            sharedOwnerByteCount: sharedOwnerByteCount,
            appendAdmissionByteCount: appendAdmissionByteCount
        )
    }

    /// Canonical admission layout for one ordinary Swift `Array` element.
    package static func forElement<Element>(
        _ type: Element.Type
    ) throws -> DatabaseRetainedArrayLayout {
        try validated(
            containerByteCount: UInt64(MemoryLayout<[Element]>.stride),
            elementCapacitySlotByteCount: UInt64(
                max(1, MemoryLayout<Element>.stride)
            ),
            sharedOwnerByteCount: 32,
            appendAdmissionByteCount: 16
        )
    }

    package func growth(
        from currentCapacity: Int,
        toFit requiredCount: Int
    ) throws -> (capacity: Int, additionalByteCount: UInt64) {
        guard currentCapacity >= 0 else {
            throw DatabaseRetainedArrayLayoutError.invalidCurrentCapacity(
                currentCapacity
            )
        }
        guard requiredCount >= 0 else {
            throw DatabaseRetainedArrayLayoutError.invalidRequiredCount(
                requiredCount
            )
        }
        guard requiredCount > currentCapacity else {
            return (currentCapacity, 0)
        }

        var capacity = max(1, currentCapacity)
        while capacity < requiredCount {
            let (next, overflow) = capacity.multipliedReportingOverflow(by: 2)
            guard !overflow else {
                throw DatabaseRetainedArrayLayoutError.capacityOverflow(
                    currentCapacity: capacity
                )
            }
            capacity = next
        }

        let additionalSlots = UInt64(capacity - currentCapacity)
        let additionalBytes = try DatabaseIntermediateFootprint(
            bytes: elementCapacitySlotByteCount
        ).multiplied(by: additionalSlots).bytes
        return (capacity, additionalBytes)
    }
}
