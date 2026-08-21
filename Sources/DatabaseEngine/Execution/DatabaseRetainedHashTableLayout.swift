/// Platform-independent admission model for retained hash-table storage.
///
/// Capacity is a canonical budget unit rather than allocator introspection.
/// The model covers the table value and deterministic key/value slots while
/// leaving key and value payloads to their semantic footprint meters.
package struct DatabaseRetainedHashTableLayout: Sendable, Equatable {
    package let containerByteCount: UInt64
    package let elementCapacitySlotByteCount: UInt64

    private init(
        containerByteCount: UInt64,
        elementCapacitySlotByteCount: UInt64
    ) {
        self.containerByteCount = containerByteCount
        self.elementCapacitySlotByteCount = elementCapacitySlotByteCount
    }

    package static func validated(
        containerByteCount: UInt64,
        elementCapacitySlotByteCount: UInt64
    ) throws -> DatabaseRetainedHashTableLayout {
        guard containerByteCount > 0 else {
            throw DatabaseRetainedHashTableLayoutError.zeroContainerByteCount
        }
        guard elementCapacitySlotByteCount > 0 else {
            throw DatabaseRetainedHashTableLayoutError
                .zeroElementCapacitySlotByteCount
        }
        return DatabaseRetainedHashTableLayout(
            containerByteCount: containerByteCount,
            elementCapacitySlotByteCount: elementCapacitySlotByteCount
        )
    }

    package func growth(
        from currentCapacity: Int,
        toFit requiredCount: Int
    ) throws -> (capacity: Int, additionalByteCount: UInt64) {
        guard currentCapacity >= 0 else {
            throw DatabaseRetainedHashTableLayoutError.invalidCurrentCapacity(
                currentCapacity
            )
        }
        guard requiredCount >= 0 else {
            throw DatabaseRetainedHashTableLayoutError.invalidRequiredCount(
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
                throw DatabaseRetainedHashTableLayoutError.capacityOverflow(
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
