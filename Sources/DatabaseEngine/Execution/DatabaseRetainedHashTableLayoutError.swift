package enum DatabaseRetainedHashTableLayoutError: Error, Sendable, Equatable {
    case zeroContainerByteCount
    case zeroElementCapacitySlotByteCount
    case invalidCurrentCapacity(Int)
    case invalidRequiredCount(Int)
    case capacityOverflow(currentCapacity: Int)
}
