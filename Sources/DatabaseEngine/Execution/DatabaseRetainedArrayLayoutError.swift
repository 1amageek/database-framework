package enum DatabaseRetainedArrayLayoutError: Error, Sendable, Equatable {
    case zeroContainerByteCount
    case zeroElementCapacitySlotByteCount
    case zeroSharedOwnerByteCount
    case zeroAppendAdmissionByteCount
    case invalidCurrentCapacity(Int)
    case invalidRequiredCount(Int)
    case capacityOverflow(currentCapacity: Int)
}
