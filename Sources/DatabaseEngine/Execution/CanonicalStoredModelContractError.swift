enum CanonicalStoredModelContractError: Error, Sendable, Equatable {
    case footprintMismatch(
        entity: String,
        admittedBytes: UInt64,
        actualBytes: UInt64
    )
}
