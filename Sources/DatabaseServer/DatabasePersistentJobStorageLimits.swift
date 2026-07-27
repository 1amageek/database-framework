@_spi(DatabaseServer) import DatabaseWire

public struct DatabasePersistentJobStorageLimits: Sendable, Hashable {
    public static let maximumResultChunkBytes = 512 * 1_024

    private static let resultManifestFixedByteCount = 123
    private static let encodedDigestByteCount = 36
    private static let planFixedByteCount = 59
    private static let minimumUnsuccessfulOutcomeBytes = 4 * 1_024
    private static let stateInfrastructureReservedBytes = 4 * 1_024

    public let maximumStorageValueBytes: Int
    public let maximumSpecificationBytes: Int
    public let maximumPlanBytes: Int
    public let maximumStateBytes: Int
    public let maximumOperationStateBytes: Int
    public let maximumUnsuccessfulOutcomeBytes: Int
    public let maximumResultBytes: Int
    public let resultChunkBytes: Int

    var maximumPlanPayloadBytes: Int {
        maximumPlanBytes - Self.planFixedByteCount
    }

    public init(
        maximumStorageValueBytes: Int,
        maximumSpecificationBytes: Int = 64 * 1_024,
        maximumPlanBytes: Int = 256 * 1_024,
        maximumStateBytes: Int = 128 * 1_024,
        maximumOperationStateBytes: Int = 64 * 1_024,
        maximumUnsuccessfulOutcomeBytes: Int = 48 * 1_024,
        maximumResultBytes: Int = 4 * 1_024 * 1_024,
        resultChunkBytes: Int = 512 * 1_024
    ) {
        self.maximumStorageValueBytes = maximumStorageValueBytes
        self.maximumSpecificationBytes = maximumSpecificationBytes
        self.maximumPlanBytes = maximumPlanBytes
        self.maximumStateBytes = maximumStateBytes
        self.maximumOperationStateBytes = maximumOperationStateBytes
        self.maximumUnsuccessfulOutcomeBytes = maximumUnsuccessfulOutcomeBytes
        self.maximumResultBytes = maximumResultBytes
        self.resultChunkBytes = resultChunkBytes
    }

    public func validate() throws {
        let statePayloadBudget = maximumOperationStateBytes
            .addingReportingOverflow(maximumUnsuccessfulOutcomeBytes)
        let reservedStateBudget = statePayloadBudget.partialValue
            .addingReportingOverflow(Self.stateInfrastructureReservedBytes)
        guard maximumStorageValueBytes > 0,
              UInt32(exactly: maximumStorageValueBytes) != nil,
              maximumSpecificationBytes > 0,
              maximumSpecificationBytes <= maximumStorageValueBytes,
              maximumPlanBytes > 0,
              maximumPlanBytes > Self.planFixedByteCount,
              maximumPlanBytes <= maximumStorageValueBytes,
              maximumStateBytes > 0,
              maximumStateBytes <= maximumStorageValueBytes,
              maximumOperationStateBytes > 0,
              maximumUnsuccessfulOutcomeBytes
                >= Self.minimumUnsuccessfulOutcomeBytes,
              !statePayloadBudget.overflow,
              !reservedStateBudget.overflow,
              reservedStateBudget.partialValue <= maximumStateBytes,
              maximumResultBytes >= 0,
              resultChunkBytes > 0,
              resultChunkBytes <= maximumStorageValueBytes,
              resultChunkBytes <= Self.maximumResultChunkBytes,
              UInt32(exactly: resultChunkBytes) != nil else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "Persistent job storage limits are inconsistent"
            )
        }
    }

    func validate(wireLimits: DatabaseWireLimits) throws {
        try validate()
        let chunkCount = maximumResultBytes == 0
            ? 0
            : ((maximumResultBytes - 1) / resultChunkBytes) + 1
        let digestBytes = chunkCount.multipliedReportingOverflow(
            by: Self.encodedDigestByteCount
        )
        let manifestBytes = Self.resultManifestFixedByteCount
            .addingReportingOverflow(digestBytes.partialValue)
        guard !digestBytes.overflow,
              !manifestBytes.overflow,
              UInt32(exactly: chunkCount) != nil,
              chunkCount <= wireLimits.maximumCollectionCount,
              maximumSpecificationBytes <= wireLimits.maximumFrameBytes,
              maximumPlanBytes <= wireLimits.maximumFrameBytes,
              maximumStateBytes <= wireLimits.maximumFrameBytes,
              manifestBytes.partialValue <= maximumSpecificationBytes,
              manifestBytes.partialValue <= wireLimits.maximumFrameBytes else {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "Persistent job result manifest exceeds configured limits"
            )
        }
        do {
            _ = try DatabasePersistentJobFailureStoragePolicy(
                storageLimits: self,
                wireLimits: wireLimits
            )
        } catch {
            throw DatabaseJobRuntimeError.invalidConfiguration(
                "Persistent job failure diagnostics exceed configured limits"
            )
        }
    }

    func planWireLimits(
        basedOn limits: DatabaseWireLimits
    ) throws(DatabaseWireLimitsError) -> DatabaseWireLimits {
        try payloadWireLimits(
            maximumBytes: maximumPlanPayloadBytes,
            basedOn: limits
        )
    }

    func stateWireLimits(
        basedOn limits: DatabaseWireLimits
    ) throws(DatabaseWireLimitsError) -> DatabaseWireLimits {
        try payloadWireLimits(
            maximumBytes: maximumOperationStateBytes,
            basedOn: limits
        )
    }

    func resultWireLimits(
        basedOn limits: DatabaseWireLimits
    ) throws(DatabaseWireLimitsError) -> DatabaseWireLimits {
        try payloadWireLimits(
            maximumBytes: maximumResultBytes,
            basedOn: limits
        )
    }

    func unsuccessfulOutcomeWireLimits(
        basedOn limits: DatabaseWireLimits
    ) throws(DatabaseWireLimitsError) -> DatabaseWireLimits {
        try payloadWireLimits(
            maximumBytes: maximumUnsuccessfulOutcomeBytes,
            basedOn: limits
        )
    }

    private func payloadWireLimits(
        maximumBytes: Int,
        basedOn limits: DatabaseWireLimits
    ) throws(DatabaseWireLimitsError) -> DatabaseWireLimits {
        try DatabaseWireLimits(
            maximumFrameBytes: min(limits.maximumFrameBytes, maximumBytes),
            maximumStringBytes: min(limits.maximumStringBytes, maximumBytes),
            maximumByteStringBytes: min(
                limits.maximumByteStringBytes,
                maximumBytes
            ),
            maximumCollectionCount: limits.maximumCollectionCount,
            maximumNestingDepth: limits.maximumNestingDepth,
            maximumObjectCount: limits.maximumObjectCount
        )
    }
}
