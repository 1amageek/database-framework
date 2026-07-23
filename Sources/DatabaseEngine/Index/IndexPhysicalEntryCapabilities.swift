/// Explicit physical-layout capabilities supplied by an index runtime provider.
public struct IndexPhysicalEntryCapabilities: Sendable {
    public let decoder: any IndexPhysicalEntryDecoder
    public let supportsItemReferenceValidation: Bool
    public let supportsIndependentEntryRepair: Bool

    public init(
        decoder: any IndexPhysicalEntryDecoder,
        supportsItemReferenceValidation: Bool,
        supportsIndependentEntryRepair: Bool
    ) {
        self.decoder = decoder
        self.supportsItemReferenceValidation = supportsItemReferenceValidation
        self.supportsIndependentEntryRepair = supportsIndependentEntryRepair
    }
}
